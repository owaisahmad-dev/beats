package kafka

import (
	"time"

	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/packetbeat/pb"
	"github.com/elastic/beats/v7/packetbeat/procs"
	"github.com/elastic/beats/v7/packetbeat/protos"
	"github.com/elastic/beats/v7/packetbeat/protos/tcp"
	conf "github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/mapstr"
	"github.com/elastic/elastic-agent-libs/monitoring"
)

var (
	unmatchedRequests  = monitoring.NewInt(nil, "kafka.unmatched_requests")
	unmatchedResponses = monitoring.NewInt(nil, "kafka.unmatched_responses")
)

type kafkaMessage struct {
	isRequest bool
	start     int
	end       int

	ts time.Time

	direction    uint8
	tcpTuple     common.TCPTuple
	cmdlineTuple *common.ProcessTuple

	apiKey        uint16
	apiVersion    uint16
	correlationId uint32
	size          uint32
	clientId      string

	isError    bool
	isFlexible bool

	topics        []string
	messages      []string
	errorMessages []string

	raw []byte
}

type kafkaTransaction struct {
	tuple   common.TCPTuple
	src     common.Endpoint
	dst     common.Endpoint
	ts      time.Time
	endTime time.Time

	isError bool

	kafka mapstr.M

	requestRaw  string
	responseRaw string

	bytesIn  uint64
	bytesOut uint64
}

type kafkaPlugin struct {
	ports        []int
	sendRequest  bool
	sendResponse bool

	transactions       map[uint32]*kafkaTransaction
	transactionTimeout time.Duration

	transactionMetadataStore map[uint32]*requestMetadata

	results protos.Reporter
	watcher procs.ProcessesWatcher

	handleKafka func(kafka *kafkaPlugin, k *kafkaMessage, tcp *common.TCPTuple, dir uint8, raw_msg []byte)
}

type requestMetadata struct {
	apiKey     uint16
	apiVersion uint16
	clientId   string
}

type kafkaPrivateData struct {
	data [2]*kafkaStream
}

func init() {
	protos.Register("kafka", New)
}

func New(
	testMode bool,
	results protos.Reporter,
	watcher procs.ProcessesWatcher,
	cfg *conf.C,
) (protos.Plugin, error) {
	p := &kafkaPlugin{}
	config := defaultConfig

	if !testMode {
		if err := cfg.Unpack(&config); err != nil {
			return nil, err
		}
	}

	if err := p.init(results, watcher, &config); err != nil {
		return nil, err
	}
	return p, nil

}

func (kafka *kafkaPlugin) init(results protos.Reporter, watcher procs.ProcessesWatcher, config *kafkaConfig) error {
	kafka.setFromConfig(config)
	kafka.handleKafka = handleKafkaMessage
	kafka.results = results
	kafka.watcher = watcher
	kafka.transactionMetadataStore = make(map[uint32]*requestMetadata)

	return nil
}

func (kafka *kafkaPlugin) setFromConfig(config *kafkaConfig) {
	kafka.ports = config.Ports
}

func (kafka *kafkaPlugin) getTransaction(correlationId uint32) *kafkaTransaction {
	v := kafka.transactions[correlationId]
	if v != nil {
		return v
	}
	return nil
}

func (kafka *kafkaPlugin) GetPorts() []int {
	return kafka.ports
}

func (kafka *kafkaPlugin) isServerPort(port uint16) bool {
	for _, sPort := range kafka.ports {
		if uint16(sPort) == port {
			return true
		}
	}
	return false
}

func (kafka *kafkaPlugin) messageComplete(tcpTuple *common.TCPTuple, dir uint8, stream *kafkaStream) {
	msg := stream.data[stream.message.start:stream.message.end]

	kafka.handleKafka(kafka, stream.message, tcpTuple, dir, msg)
}

func (kafka *kafkaPlugin) Parse(pkt *protos.Packet, tcpTuple *common.TCPTuple, dir uint8, private protos.ProtocolData) protos.ProtocolData {
	defer logp.Recover("Parse Kafka Exception")

	priv := kafkaPrivateData{}

	if private != nil {
		var ok bool
		priv, ok = private.(kafkaPrivateData)
		if !ok {
			priv = kafkaPrivateData{}
		}
	}

	if priv.data[dir] == nil {
		dstPort := tcpTuple.DstPort
		if dir == tcp.TCPDirectionReverse {
			dstPort = tcpTuple.SrcPort
		}
		priv.data[dir] = &kafkaStream{
			data:     pkt.Payload,
			message:  &kafkaMessage{ts: pkt.Ts},
			isClient: kafka.isServerPort(dstPort),
		}
	} else {
		priv.data[dir].data = append(priv.data[dir].data, pkt.Payload...)
		if len(priv.data[dir].data) > tcp.TCPMaxDataInStream {
			logp.Debug("kafka", "Stream data too large, dropping TCP stream")
			priv.data[dir] = nil
			return priv
		}
	}

	stream := priv.data[dir]
	for len(stream.data) > 0 {
		stream.message = &kafkaMessage{ts: pkt.Ts}

		ok, complete := kafka.kafkaMessageParser(stream)
		logp.Debug("kafka_detailed", "kafkaMessageParser returned ok=%v complete=%v", ok, complete)
		if !ok {
			priv.data[dir] = nil
			logp.Debug("kafka", "Ignore Kafka message. Drop tcp stream. Try parsing with the next segment")
			return priv
		}

		if complete {
			kafka.messageComplete(tcpTuple, dir, stream)
		} else {
			break
		}

	}

	return priv

}

func isFlexible(apiKey uint16, apiVersion uint16) bool {
	if (apiKey == 0 && apiVersion >= 9) || (apiKey == 1 && apiVersion >= 12) {
		return true
	} else {
		return false
	}
}

func (kafka *kafkaPlugin) kafkaMessageParser(s *kafkaStream) (bool, bool) {
	lengthOfData := s.parseInt32(&s.data)
	if int(lengthOfData) > len(s.data) {
		logp.Debug("kafka_detailed", "parse_response: Incomplete message")
		return true, false
	}
	s.message.size = uint32(lengthOfData)
	msg := s.message
	if s.isClient {

		msg.apiKey = uint16(s.parseInt16(&s.data))
		msg.apiVersion = uint16(s.parseInt16(&s.data))
		msg.correlationId = uint32(s.parseInt32(&s.data))
		msg.isFlexible = isFlexible(msg.apiKey, msg.apiVersion)
		msg.clientId = s.parseString(&s.data)

		msg.isRequest = true
		kafka.transactionMetadataStore[msg.correlationId] = &requestMetadata{apiKey: msg.apiKey, apiVersion: msg.apiVersion, clientId: msg.clientId}

		switch msg.apiKey {
		case 0:
			ok, complete := s.parseProduceRequest(&s.data, msg.apiVersion)
			return ok, complete
		case 1:
			ok, complete := s.parseFetchRequest(&s.data, msg.apiVersion)
			return ok, complete
		default:
			logp.Debug("kafka_detailed", "Kafka unknown message key = %d", msg.apiKey)
			return false, false
		}

	} else {
		msg.correlationId = uint32(s.parseInt32(&s.data))
		reqMetadata := kafka.transactionMetadataStore[msg.correlationId]
		if reqMetadata == nil {
			return false, false
		}
		msg.apiKey = reqMetadata.apiKey
		msg.apiVersion = reqMetadata.apiVersion
		msg.isFlexible = isFlexible(msg.apiKey, msg.apiVersion)
		msg.clientId = reqMetadata.clientId

		switch msg.apiKey {
		case 0:
			ok, complete := s.parseProduceResponse(&s.data, s.message.apiVersion)
			return ok, complete
		case 1:
			ok, complete := s.parseFetchResponse(&s.data, s.message.apiVersion)
			return ok, complete
		default:
			logp.Debug("kafka_detailed", "Kafka unknown message key = %d", msg.apiKey)
			return false, false
		}
	}
}

func handleKafkaMessage(kafka *kafkaPlugin, m *kafkaMessage, tcpTuple *common.TCPTuple, dir uint8, rawMsg []byte) {
	m.tcpTuple = *tcpTuple
	m.direction = dir
	m.cmdlineTuple = kafka.watcher.FindProcessesTupleTCP(tcpTuple.IPPort())
	m.raw = rawMsg

	if m.isRequest {
		kafka.receivedKafkaRequest(m)
	} else {
		delete(kafka.transactionMetadataStore, m.correlationId)
		kafka.receivedKafkaResponse(m)
	}
}

func (kafka *kafkaPlugin) receivedKafkaRequest(msg *kafkaMessage) {
	tuple := msg.tcpTuple
	trans := kafka.getTransaction(msg.correlationId)

	if trans != nil {
		if trans.kafka != nil {
			logp.Debug("kafka", "two requests without a Response, Dropping old request: %s", trans.kafka)
			unmatchedRequests.Add(1)
		}
	} else {
		trans = &kafkaTransaction{tuple: tuple}
		kafka.transactions[msg.correlationId] = trans
	}

	trans.ts = msg.ts
	trans.src, trans.dst = common.MakeEndpointPair(msg.tcpTuple.BaseTuple, msg.cmdlineTuple)

	if msg.direction == tcp.TCPDirectionReverse {
		trans.src, trans.dst = trans.dst, trans.src
	}

	trans.kafka = mapstr.M{}
	if msg.apiKey == 1 {
		trans.kafka["topics"] = msg.topics
		trans.kafka["clientId"] = msg.clientId
	}

	if msg.apiKey == 0 {
		trans.kafka["clientId"] = msg.clientId
		trans.kafka["topics"] = msg.topics
		trans.kafka["messages"] = msg.messages
	}

	trans.requestRaw = string(msg.raw)
}

func (kafka *kafkaPlugin) receivedKafkaResponse(msg *kafkaMessage) {
	trans := kafka.getTransaction(msg.correlationId)

	if trans == nil {
		logp.Debug("kafka", "Response from unknown transaction, ignoring")
		unmatchedRequests.Add(1)
		return
	}
	if trans.kafka == nil {
		logp.Debug("kafka", "Response from unknown transaction, ignoring")
		unmatchedRequests.Add(1)
		return
	}
	trans.isError = msg.isError

	if trans.isError {
		trans.kafka["error_messages"] = msg.errorMessages
	}

	if msg.apiKey == 1 {
		// its the fetch request
		trans.kafka["messages"] = msg.messages
	}
	trans.responseRaw = string(msg.raw)

}

func (kafka *kafkaPlugin) publishTransaction(t *kafkaTransaction) {
	if kafka.results == nil {
		return
	}

	evt, pbf := pb.NewBeatEvent(t.ts)
	pbf.SetSource(&t.src)
	pbf.AddIP(t.src.IP)
	pbf.SetDestination(&t.dst)
	pbf.AddIP(t.dst.IP)
	pbf.Source.Bytes = int64(t.bytesIn)
	pbf.Destination.Bytes = int64(t.bytesOut)
	pbf.Event.Start = t.ts
	pbf.Event.End = t.endTime
	pbf.Network.Transport = "tcp"
	pbf.Network.Protocol = "kafka"
	pbf.Error.Message = []string{"??"}

	fields := evt.Fields
	fields["type"] = pbf.Event.Dataset
	fields["kafka"] = t.kafka

	if t.isError {
		fields["status"] = common.ERROR_STATUS
	} else {
		fields["status"] = common.OK_STATUS
	}

	kafka.results(evt)

}
