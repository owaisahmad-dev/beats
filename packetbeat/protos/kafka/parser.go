package kafka

import (
	"encoding/binary"

	"github.com/google/uuid"
)

type kafkaStream struct {
	currentOffset int
	sizeOfMessage int
	message       *kafkaMessage
	data          []byte
	isClient      bool
	kafka         *kafkaPlugin
}

type errorStruct struct {
	message string
}

func (p *kafkaStream) parseInt32(bytes *[]byte) int32 {
	parsedValue := int32(binary.BigEndian.Uint32((*bytes)[p.currentOffset : p.currentOffset+4]))
	p.currentOffset += 4
	return parsedValue
}

func (p *kafkaStream) parseInt64(bytes *[]byte) int64 {
	parsedValue := int64(binary.BigEndian.Uint64((*bytes)[p.currentOffset : p.currentOffset+8]))
	p.currentOffset += 8
	return parsedValue
}

func (p *kafkaStream) parseInt16(bytes *[]byte) int16 {
	parsedValue := int16(binary.BigEndian.Uint16((*bytes)[p.currentOffset : p.currentOffset+2]))
	p.currentOffset += 2
	return parsedValue
}

func (p *kafkaStream) parseVarInt(bytes *[]byte) (int64, *errorStruct) {
	varint, n := binary.Varint((*bytes)[p.currentOffset:])
	var err = &errorStruct{message: "Error parsing var int"}
	if n < 1 {
		return -1, err
	}
	p.currentOffset += n
	return int64(varint), nil
}

func (p *kafkaStream) parseUnsignedVarInt(bytes *[]byte) (uint64, *errorStruct) {
	varint, n := binary.Uvarint((*bytes)[p.currentOffset:])
	var err = &errorStruct{message: "Error parsing unsigned var int"}
	if n < 1 {
		return 0, err
	}
	p.currentOffset += n
	return uint64(varint), nil

}

func (p *kafkaStream) parseString(bytes *[]byte) string {
	size := p.parseInt16(bytes)
	if size < 0 {
		return ""
	}
	stringVal := (*bytes)[p.currentOffset : p.currentOffset+int(size)]
	p.currentOffset += int(size)

	return string(stringVal)
}

func (p *kafkaStream) parseCompactString(bytes *[]byte) (string, *errorStruct) {
	topicNameSize, err := p.parseUnsignedVarInt(bytes)
	// the field is null
	if topicNameSize == 0 {
		return "", nil
	}
	topicNameSize -= 1
	if topicNameSize < 0 || err != nil {
		return "", err
	}
	topicName := string((*bytes)[p.currentOffset : p.currentOffset+int(topicNameSize)])
	p.currentOffset += int(topicNameSize)

	return topicName, nil
}

func (p *kafkaStream) parseUUID(bytes *[]byte) (string, *errorStruct) {
	parsedUUID, err := uuid.FromBytes((*bytes)[p.currentOffset : p.currentOffset+16])
	if err != nil {
		return "", &errorStruct{message: "Could not parse UUID"}
	}
	p.currentOffset += 16
	return parsedUUID.String(), nil
}

func (p *kafkaStream) parseFetchResponse(message *[]byte, version uint16) (bool, bool) {
	if p.message.isFlexible {
		p.parseTags(message)
	}
	if version >= 1 {
		p.parseInt32(message)
	}

	if version >= 7 {
		var ec ErrorCode = ErrorCode(p.parseInt16(message))
		var errorMessage string
		var isError bool = false
		if ec != 0 {
			if ec == -1 {
				errorMessage = "UNKNOWN_SERVER_ERROR"
			}
			errorMessage = ec.String()
		}
		p.parseInt32(message)
		p.message.isError = isError
		p.message.errorMessages = []string{errorMessage}
	}

	var messages []string

	var numberOfResponses uint64
	var err *errorStruct
	if version > 11 {
		numberOfResponses, err = p.parseUnsignedVarInt(message)
		numberOfResponses -= 1
	} else {
		numberOfResponses = uint64(p.parseInt32(message))
	}

	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return false, false
	}

	for i := 0; i < int(numberOfResponses); i++ {
		messages = append(messages, p.parseFetchTopicResponse(message, version)...)
		if p.message.isFlexible {
			p.parseTags(message)
		}
	}
	p.message.messages = messages

	return true, true
}

func (p *kafkaStream) parseFetchResponsePartitions(message *[]byte, version uint16) []string {
	var numberOfPartitions uint64
	var err *errorStruct
	if version > 12 {
		numberOfPartitions, err = p.parseUnsignedVarInt(message)
		numberOfPartitions -= 1
	} else {
		numberOfPartitions = uint64(p.parseInt32(message))
	}
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return []string{}
	}
	var messages []string
	for i := 0; i < int(numberOfPartitions); i++ {
		// SKIPPING UNNECESSARY BYTES START
		p.currentOffset += 14

		if version <= 2 {
			messageSize := p.parseInt32(message)
			if messageSize > 0 {
				var newMessages []string
				newMessages, err = p.parseMessageSet(message, version)
				if err != nil {
					p.message.errorMessages = append(p.message.errorMessages, err.message)
					p.message.isError = true
					break
				}
				messages = append(messages, newMessages...)
			}
		} else {
			if version >= 4 {
				p.currentOffset += 8
				if version >= 5 {
					p.currentOffset += 8
				}
				var numberOfAbortedTransactions uint64
				if version > 11 {
					numberOfAbortedTransactions, err = p.parseUnsignedVarInt(message)
					numberOfAbortedTransactions -= 1
					if err != nil {
						p.message.errorMessages = append(p.message.errorMessages, err.message)
						break
					}
				} else {
					numberOfAbortedTransactions = uint64(p.parseInt32(message))
				}
				for i := 0; i < int(numberOfAbortedTransactions); i++ {
					p.currentOffset += 16
					if p.message.isFlexible {
						p.parseTags(message)
					}
				}
				if version >= 11 {
					p.currentOffset += 4
				}
			}
			// SKIPPING UNNECESSARY BYTES END
			var newMessages []string
			newMessages = p.parseRecordBatch(message, version)
			if p.message.isError {
				return messages
			}
			messages = append(messages, newMessages...)
			if p.message.isFlexible {
				p.parseTags(message)
			}
		}

	}
	return messages
}

func (p *kafkaStream) parseMessageSet(message *[]byte, version uint16) ([]string, *errorStruct) {
	p.parseInt64(message)
	messageSize := p.parseInt32(message)

	parsedMessageSize := 0
	var messages []string
	for parsedMessageSize < int(messageSize) {
		startingOffset := p.currentOffset
		p.parseInt32(message)

		// skip magic byte
		p.currentOffset += 1

		//skip attributes byte
		p.currentOffset += 1

		// skip timestamp
		p.parseInt64(message)

		// key
		p.parseString(message)
		value := p.parseString(message)
		if value != "" {
			messages = append(messages, value)
		} else {
			parsedMessageSize += p.currentOffset - startingOffset
			remainingSize := messageSize - int32(parsedMessageSize)
			p.currentOffset += int(remainingSize)
			break
		}

	}
	if parsedMessageSize > int(messageSize) {
		return messages, &errorStruct{message: "Incorrect packet data"}
	}
	return messages, nil
}

func (p *kafkaStream) parseRecordBatch(message *[]byte, version uint16) []string {
	var sizeOfRecordSets uint64
	var err *errorStruct
	if (p.message.apiKey == 1 && version > 11) || (p.message.apiKey == 0 && version > 8) {
		sizeOfRecordSets, err = p.parseUnsignedVarInt(message)
		sizeOfRecordSets -= 1
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
			return []string{}
		}

	} else {
		sizeOfRecordSets = uint64(p.parseInt32(message))
	}

	parsedMessageSize := 0

	i := 0
	var messages []string
	for parsedMessageSize < int(sizeOfRecordSets) {
		startingOffset := p.currentOffset
		p.currentOffset += 57

		numberOfRecords := p.parseInt32(message)

		for j := 0; j < int(numberOfRecords); j++ {
			// "_" represents throwable values
			_, err = p.parseVarInt(message)
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}
			// Skipping attributes byte
			p.currentOffset += 1
			_, err = p.parseVarInt(message)
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}
			_, err = p.parseVarInt(message)
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}

			var keyLength int64
			keyLength, err = p.parseVarInt(message)
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}
			if keyLength > 0 {
				p.currentOffset += int(keyLength)
			}

			var valueLength int64
			valueLength, err = p.parseVarInt(message)
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}
			value := string((*message)[p.currentOffset : p.currentOffset+int(valueLength)])
			p.currentOffset += int(valueLength)
			messages = append(messages, value)

			var numberOfHeaders int64
			numberOfHeaders, err = p.parseVarInt(message)
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}
			for z := 0; z < int(numberOfHeaders); z++ {
				var lengthOfString int64
				lengthOfString, err = p.parseVarInt(message)
				if err != nil {
					p.message.errorMessages = append(p.message.errorMessages, err.message)
					p.message.isError = true
					break
				}
				p.currentOffset += int(lengthOfString)

				lengthOfString, err = p.parseVarInt(message)
				if err != nil {
					p.message.errorMessages = append(p.message.errorMessages, err.message)
					p.message.isError = true
					break
				}
				p.currentOffset += int(lengthOfString)
			}
			if p.message.isError {
				break
			}
			i++

		}
		if err != nil || p.message.isError {
			break
		}
		parsedMessageSize += p.currentOffset - startingOffset
	}
	if parsedMessageSize > int(sizeOfRecordSets) {
		p.message.errorMessages = append(p.message.errorMessages, "Incorrect packet data")
		p.message.isError = true
		return messages
	}

	return messages
}

func (p *kafkaStream) parseFetchTopicResponse(message *[]byte, version uint16) []string {
	p.parseFetchTopic(message, version)
	return p.parseFetchResponsePartitions(message, version)
}

func (p *kafkaStream) parseFetchTopicRequest(message *[]byte, version uint16) string {
	var topicName = p.parseFetchTopic(message, version)
	if p.message.isError {
		return ""
	}
	p.skipFetchRequestPartitions(message, version)
	return topicName
}

func (p *kafkaStream) skipFetchRequestPartitions(message *[]byte, version uint16) {
	var numberOfPartitions uint64
	var err *errorStruct = nil
	if version > 12 {
		numberOfPartitions, err = p.parseUnsignedVarInt(message)
		numberOfPartitions -= 1
	} else {
		numberOfPartitions = uint64(p.parseInt32(message))
	}
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return
	}

	if version < 5 {
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 16
		}
	} else if version < 9 {
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 20
		}
	} else if version < 11 {
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 28
		}
	} else {
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 32
			if p.message.isFlexible {
				p.parseTags(message)
			}
		}
	}

}

func (p *kafkaStream) parseProduceRequestPartitions(message *[]byte, version uint16) []string {
	var numberOfPartitions uint64
	var err *errorStruct
	if version == 9 {
		numberOfPartitions, err = p.parseUnsignedVarInt(message)
		numberOfPartitions -= 1
	} else {
		numberOfPartitions = uint64(p.parseInt32(message))
	}
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return []string{}
	}
	var messages []string
	for i := 0; i < int(numberOfPartitions); i++ {
		p.parseInt32(message)
		var msgs []string
		msgs = p.parseRecordBatch(message, version)
		if p.message.isError {
			break
		}
		messages = append(messages, msgs...)
		if p.message.isFlexible {
			p.parseTags(message)
		}
	}
	return messages
}

func (p *kafkaStream) parseFetchTopic(message *[]byte, version uint16) string {
	if version < 12 {
		topicName := p.parseString(message)
		return topicName
	} else if version == 12 {
		topicName, err := p.parseCompactString(message)
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
			return ""
		}
		return topicName
	} else {
		parsedUUID, err := p.parseUUID(message)
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
			return ""
		}
		if p.kafka.topicUUIDStore[parsedUUID] != "" {
			return p.kafka.topicUUIDStore[parsedUUID]
		}
		return parsedUUID
	}
}

func (p *kafkaStream) parseProduceTopic(message *[]byte, version uint16) string {
	if version <= 8 {
		topicName := p.parseString(message)
		return topicName
	} else {
		topicName, err := p.parseCompactString(message)
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
		}
		return topicName
	}
}

func (p *kafkaStream) parseTags(message *[]byte) {
	taggedCount, err := p.parseUnsignedVarInt(message)
	taggedCount -= 1
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return
	}
	for i := 0; i < int(taggedCount); i++ {
		p.parseUnsignedVarInt(message) // tagID
		size, err := p.parseUnsignedVarInt(message)
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
			break
		}
		// throw away
		p.currentOffset += int(size)
	}
}
func (p *kafkaStream) parseFetchRequest(message *[]byte, version uint16) (bool, bool) {
	// Skip next 25 Bytes unnecessary information
	if p.message.isFlexible {
		p.parseTags(message)
	}
	p.currentOffset += 12
	if version >= 3 {
		p.currentOffset += 4
		if version >= 4 {
			p.currentOffset += 1
			if version >= 7 {
				p.currentOffset += 8
			}
		}
	}

	var topics []string
	var numberOfTopics uint64
	var err *errorStruct
	if version > 12 {
		numberOfTopics, err = p.parseUnsignedVarInt(message)
		numberOfTopics -= 1
	} else {
		numberOfTopics = uint64(p.parseInt32(message))
	}
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return false, false
	}
	for i := 0; i < int(numberOfTopics); i++ {
		topic := p.parseFetchTopicRequest(message, version)
		topics = append(topics, topic)
		if p.message.isFlexible {
			p.parseTags(message)
		}
		if len(p.message.errorMessages) > 0 {
			return false, false
		}
	}
	p.message.topics = topics

	return true, true
}

func (p *kafkaStream) parseProduceTopicRequest(message *[]byte, version uint16) (string, []string) {
	topicName := p.parseProduceTopic(message, version)
	messages := p.parseProduceRequestPartitions(message, version)

	return topicName, messages
}

func (p *kafkaStream) parseProduceRequest(message *[]byte, version uint16) (bool, bool) {
	if p.message.isFlexible {
		p.parseTags(message)
	}
	if version >= 3 {
		if version == 9 {
			p.parseCompactString(message)
		} else {
			p.parseString(message)
		}
	}

	p.parseInt16(message)

	p.parseInt32(message)
	var topics []string
	var messages []string
	var numberOfTopics uint64
	var err *errorStruct = nil
	if version == 9 {
		numberOfTopics, err = p.parseUnsignedVarInt(message)
		numberOfTopics -= 1
	} else {
		numberOfTopics = uint64(p.parseInt32(message))
	}
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		return false, false
	}
	for i := 0; i < int(numberOfTopics); i++ {
		topic, msgs := p.parseProduceTopicRequest(message, version)
		topics = append(topics, topic)
		messages = append(messages, msgs...)
		if p.message.isFlexible {
			p.parseTags(message)
		}
		if p.message.isError {
			return false, false
		}
	}

	p.message.topics = topics
	p.message.messages = messages

	return true, true

}

func (p *kafkaStream) parseProducePartitionResponse(message *[]byte, version uint16) (bool, string) {
	p.parseInt32(message)
	var ec ErrorCode = ErrorCode(p.parseInt16(message))
	var errorMessage string
	var isError bool = false
	if ec != 0 {
		if ec == -1 {
			errorMessage = "UNKNOWN_SERVER_ERROR"
		}
		errorMessage = ec.String()
	}
	p.parseInt64(message)
	if version >= 2 {
		p.parseInt64(message)
		if version >= 5 {
			p.parseInt64(message)
		}
		if version >= 8 {
			var numberOfRecordErrors uint64
			var err *errorStruct
			if version > 8 {
				numberOfRecordErrors, err = p.parseUnsignedVarInt(message)
				numberOfRecordErrors -= 1
			} else {
				numberOfRecordErrors = uint64(p.parseInt32(message))
			}
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				return true, err.message
			}

			for i := 0; i < int(numberOfRecordErrors); i++ {
				p.parseInt32(message)
				if version == 9 {
					p.parseCompactString(message)
				} else {
					p.parseString(message)
				}
			}

			if version == 9 {
				p.parseCompactString(message)
			} else {
				p.parseString(message)
			}
		}
	}

	return isError, errorMessage
}

func (p *kafkaStream) parseProducePartitionsResponse(message *[]byte, version uint16) bool {
	var numberOfPartitions uint64
	var err *errorStruct
	if version > 8 {
		numberOfPartitions, err = p.parseUnsignedVarInt(message)
		numberOfPartitions -= 1
	} else {
		numberOfPartitions = uint64(p.parseInt32(message))
	}

	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return false
	}

	var errorMessages []string
	var isErrorState bool = false

	for i := 0; i < int(numberOfPartitions); i++ {
		isError, errorMessage := p.parseProducePartitionResponse(message, version)
		isErrorState = isErrorState || isError
		errorMessages = append(errorMessages, errorMessage)
		if version > 0 {
			p.parseInt32(message)
		}
		if p.message.isFlexible {
			p.parseTags(message)
		}
	}
	p.message.errorMessages = errorMessages
	return !isErrorState
}

func (p *kafkaStream) parseProduceTopicResponse(message *[]byte, version uint16) bool {
	p.parseProduceTopic(message, version)
	return p.parseProducePartitionsResponse(message, version)
}

func (p *kafkaStream) parseProduceResponse(message *[]byte, version uint16) (bool, bool) {
	if p.message.isFlexible {
		p.parseTags(message)
	}
	var numberOfTopics uint64
	var err *errorStruct
	if version > 8 {
		numberOfTopics, err = p.parseUnsignedVarInt(message)
	} else {
		numberOfTopics = uint64(p.parseInt32(message))
	}
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return false, false
	}
	ok := true
	complete := true

	for i := 0; i < int(numberOfTopics); i++ {
		ok = ok && p.parseProduceTopicResponse(message, version)
		if p.message.isFlexible {
			p.parseTags(message)
		}
		if p.message.isError {
			break
		}
	}

	p.message.isError = !ok

	return true, complete
}

func (p *kafkaStream) parseMetadataResponse(message *[]byte, version uint16) (bool, bool) {
	if version < 10 {
		p.message.errorMessages = append(p.message.errorMessages, "Unsupported version for metadata request")
		return false, false
	}
	p.parseTags(message)

	// throttle time
	p.parseInt32(message)

	var numberOfBrokers, err = p.parseUnsignedVarInt(message)
	numberOfBrokers -= 1

	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return false, false
	}
	for i := 0; i < int(numberOfBrokers); i++ {
		// node_id
		p.parseInt32(message)
		// host
		p.parseCompactString(message)
		// port
		p.parseInt32(message)
		// rack
		p.parseCompactString(message)
		// tags
		p.parseTags(message)
	}

	// cluster id
	p.parseCompactString(message)

	// controller id
	p.parseInt32(message)

	var numberOfTopics uint64
	numberOfTopics, err = p.parseUnsignedVarInt(message)
	numberOfTopics -= 1
	if err != nil {
		p.message.errorMessages = append(p.message.errorMessages, err.message)
		p.message.isError = true
		return false, false
	}
	for i := 0; i < int(numberOfTopics); i++ {
		// error code
		p.parseInt16(message)
		var err *errorStruct
		topicName, err := p.parseCompactString(message)
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
			break
		}
		topicId, err := p.parseUUID(message)
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
			break
		}

		kafka := p.kafka
		kafka.topicUUIDStore[topicId] = topicName

		// is_internal
		p.currentOffset += 1
		var numberOfPartitions uint64
		numberOfPartitions, err = p.parseUnsignedVarInt(message)
		numberOfPartitions -= 1
		if err != nil {
			p.message.errorMessages = append(p.message.errorMessages, err.message)
			p.message.isError = true
			break
		}
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 14
			numberOfReplicaNodes, err := p.parseUnsignedVarInt(message)
			numberOfReplicaNodes -= 1
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}

			for z := 0; z < int(numberOfReplicaNodes); z++ {
				p.parseInt32(message)
			}

			numberOfIsrNodes, err := p.parseUnsignedVarInt(message)
			numberOfIsrNodes -= 1
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}
			for z := 0; z < int(numberOfIsrNodes); z++ {
				p.parseInt32(message)
			}

			numberOfOfflineNodes, err := p.parseUnsignedVarInt(message)
			numberOfOfflineNodes -= 1
			if err != nil {
				p.message.errorMessages = append(p.message.errorMessages, err.message)
				p.message.isError = true
				break
			}
			for z := 0; z < int(numberOfOfflineNodes); z++ {
				p.parseInt32(message)
			}
			p.parseTags(message)
		}
		// topic authorized operations
		if p.message.isError {
			return false, false
		}
		p.parseInt32(message)
		p.parseTags(message)
	}

	return true, true
}
