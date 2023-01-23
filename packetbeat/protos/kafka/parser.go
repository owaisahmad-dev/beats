package kafka

import (
	"encoding/binary"
	"fmt"

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

func (p *kafkaStream) parseVarInt(bytes *[]byte) int64 {
	varint, n := binary.Varint((*bytes)[p.currentOffset:])
	if n < 1 {
		panic("Error parsing VAR INT")
	}
	p.currentOffset += n
	return int64(varint)
}

func (p *kafkaStream) parseUnsignedVarInt(bytes *[]byte) uint64 {
	varint, n := binary.Uvarint((*bytes)[p.currentOffset:])
	if n < 1 {
		panic("Error parsing VAR INT")
	}
	p.currentOffset += n
	return uint64(varint)

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

func (p *kafkaStream) parseCompactString(bytes *[]byte) string {
	topicNameSize := p.parseUnsignedVarInt(bytes)
	// the field is null
	if topicNameSize == 0 {
		return ""
	}
	topicNameSize -= 1
	if topicNameSize < 0 {
		panic("Invalid Var Int could not parse")
	}
	topicName := string((*bytes)[p.currentOffset : p.currentOffset+int(topicNameSize)])
	p.currentOffset += int(topicNameSize)

	return topicName
}

func (p *kafkaStream) parseUUID(bytes *[]byte) string {
	parsedUUID, error := uuid.FromBytes((*bytes)[p.currentOffset : p.currentOffset+16])
	if error != nil {
		panic(error)
	}
	p.currentOffset += 16
	return parsedUUID.String()
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

	var numberOfResponses int
	if version > 11 {
		numberOfResponses = int(p.parseUnsignedVarInt(message) - 1)
	} else {
		numberOfResponses = int(p.parseInt32(message))
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
	var numberOfPartitions int
	if version > 12 {
		numberOfPartitions = int(p.parseUnsignedVarInt(message) - 1)
	} else {
		numberOfPartitions = int(p.parseInt32(message))
	}
	var messages []string
	for i := 0; i < int(numberOfPartitions); i++ {
		// SKIPPING UNNECESSARY BYTES START
		p.currentOffset += 14

		if version <= 2 {
			messageSize := p.parseInt32(message)
			if messageSize > 0 {
				messages = append(messages, p.parseMessageSet(message, version)...)
			}
		} else {
			if version >= 4 {
				p.currentOffset += 8
				if version >= 5 {
					p.currentOffset += 8
				}
				var numberOfAbortedTransactions int
				if version > 11 {
					numberOfAbortedTransactions = int(p.parseUnsignedVarInt(message))
				} else {
					numberOfAbortedTransactions = int(p.parseInt32(message))
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
			messages = append(messages, p.parseRecordBatch(message, version)...)
			if p.message.isFlexible {
				p.parseTags(message)
			}
		}

	}
	return messages
}

func (p *kafkaStream) parseMessageSet(message *[]byte, version uint16) []string {
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

		key := p.parseString(message)
		if key != "" {
			fmt.Println("\tRecord Value: ", key)
		}
		value := p.parseString(message)
		if value != "" {
			fmt.Println("\tRecord Value: ", value)
			messages = append(messages, value)
		} else {
			parsedMessageSize += p.currentOffset - startingOffset
			remainingSize := messageSize - int32(parsedMessageSize)
			fmt.Println("Value: ", string((*message)[p.currentOffset:p.currentOffset+int(remainingSize)]))
			p.currentOffset += int(remainingSize)
			break
		}

	}
	if parsedMessageSize > int(messageSize) {
		panic("Incorrect Packet Data")
	}
	return messages
}

func (p *kafkaStream) parseRecordBatch(message *[]byte, version uint16) []string {
	sizeOfRecordSets := 0
	if (p.message.apiKey == 1 && version > 11) || (p.message.apiKey == 0 && version > 8) {
		sizeOfRecordSets = int(p.parseUnsignedVarInt(message) - 1)
	} else {
		sizeOfRecordSets = int(p.parseInt32(message))
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
			_ = p.parseVarInt(message)
			// Skipping attributes byte
			p.currentOffset += 1
			_ = p.parseVarInt(message)
			_ = p.parseVarInt(message)

			keyLength := p.parseVarInt(message)
			if keyLength > 0 {
				fmt.Println("\tRecord Key: ", string((*message)[p.currentOffset:p.currentOffset+int(keyLength)]))
				p.currentOffset += int(keyLength)
			}

			valueLength := p.parseVarInt(message)
			value := string((*message)[p.currentOffset : p.currentOffset+int(valueLength)])
			fmt.Println("\tRecord Value: ", value)
			p.currentOffset += int(valueLength)
			messages = append(messages, value)

			numberOfHeaders := p.parseVarInt(message)
			for z := 0; z < int(numberOfHeaders); z++ {
				lengthOfString := p.parseVarInt(message)
				p.currentOffset += int(lengthOfString)

				lengthOfString = p.parseVarInt(message)
				p.currentOffset += int(lengthOfString)
			}
			i++
		}
		parsedMessageSize += p.currentOffset - startingOffset
	}
	if parsedMessageSize > int(sizeOfRecordSets) {
		panic("Incorrect Packet Data")
	}

	return messages
}

func (p *kafkaStream) parseFetchTopicResponse(message *[]byte, version uint16) []string {
	var topicName = p.parseFetchTopic(message, version)
	fmt.Println("Topic Name:", topicName)
	return p.parseFetchResponsePartitions(message, version)
}

func (p *kafkaStream) parseFetchTopicRequest(message *[]byte, version uint16) string {
	var topicName = p.parseFetchTopic(message, version)
	fmt.Println("Topic Name:", topicName)
	p.skipFetchRequestPartitions(message, version)
	return topicName
}

func (p *kafkaStream) skipFetchRequestPartitions(message *[]byte, version uint16) {
	var numberOfPartitions int
	if version > 12 {
		numberOfPartitions = int(p.parseUnsignedVarInt(message) - 1)
	} else {
		numberOfPartitions = int(p.parseInt32(message))
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
	var numberOfPartitions uint32
	if version == 9 {
		numberOfPartitions = uint32(p.parseUnsignedVarInt(message)) - 1
	} else {
		numberOfPartitions = uint32(p.parseInt32(message))
	}
	var messages []string
	for i := 0; i < int(numberOfPartitions); i++ {
		p.parseInt32(message)
		msgs := p.parseRecordBatch(message, version)
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
		topicName := p.parseCompactString(message)
		return topicName
	} else {
		parsedUUID := p.parseUUID(message)
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
		topicName := p.parseCompactString(message)
		return topicName
	}
}

func (p *kafkaStream) parseTags(message *[]byte) {
	taggedCount := int(p.parseUnsignedVarInt(message))
	for i := 0; i < taggedCount; i++ {
		p.parseUnsignedVarInt(message) // tagID
		size := p.parseUnsignedVarInt(message)

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
	var numberOfTopics int
	if version > 12 {
		numberOfTopics = int(p.parseUnsignedVarInt(message) - 1)
	} else {
		numberOfTopics = int(p.parseInt32(message))
	}
	for i := 0; i < int(numberOfTopics); i++ {
		topic := p.parseFetchTopicRequest(message, version)
		topics = append(topics, topic)
		if p.message.isFlexible {
			p.parseTags(message)
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
	if version == 9 {
		numberOfTopics = p.parseUnsignedVarInt(message) - 1
	} else {
		numberOfTopics = uint64(p.parseInt32(message))
	}

	for i := 0; i < int(numberOfTopics); i++ {
		topic, msgs := p.parseProduceTopicRequest(message, version)
		topics = append(topics, topic)
		messages = append(messages, msgs...)
		if p.message.isFlexible {
			p.parseTags(message)
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
			var numberOfRecordErrors int
			if version > 8 {
				numberOfRecordErrors = int(p.parseUnsignedVarInt(message)) - 1
			} else {
				numberOfRecordErrors = int(p.parseInt32(message))
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
	var numberOfPartitions int
	if version > 8 {
		numberOfPartitions = int(p.parseUnsignedVarInt(message) - 1)
	} else {
		numberOfPartitions = int(p.parseInt32(message))
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
	var numberOfTopics int
	if version > 8 {
		numberOfTopics = int(p.parseUnsignedVarInt(message) - 1)
	} else {
		numberOfTopics = int(p.parseInt32(message))
	}
	ok := true
	complete := true

	for i := 0; i < int(numberOfTopics); i++ {
		ok = ok && p.parseProduceTopicResponse(message, version)
		if p.message.isFlexible {
			p.parseTags(message)
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

	var numberOfBrokers = int(p.parseUnsignedVarInt(message) - 1)
	for i := 0; i < numberOfBrokers; i++ {
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

	var numberOfTopics = int(p.parseUnsignedVarInt(message) - 1)
	for i := 0; i < numberOfTopics; i++ {
		// error code
		p.parseInt16(message)
		topicName := p.parseCompactString(message)
		topicId := p.parseUUID(message)

		kafka := p.kafka
		kafka.topicUUIDStore[topicId] = topicName

		// is_internal
		p.currentOffset += 1
		var numberOfPartitions = int(p.parseUnsignedVarInt(message) - 1)
		for j := 0; j < numberOfPartitions; j++ {
			p.currentOffset += 14
			numberOfReplicaNodes := int(p.parseUnsignedVarInt(message) - 1)
			for z := 0; z < numberOfReplicaNodes; z++ {
				p.parseInt32(message)
			}

			numberOfIsrNodes := int(p.parseUnsignedVarInt(message) - 1)
			for z := 0; z < numberOfIsrNodes; z++ {
				p.parseInt32(message)
			}

			numberOfOfflineNodes := int(p.parseUnsignedVarInt(message) - 1)
			for z := 0; z < numberOfOfflineNodes; z++ {
				p.parseInt32(message)
			}
			p.parseTags(message)
		}
		// topic authorized operations
		p.parseInt32(message)
		p.parseTags(message)
	}

	return true, true
}
