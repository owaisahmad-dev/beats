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

func (p *kafkaStream) parseUnsignedVarInt(bytes *[]byte) int64 {
	varint, n := binary.Uvarint((*bytes)[p.currentOffset:])
	if n < 1 {
		panic("Error parsing VAR INT")
	}
	p.currentOffset += n
	return int64(varint)
}

func (p *kafkaStream) parseString(bytes *[]byte) string {
	size := p.parseInt16(bytes)
	stringVal := (*bytes)[p.currentOffset : p.currentOffset+int(size)]
	p.currentOffset += int(size)

	return string(stringVal)
}

func (p *kafkaStream) parseCompactString(bytes *[]byte) string {
	topicNameSize := p.parseVarInt(bytes)
	topicNameSize -= 1
	if topicNameSize < 0 {
		panic("Invalid Var Int could not parse")
	}
	topicName := string((*bytes)[p.currentOffset : p.currentOffset+int(topicNameSize)])
	p.currentOffset += int(topicNameSize)

	return topicName
}

func (p *kafkaStream) parseUUID(bytes *[]byte) string {
	parsedUUID, error := uuid.ParseBytes((*bytes)[p.currentOffset : p.currentOffset+16])
	if error != nil {
		panic(error)
	}
	p.currentOffset += 16
	return parsedUUID.String()
}

func (p *kafkaStream) parseFetchResponse(message *[]byte, version uint16) (bool, bool, []string) {
	// FIXME
	var messages []string
	if version == 11 {
		// skip fields

		p.currentOffset += 10

		numberOfResponses := p.parseInt32(message)

		for i := 0; i < int(numberOfResponses); i++ {
			messages = append(messages, p.parseTopicResponse(message, version)...)
		}
	}
	return true, true, messages
}

func (p *kafkaStream) parseFetchResponsePartitions(message *[]byte, version uint16) []string {
	numberOfPartitions := p.parseInt32(message)
	var messages []string
	for i := 0; i < int(numberOfPartitions); i++ {
		// SKIPPING UNNECESSARY BYTES START
		p.currentOffset += 14

		if version >= 4 {
			p.currentOffset += 8
			if version >= 5 {
				p.currentOffset += 8
			}
			numberOfAbortedTransactions := p.parseInt32(message)
			for i := 0; i < int(numberOfAbortedTransactions); i++ {
				p.currentOffset += 16
			}
			if version >= 11 {
				p.currentOffset += 4
			}
		}
		// SKIPPING UNNECESSARY BYTES END
		messages = append(messages, p.parseRecordBatch(message, version)...)
	}
	return messages
}

func (p *kafkaStream) parseRecordBatch(message *[]byte, version uint16) []string {
	sizeOfRecordSets := 0
	if version > 11 {
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

func (p *kafkaStream) parseTopicResponse(message *[]byte, version uint16) []string {
	var topicName = p.parseTopic(message, version)
	fmt.Println("Topic Name:", topicName)
	return p.parseFetchResponsePartitions(message, version)
}

func (p *kafkaStream) parseTopicRequest(message *[]byte, version uint16) string {
	var topicName = p.parseTopic(message, version)
	fmt.Println("Topic Name:", topicName)
	p.skipFetchRequestPartitions(message, version)
	return topicName
}

func (p *kafkaStream) skipFetchRequestPartitions(message *[]byte, version uint16) {
	numberOfPartitions := p.parseInt32(message)

	if version < 5 {
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 16
		}
	} else if version < 9 {
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 20
		}
	} else {
		for j := 0; j < int(numberOfPartitions); j++ {
			p.currentOffset += 28
		}
	}

}

func (p *kafkaStream) parseTopic(message *[]byte, version uint16) string {
	if version < 12 {
		topicName := p.parseString(message)
		return topicName

	} else if version == 12 {
		topicName := p.parseCompactString(message)
		return topicName
	} else {
		parsedUUID := p.parseUUID(message)
		return parsedUUID
	}
}

func (p *kafkaStream) parseFetchRequest(message *[]byte, version uint16) (bool, bool, []string) {
	// Skip next 25 Bytes unnecessary information
	p.currentOffset += 25

	var topics []string
	numberOfTopics := p.parseInt32(message)
	for i := 0; i < int(numberOfTopics); i++ {
		topic := p.parseTopicRequest(message, version)
		topics = append(topics, topic)
	}

	return true, true, topics
}
