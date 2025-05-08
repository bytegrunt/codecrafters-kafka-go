package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type DescribeTopicPartitionsRequestBody struct {
	TopicName            string
	ResponsePartitionLimit int32
}

func parseDescribeTopicPartitionsBody(payload []byte, offset int) (*DescribeTopicPartitionsRequestBody, error) {
	i := offset

	// Topics array length (1 byte, minus 1)
	if i >= len(payload) {
		return nil, fmt.Errorf("unexpected end of payload while reading topics array length")
	}
	topicsArrayLen := int(payload[i]) - 1
	i++

	if topicsArrayLen < 1 {
		return nil, fmt.Errorf("no topics found in request")
	}

	// Topic name length (1 byte, minus 1)
	if i >= len(payload) {
		return nil, fmt.Errorf("unexpected end of payload while reading topic name length")
	}
	topicNameLen := int(payload[i]) - 1
	i++

	if topicNameLen < 1 || i+topicNameLen > len(payload) {
		return nil, fmt.Errorf("invalid topic name length")
	}

	// Topic name
	topicName := string(payload[i : i+topicNameLen])
	i += topicNameLen

	// Skip empty buffer (1 byte)
	if i >= len(payload) {
		return nil, fmt.Errorf("unexpected end of payload while skipping empty buffer")
	}
	i++

	// ResponsePartitionLimit (4 bytes)
	if i+4 > len(payload) {
		return nil, fmt.Errorf("unexpected end of payload while reading ResponsePartitionLimit")
	}
	responsePartitionLimit := int32(binary.BigEndian.Uint32(payload[i : i+4]))

	return &DescribeTopicPartitionsRequestBody{
		TopicName:              topicName,
		ResponsePartitionLimit: responsePartitionLimit,
	}, nil
}

func writeDescribeTopicPartitionsResponse(conn net.Conn, correlationId int32, reqBody *DescribeTopicPartitionsRequestBody) error {
	var b bytes.Buffer

	// Correlation ID (4 bytes)
	binary.Write(&b, binary.BigEndian, correlationId)
	// Empty 1 byte buffer
	binary.Write(&b, binary.BigEndian, byte(0))
	// Throttle time (4 bytes, all 0)
	binary.Write(&b, binary.BigEndian, int32(0))
	// Topics array length (1 byte, hardcoded to 2 for 1 topic)
	binary.Write(&b, binary.BigEndian, byte(2))

	// --- Topic array entry ---
	// Error code (2 bytes, hardcoded to 3 for UNKNOWN_TOPIC)
	binary.Write(&b, binary.BigEndian, int16(3))
	// Topic name length (1 byte, topic name length + 1)
	topicNameLen := len(reqBody.TopicName)
	binary.Write(&b, binary.BigEndian, byte(topicNameLen+1))
	// Topic name (x bytes)
	b.Write([]byte(reqBody.TopicName))
	// Topic ID (16 bytes, all 0)
	b.Write(make([]byte, 16))
	// isInternal (1 byte, 0)
	binary.Write(&b, binary.BigEndian, byte(0))
	// Partitions array (1 byte, hardcoded to 1 for empty array)
	binary.Write(&b, binary.BigEndian, byte(1))
	// Topic authorized operations (4 bytes, 0x00000df8)
	binary.Write(&b, binary.BigEndian, uint32(0x00000df8))
	// Empty 1 byte buffer (end of topic)
	binary.Write(&b, binary.BigEndian, byte(0))
	// --- End topic array entry ---

	// Cursor (1 byte, hardcoded to 0xff for null)
	binary.Write(&b, binary.BigEndian, byte(0xff))
	// Final empty 1 byte buffer
	binary.Write(&b, binary.BigEndian, byte(0))

	// Write message size (excluding the 4 bytes for the size itself)
	messageSize := make([]byte, 4)
	binary.BigEndian.PutUint32(messageSize, uint32(b.Len()))
	if _, err := conn.Write(messageSize); err != nil {
		return err
	}
	_, err := conn.Write(b.Bytes())
	return err
}