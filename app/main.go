package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type Request struct {
	MessageSize       int32
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientId          string
}

type DescribeTopicPartitionsRequestBody struct {
	TopicName            string
	ResponsePartitionLimit int32
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Channel to signal when to close the connection
	done := make(chan bool, 1)

	go func() {
		for {
			req, offset, payload, err := parseRequest(conn)
			if err != nil {
				fmt.Println("Error parsing request: ", err.Error())
				done <- true // signal to close connection
				return
			}
			fmt.Println("Received request: ", req)

			// If this is a DescribeTopicPartitions request, parse the body
			if req.RequestApiKey == 75 && req.RequestApiVersion == 0 {
				reqBody, err := parseDescribeTopicPartitionsBody(payload, offset)
				if err != nil {
					fmt.Println("Error parsing describe topic partitions body: ", err.Error())
					done <- true
					return
				}
				if err := writeDescribeTopicPartitionsResponse(conn, req.CorrelationId, reqBody); err != nil {
					fmt.Println("Error writing describe topic partitions response: ", err.Error())
					done <- true
					return
				}
				continue
			}

			var errorCode int16
			if req.RequestApiKey == 18 { 
			if req.RequestApiVersion < 0 || req.RequestApiVersion > 4 {
				fmt.Println("Error: ApiVersion out of range")
				errorCode = 35
			} else {
				fmt.Println("ApiVersion is within range")
				errorCode = 0
			}
			if err := writeResponse(conn, req, errorCode); err != nil {
				fmt.Println("Error writing response: ", err.Error())
				done <- true // signal to close connection
				return
			}
		}

		}
	}()

	<-done // Wait for signal to close connection
}

func writeResponse(conn net.Conn, req *Request, errorCode int16) error {
	var b bytes.Buffer

	// Correlation ID
	binary.Write(&b, binary.BigEndian, int32(req.CorrelationId))
	// Error code
	binary.Write(&b, binary.BigEndian, int16(errorCode))
	// Number of API keys (INT8, should be 2)
	binary.Write(&b, binary.BigEndian, int8(3))
	// API key entry for ApiVersions (18)
	binary.Write(&b, binary.BigEndian, int16(18)) // api_key
	binary.Write(&b, binary.BigEndian, int16(0))  // min_version
	binary.Write(&b, binary.BigEndian, int16(4))  // max_version
	binary.Write(&b, binary.BigEndian, int8(0))   // tagged fields after api_key
	// API key entry for ApiVersions (75)
	binary.Write(&b, binary.BigEndian, int16(75)) // api_key
	binary.Write(&b, binary.BigEndian, int16(0))  // min_version
	binary.Write(&b, binary.BigEndian, int16(0))  // max_version
	binary.Write(&b, binary.BigEndian, int8(0))   // tagged fields after api_key
	// Throttle time (INT32, set to 0)
	binary.Write(&b, binary.BigEndian, int32(0))
	// Tagged fields (INT8, always 0) after throttle_time_ms
	binary.Write(&b, binary.BigEndian, int8(0))

	// Write message size (excluding the 4 bytes for the size itself)
	messageSize := make([]byte, 4)
	binary.BigEndian.PutUint32(messageSize, uint32(b.Len()))
	if _, err := conn.Write(messageSize); err != nil {
		return err
	}
	_, err := conn.Write(b.Bytes())
	return err
}

// parseRequest now returns *Request, the offset where it stopped parsing, and the payload
func parseRequest(conn net.Conn) (*Request, int, []byte, error) {
	// Read the first 4 bytes to get the message size
	sizeBuf := make([]byte, 4)
	if _, err := conn.Read(sizeBuf); err != nil {
		return nil, 0, nil, fmt.Errorf("failed to read message size: %v", err)
	}
	messageSize := binary.BigEndian.Uint32(sizeBuf)

	// Read the rest of the message (messageSize bytes)
	payload := make([]byte, messageSize)
	read := 0
	for read < int(messageSize) {
		n, err := conn.Read(payload[read:])
		if err != nil {
			return nil, 0, nil, fmt.Errorf("failed to read message payload: %v", err)
		}
		read += n
	}

	offset := 0
	apiKey := binary.BigEndian.Uint16(payload[offset : offset+2])
	offset += 2
	apiVersion := binary.BigEndian.Uint16(payload[offset : offset+2])
	offset += 2
	correlationId := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4

	// Read ClientId length (2 bytes)
	clientIdLen := binary.BigEndian.Uint16(payload[offset : offset+2])
	offset += 2
	clientId := ""
	if clientIdLen > 0 {
		clientId = string(payload[offset : offset+int(clientIdLen)])
		offset += int(clientIdLen)
	}
	// Skip 1 byte empty buffer
	offset += 1

	return &Request{
		MessageSize:       int32(messageSize),
		RequestApiKey:     int16(apiKey),
		RequestApiVersion: int16(apiVersion),
		CorrelationId:     int32(correlationId),
		ClientId:          clientId,
	}, offset, payload, nil
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