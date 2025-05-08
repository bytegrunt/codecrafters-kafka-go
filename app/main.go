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
			req, err := parseRequest(conn)
			if err != nil {
				fmt.Println("Error parsing request: ", err.Error())
				done <- true // signal to close connection
				return
			}
			fmt.Println("Received request: ", req)

			var errorCode int16
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
	// Tagged fields (INT8, always 0) after api_key entry
	binary.Write(&b, binary.BigEndian, int8(0))
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

func parseRequest(conn net.Conn) (*Request, error) {
	// Read the first 4 bytes to get the message size
	sizeBuf := make([]byte, 4)
	if _, err := conn.Read(sizeBuf); err != nil {
		return nil, fmt.Errorf("failed to read message size: %v", err)
	}
	messageSize := binary.BigEndian.Uint32(sizeBuf)

	// Read the rest of the message (messageSize bytes)
	payload := make([]byte, messageSize)
	read := 0
	for read < int(messageSize) {
		n, err := conn.Read(payload[read:])
		if err != nil {
			return nil, fmt.Errorf("failed to read message payload: %v", err)
		}
		read += n
	}

	apiKey := binary.BigEndian.Uint16(payload[0:2])
	apiVersion := binary.BigEndian.Uint16(payload[2:4])
	correlationId := binary.BigEndian.Uint32(payload[4:8])

	return &Request{
		MessageSize:       int32(messageSize),
		RequestApiKey:     int16(apiKey),
		RequestApiVersion: int16(apiVersion),
		CorrelationId:     int32(correlationId),
	}, nil
}