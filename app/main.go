package main

import (
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

	req, err := parseRequest(conn)
	if err != nil {
		fmt.Println("Error parsing request: ", err.Error())
		return
	}
	fmt.Println("Received request: ", req)

	// Switch req.ApiVersion is within range of 0-4
	// set erroCode to 0 if it is within range, else set it to 35
	var errorCode int16
	if req.RequestApiVersion < 0 || req.RequestApiVersion > 4 {
		fmt.Println("Error: ApiVersion out of range")
		errorCode = 35
	} else {
		fmt.Println("ApiVersion is within range")
		errorCode = 0
	}

	writeResponse(conn, req, errorCode)
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		return
	}
}

func writeResponse(conn net.Conn, req *Request, errorCode int16) error {
	// Prepare ApiVersions response with one entry for ApiKey 18
	apiKey := uint16(18)
	minVersion := uint16(0)
	maxVersion := uint16(4)

	// Calculate response body size:
	// correlationId (4) + errorCode (2) + apiKeysCount (4) + apiKeyEntry (6)
	bodyLen := 4 + 2 + 4 + 6

	response := make([]byte, 4+bodyLen) // 4 bytes for length prefix

	// Set response length (excluding the length field itself)
	binary.BigEndian.PutUint32(response[0:4], uint32(bodyLen))

	// Correlation ID
	binary.BigEndian.PutUint32(response[4:8], uint32(req.CorrelationId))

	// Error code
	binary.BigEndian.PutUint16(response[8:10], uint16(errorCode))

	// Number of API keys (1)
	binary.BigEndian.PutUint32(response[10:14], 1)

	// API key entry
	binary.BigEndian.PutUint16(response[14:16], apiKey)
	binary.BigEndian.PutUint16(response[16:18], minVersion)
	binary.BigEndian.PutUint16(response[18:20], maxVersion)

	_, err := conn.Write(response)
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