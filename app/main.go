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

	writeResponse(conn, req)
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		return
	}
}

func writeResponse(conn net.Conn, req *Request) error {

	byteArray := make([]byte, 4)
	binary.BigEndian.PutUint32(byteArray, uint32(req.MessageSize))
	_, err := conn.Write(byteArray)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(byteArray, uint32(req.CorrelationId))
	fmt.Println("correlationId: ", byteArray)
	_, err = conn.Write(byteArray)
	if err != nil {
		return err
	}

	return nil
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

	// Parse the standard Kafka request header from the payload
	if len(payload) < 8 {
		return nil, fmt.Errorf("payload too short for Kafka header")
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