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

func parseRequest(request net.Conn) (*Request, error) {
	headerLen := 12
	buffer := make([]byte, headerLen)
	read := 0
	for read < headerLen {
		n, err := request.Read(buffer[read:])
		if err != nil {
			return nil, fmt.Errorf("failed to read message header: %v", err)
		}
		read += n
	}
	messageSize := binary.BigEndian.Uint32(buffer[0:4])
	apiKey := binary.BigEndian.Uint16(buffer[4:6])
	apiVersion := binary.BigEndian.Uint16(buffer[6:8])
	correlationId := binary.BigEndian.Uint32(buffer[8:12])

	// Read the rest of the request body (if any)
	bodyLen := int(messageSize) - (headerLen - 4) // messageSize excludes itself
	if bodyLen > 0 {
		body := make([]byte, bodyLen)
		read := 0
		for read < bodyLen {
			n, err := request.Read(body[read:])
			if err != nil {
				return nil, fmt.Errorf("failed to read message body: %v", err)
			}
			read += n
		}
	}

	return &Request{
		MessageSize:       int32(messageSize),
		RequestApiKey:     int16(apiKey),
		RequestApiVersion: int16(apiVersion),
		CorrelationId:     int32(correlationId),
	}, nil
}