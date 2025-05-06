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
	MessageSize  int32
	RequestApiKey int16
	RequestApiVersion    int16
	CorrelationId int32
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
}

func writeResponse(conn net.Conn, req *Request) {

	response := make([]byte, 8)
	binary.BigEndian.PutUint32(response[0:4], uint32(req.MessageSize))
	binary.BigEndian.PutUint32(response[4:8], uint32(req.CorrelationId))

	fmt.Println("Response: ", response)
	conn.Write(response)
}

func parseRequest(request net.Conn) (*Request, error) {
	// Read the first 4 bytes to get the message size
	buffer := make([]byte, 4)
	_, err := request.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %v", err)
	}
	fmt.Println("messageSize: ", buffer)
	messageSize := binary.BigEndian.Uint32(buffer)

	// read the next 2 bytes for ap key
	buffer = make([]byte, 2)
	_, err = request.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read api key: %v", err)
	}
	fmt.Println("apiKey: ", buffer)
	apiKey := binary.BigEndian.Uint16(buffer)

	//read the next 2 bytes for api version
	buffer = make([]byte, 2)
	_, err = request.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read api version: %v", err)
	}
	fmt.Println("apiVersion: ", buffer)
	apiVersion := binary.BigEndian.Uint16(buffer)

	// read the next 4 bytes for correlation id
	buffer = make([]byte, 4)
	_, err = request.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read correlation id: %v", err)
	}
	fmt.Println("correlationId: ", buffer)
	correlationId := binary.BigEndian.Uint32(buffer)


	return &Request{
		MessageSize: int32(messageSize),
		RequestApiKey: int16(apiKey),
		RequestApiVersion: int16(apiVersion),
		CorrelationId: int32(correlationId),
	}, nil
}