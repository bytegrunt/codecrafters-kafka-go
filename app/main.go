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
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		return
	}
}

func writeResponse(conn net.Conn, req *Request) error{

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
	buffer := make([]byte, 4096)
	_, err := request.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %v", err)
	}
	// Read the first 4 bytes to get the message size
	fmt.Println("messageSize: ", buffer[0:4])
	messageSize := binary.BigEndian.Uint32(buffer[0:4])

	// read the next 2 bytes for ap key
	fmt.Println("apiKey: ", buffer[4:6])
	apiKey := binary.BigEndian.Uint16(buffer[4:6])

	//read the next 2 bytes for api version
	fmt.Println("apiVersion: ", buffer[6:8])
	apiVersion := binary.BigEndian.Uint16(buffer[6:8])

	// read the next 4 bytes for correlation id
	fmt.Println("correlationId: ", buffer[8:12])
	correlationId := binary.BigEndian.Uint32(buffer[8:12])

	return &Request{
		MessageSize: int32(messageSize),
		RequestApiKey: int16(apiKey),
		RequestApiVersion: int16(apiVersion),
		CorrelationId: int32(correlationId),
	}, nil
}