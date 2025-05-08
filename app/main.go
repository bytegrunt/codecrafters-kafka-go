package main

import (
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
			continue
		}

		}
	}()

	<-done // Wait for signal to close connection
}
