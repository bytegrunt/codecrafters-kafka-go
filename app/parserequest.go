package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

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
