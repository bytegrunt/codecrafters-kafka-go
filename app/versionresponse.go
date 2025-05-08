package main

import (
	"bytes"
	"encoding/binary"
	"net"
)

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
