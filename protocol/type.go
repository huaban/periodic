package protocol

import (
	"bytes"
	"strconv"
)

// ClientType Define the client type.
type ClientType int

const (
	// TYPECLIENT defined the connection client is a client.
	TYPECLIENT ClientType = iota + 1
	// TYPEWORKER defined the connection client is a worker.
	TYPEWORKER
)

// Bytes convert client type to Byte
func (c ClientType) Bytes() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(c))
	return buf.Bytes()
}

// to string `TYPECLIENT`, `TYPEWORKER`.
func (c ClientType) String() string {
	switch c {
	case TYPECLIENT:
		return "TYPECLIENT"
	case TYPEWORKER:
		return "TYPEWORKER"
	}
	panic("Unknow ClientType " + strconv.Itoa(int(c)))
}
