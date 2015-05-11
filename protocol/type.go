package protocol

import (
    "bytes"
    "strconv"
)

// Define the client type.
type ClientType int

const (
    // Client type.
    TYPE_CLIENT ClientType = iota + 1
    // Worker type.
    TYPE_WORKER
)

// to byte
func (c ClientType) Bytes() []byte {
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(c))
    return buf.Bytes()
}

// to string `TYPE_CLIENT`, `TYPE_WORKER`.
func (c ClientType) String() string {
    switch c {
        case TYPE_CLIENT:
            return "TYPE_CLIENT"
        case TYPE_WORKER:
            return "TYPE_WORKER"
    }
    panic("Unknow ClientType " + strconv.Itoa(int(c)))
}
