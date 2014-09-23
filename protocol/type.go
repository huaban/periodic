package protocol

import (
    "bytes"
    "strconv"
)

type ClientType int

const (
    TYPE_CLIENT ClientType = iota + 1
    TYPE_WORKER
)


func (c ClientType) Bytes() []byte {
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(c))
    return buf.Bytes()
}


func (c ClientType) String() string {
    switch c {
        case 1:
            return "TYPE_CLIENT"
        case 2:
            return "TYPE_WORKER"
    }
    panic("Unknow ClientType " + strconv.Itoa(int(c)))
}
