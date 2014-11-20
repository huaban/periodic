package protocol

import (
    "fmt"
    "bytes"
    "errors"
    "strconv"
)

var NULL_CHAR = []byte("\x00\x01")


func ParseCommand(payload []byte) (msgId int64, cmd Command, data []byte) {
    parts := bytes.SplitN(payload, NULL_CHAR, 3)
    var err = fmt.Sprintf("InvalId %v\n", payload)
    if len(parts) == 1 {
        panic(err)
    }
    msgId, _ = strconv.ParseInt(string(parts[0]), 10, 0)
    if len(parts[1]) != 1 {
        panic(err)
    }
    cmd = Command(parts[1][0])
    if len(parts) == 3 && len(parts) > 0 {
        data = parts[2]
    } else {
        panic(err)
    }
    return
}


func makeHeader(data []byte) ([]byte, error) {
    header := make([]byte, 4)

    length := uint32(len(data))

    if length > 0x7fffffff {
        return nil, errors.New("Data to large")
    }

    header[0] = byte((length >> 24) & 0xff)
    header[1] = byte((length >> 16) & 0xff)
    header[2] = byte((length >> 8) & 0xff)
    header[3] = byte((length >> 0) & 0xff)

    return header, nil
}


func parseHeader(header []byte) (uint32) {
    length := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])
    length = length & ^uint32(0x80000000)

    return length
}
