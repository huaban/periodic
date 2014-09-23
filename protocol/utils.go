package protocol

import (
    "fmt"
    "bytes"
    "strconv"
)

var NULL_CHAR = []byte("\x00\x01")


func ParseCommand(payload []byte) (msgId int64, cmd Command, data []byte) {
    parts := bytes.SplitN(payload, NULL_CHAR, 3)
    if len(parts) == 1 {
        err := fmt.Sprint("ParseCommand InvalId %v\n", payload)
        panic(err)
    }
    msgId, _ = strconv.ParseInt(string(parts[0]), 10, 0)
    cmd = Command(parts[1][0])
    if len(parts) == 3 {
        data = parts[2]
    }
    return
}
