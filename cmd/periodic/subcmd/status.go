package subcmd

import (
    "net"
    "bytes"
    "strings"
    "github.com/Lupino/periodic"
    "github.com/Lupino/periodic/protocol"
    "fmt"
    "log"
)

func ShowStatus(entryPoint string) {
    parts := strings.SplitN(entryPoint, "://", 2)
    c, err := net.Dial(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    conn := periodic.Conn{Conn: c}
    defer conn.Close()
    err = conn.Send(protocol.TYPE_CLIENT.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    var msgId = []byte("100")
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(protocol.NULL_CHAR)
    buf.Write(protocol.STATUS.Bytes())
    err = conn.Send(buf.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    payload, err := conn.Receive()
    if err != nil {
        log.Fatal(err)
    }
    _parts := bytes.SplitN(payload, protocol.NULL_CHAR, 2)
    if len(_parts) != 2 {
        err := fmt.Sprint("ParseCommand InvalId %v\n", payload)
        panic(err)
    }
    stats := strings.Split(string(_parts[1]), "\n")
    for _, stat := range stats {
        line := strings.Split(stat, ",")
        fmt.Printf("Func: %s\tWorker: %d\tJob: %d\tProcessing: %d\n",
                   line[0], line[1], line[2], line[3])
    }
}
