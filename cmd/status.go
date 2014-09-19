package cmd

import (
    "net"
    "bytes"
    "strings"
    "encoding/json"
    "periodic/sched"
    "fmt"
    "log"
)

func ShowStatus(entryPoint string) {
    parts := strings.SplitN(entryPoint, "://", 2)
    c, err := net.Dial(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    conn := sched.Conn{Conn: c}
    defer conn.Close()
    err = conn.Send(sched.TYPE_CLIENT.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    var msgId = []byte("100")
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(sched.NULL_CHAR)
    buf.Write(sched.STATUS.Bytes())
    err = conn.Send(buf.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    payload, err := conn.Receive()
    if err != nil {
        log.Fatal(err)
    }
    _parts := bytes.SplitN(payload, sched.NULL_CHAR, 2)
    if len(_parts) != 2 {
        err := fmt.Sprint("ParseCommand InvalId %v\n", payload)
        panic(err)
    }
    stats := make(map[string]sched.FuncStat)
    err = json.Unmarshal(_parts[1], &stats)
    if err != nil {
        log.Fatal(err)
    }
    for Func, stat := range stats {
        fmt.Printf("Func: %s\tWorker: %d\tJob: %d\tProcessing: %d\n", Func, stat.Worker, stat.Job, stat.Processing)
    }
}
