package cmd


import (
    "net"
    "strings"
    "encoding/json"
    "periodic/sched"
    "fmt"
    "log"
    "bytes"
)


func SubmitJob(entryPoint string, job sched.Job) {
    parts := strings.SplitN(entryPoint, "://", 2)
    c, err := net.Dial(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    conn := sched.Conn{Conn: c}
    defer conn.Close()
    err = conn.Send(sched.PackCmd(sched.TYPE_CLIENT))
    if err != nil {
        log.Fatal(err)
    }
    var data []byte
    data, _ = json.Marshal(job)
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(sched.SUBMIT_JOB))
    buf.Write(sched.NULL_CHAR)
    buf.Write(data)
    err = conn.Send(buf.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    payload, err := conn.Receive()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%s\n", payload)
}
