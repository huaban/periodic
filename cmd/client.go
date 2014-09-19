package cmd


import (
    "net"
    "strings"
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
    err = conn.Send(sched.TYPE_CLIENT.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    var msgId = []byte("100")
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(sched.NULL_CHAR)
    buf.WriteByte(byte(sched.SUBMIT_JOB))
    buf.Write(sched.NULL_CHAR)
    buf.Write(job.Bytes())
    err = conn.Send(buf.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    payload, err := conn.Receive()
    if err != nil {
        log.Fatal(err)
    }
    _, cmd, _ := sched.ParseCommand(payload)
    fmt.Printf("%s\n", cmd.String())
}
