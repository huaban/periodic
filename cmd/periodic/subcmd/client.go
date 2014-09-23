package subcmd


import (
    "net"
    "strings"
    "github.com/Lupino/periodic"
    "github.com/Lupino/periodic/driver"
    "fmt"
    "log"
    "bytes"
)


func SubmitJob(entryPoint string, job driver.Job) {
    parts := strings.SplitN(entryPoint, "://", 2)
    c, err := net.Dial(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    conn := periodic.Conn{Conn: c}
    defer conn.Close()
    err = conn.Send(periodic.TYPE_CLIENT.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    var msgId = []byte("100")
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(periodic.NULL_CHAR)
    buf.WriteByte(byte(periodic.SUBMIT_JOB))
    buf.Write(periodic.NULL_CHAR)
    buf.Write(job.Bytes())
    err = conn.Send(buf.Bytes())
    if err != nil {
        log.Fatal(err)
    }
    payload, err := conn.Receive()
    if err != nil {
        log.Fatal(err)
    }
    _, cmd, _ := periodic.ParseCommand(payload)
    fmt.Printf("%s\n", cmd.String())
}
