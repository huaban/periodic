package cmd


import (
    "net"
    "strings"
    "periodic/sched"
    "fmt"
    "log"
    "bytes"
)


func DropFunc(entryPoint, Func string) {
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
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(sched.DROP_FUNC))
    buf.Write(sched.NULL_CHAR)
    buf.WriteString(Func)
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
