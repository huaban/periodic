package cmd

import (
    "net"
    "strings"
    "encoding/json"
    "huabot-sched/sched"
    "fmt"
)

func ShowStatus(entryPoint string) {
    parts := strings.SplitN(entryPoint, "://", 2)
    c, err := net.Dial(parts[0], parts[1])
    if err != nil {
        panic(err)
    }
    conn := sched.Conn{Conn: c}
    defer conn.Close()
    err = conn.Send(sched.PackCmd(sched.TYPE_CLIENT))
    if err != nil {
        panic(err)
    }
    err = conn.Send(sched.PackCmd(sched.STATUS))
    if err != nil {
        panic(err)
    }
    payload, err := conn.Receive()
    if err != nil {
        panic(err)
    }
    stats := make(map[string]sched.FuncStat)
    err = json.Unmarshal(payload, &stats)
    if err != nil {
        panic(err)
    }
    for Func, stat := range stats {
        fmt.Printf("Func: %s\tWorker: %d\tJob: %d\tProcessing: %d\n", Func, stat.Worker, stat.Job, stat.Processing)
    }
}
