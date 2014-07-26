package main

import (
    "log"
    "net"
)

var sched *Sched


func handleAccept(listen net.Listener) {
    defer listen.Close()
    for {
        conn, err := listen.Accept()
        if err != nil {
            log.Fatal(err)
        }
        sched.NewConnectioin(conn)
    }
}


func main() {
    Connect("127.0.0.1:6379")
    sched = NewSched()
    sched.Start()
    listen, err := net.Listen("unix", "huabot-sched.sock")
    log.Printf("Started at huabot-sched.sock")
    if err != nil {
        log.Fatal(err)
    }
    handleAccept(listen)
}
