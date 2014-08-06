package main

import (
    "log"
    "net"
    "os"
)

var sched *Sched

const SOCK_FILE = "huabot-sched.sock"

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

    _, err := os.Stat(SOCK_FILE)

    if err == nil || os.IsExist(err) {
        _, err = net.Dial("unix", SOCK_FILE)
        if err == nil {
            panic("Huabot-sched is already started.")
        }
        os.Remove(SOCK_FILE)
    }
    listen, err := net.Listen("unix", "huabot-sched.sock")
    log.Printf("Started at huabot-sched.sock")
    if err != nil {
        log.Fatal(err)
    }
    handleAccept(listen)
}
