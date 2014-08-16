// +build !ledis

package main

import (
    "flag"
    "huabot-sched/db"
)

var sched *Sched

var redisPort string
var sockFile string
var addr string


func init() {
    flag.StringVar(&sockFile, "sock", "huabot-sched.sock", "the sockFile")
    flag.StringVar(&redisPort, "redis", "127.0.0.1:6379", "redis server")
    flag.StringVar(&addr, "addr", "127.0.0.1:3000", "api address")
    flag.Parse()
}


func main() {
    db.Connect(redisPort)
    sched = NewSched(sockFile)
    go sched.Serve()
    StartHttpServer(addr, sched)
}
