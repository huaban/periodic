package main

import (
    "flag"
    "huabot-sched/db"
)

var sched *Sched

var redisPort string
var sockFile string


func init() {
    flag.StringVar(&sockFile, "sock", "huabot-sched.sock", "the sockFile")
    flag.StringVar(&redisPort, "redis", "127.0.0.1:6379", "redis server")
    flag.Parse()
}


func main() {
    db.Connect(redisPort)
    sched = NewSched(sockFile)
    sched.Serve()
}
