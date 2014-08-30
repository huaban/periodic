// +build !ledis

package main

import (
    "flag"
    "huabot-sched/db"
    "huabot-sched/store"
    sch "huabot-sched/sched"
)

var sched *sch.Sched

var redisPort string
var entryPoint string
var addr string


func init() {
    flag.StringVar(&entryPoint, "H", "unix://huabot-sched.sock", "host eg: tcp://127.0.0.1:5000")
    flag.StringVar(&redisPort, "redis", "127.0.0.1:6379", "redis server")
    flag.StringVar(&addr, "addr", "", "the http api address")
    flag.Parse()
}


func main() {
    db.Connect(redisPort)
    sched = sch.NewSched(entryPoint, store.RedisStorer{})
    if len(addr) > 0 {
        go sched.Serve()
        sch.StartHttpServer(addr, sched)
    } else {
        sched.Serve()
    }
}
