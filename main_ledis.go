// +build ledis

package main

import (
    "flag"
    "huabot-sched/db"
    "github.com/siddontang/ledisdb/config"
)

var sched *Sched

var configFile string
var sockFile string
var addr string


func init() {
    flag.StringVar(&sockFile, "sock", "huabot-sched.sock", "the sockFile")
    flag.StringVar(&configFile, "config", "config.json", "the ledis config filename")
    flag.StringVar(&addr, "addr", "127.0.0.1:3000", "api address")
    flag.Parse()
}


func main() {
    cfg, err := config.NewConfigWithFile(configFile)
    if err != nil {
        if configFile != "config.json" {
            panic(err)
        } else {
            cfg = config.NewConfigDefault()
        }
    }
    db.Connect(cfg)
    sched = NewSched(sockFile)
    go sched.Serve()
    StartHttpServer(addr, sched)
}
