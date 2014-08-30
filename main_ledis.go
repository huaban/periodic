// +build ledis

package main

import (
    "flag"
    "huabot-sched/db"
    "huabot-sched/store"
    sch "huabot-sched/sched"
    "github.com/siddontang/ledisdb/config"
)

var sched *sch.Sched

var configFile string
var entryPoint string


func init() {
    flag.StringVar(&entryPoint, "H", "unix://huabot-sched.sock", "host eg: tcp://127.0.0.1:5000")
    flag.StringVar(&configFile, "config", "config.json", "the ledis config filename")
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
    sched = sch.NewSched(entryPoint, store.RedisStorer{})
    sched.Serve()
}
