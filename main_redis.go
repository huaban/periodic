package main

import (
    "os"
    "huabot-sched/db"
    "huabot-sched/store"
    sch "huabot-sched/sched"
    "github.com/codegangsta/cli"
)


func main() {
    app := cli.NewApp()
    app.Name = "huabot-sched"
    app.Usage = ""
    app.Version = "0.0.1"
    app.Flags = []cli.Flag {
        cli.StringFlag{
            Name: "H",
            Value: "unix://huabot-sched.sock",
            Usage: "the server address eg: tcp://127.0.0.1:5000",
            EnvVar: "HUABOT_SCHED_PORT",
        },
        cli.StringFlag{
            Name: "redis",
            Value: "tcp://127.0.0.1:6379",
            Usage: "the redis server address",
            EnvVar: "REDIS_PORT",
        },
        cli.BoolFlag{
            Name: "d",
            Usage: "Enable daemon mode",
        },
    }
    app.Action = func(c *cli.Context) {
        if c.Bool("d") {
            db.Connect(c.String("redis"))
            sched := sch.NewSched(c.String("H"), store.RedisStorer{})
            sched.Serve()
        } else {
            cli.ShowAppHelp(c)
        }
    }

    app.Run(os.Args)
}
