package main

import (
    "os"
    "log"
    "time"
    "os/signal"
    "huabot-sched/drivers"
    sch "huabot-sched/sched"
    "huabot-sched/cmd"
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
            Usage: "The redis server address, required driver redis",
        },
        cli.StringFlag{
            Name: "driver",
            Value: "leveldb",
            Usage: "The driver driver [leveldb, redis]",
        },
        cli.StringFlag{
            Name: "dbpath",
            Value: "leveldb",
            Usage: "The db path, required driver leveldb",
        },
        cli.BoolFlag{
            Name: "d",
            Usage: "Enable daemon mode",
        },
    }
    app.Commands = []cli.Command{
        {
            Name: "status",
            Usage: "Show status",
            Action: func(c *cli.Context) {
                cmd.ShowStatus(c.GlobalString("H"))
            },
        },
        {
            Name: "submit",
            Usage: "Submit job",
            Flags: []cli.Flag {
                cli.StringFlag{
                    Name: "f",
                    Value: "",
                    Usage: "function name",
                },
                cli.StringFlag{
                    Name: "n",
                    Value: "",
                    Usage: "job name",
                },
                cli.StringFlag{
                    Name: "args",
                    Value: "",
                    Usage: "job workload",
                },
                cli.IntFlag{
                    Name: "t",
                    Value: 500,
                    Usage: "job running timeout",
                },
                cli.IntFlag{
                    Name: "sched_later",
                    Value: 0,
                    Usage: "job sched_later",
                },
            },
            Action: func(c *cli.Context) {
                var job = sch.Job{
                    Name: c.String("n"),
                    Func: c.String("f"),
                    Args: c.String("args"),
                    Timeout: int64(c.Int("t")),
                }
                if len(job.Name) == 0 || len(job.Func) == 0 {
                    cli.ShowCommandHelp(c, "submit")
                    log.Fatal("Job name and func is require")
                }
                delay := c.Int("sched_later")
                var now = time.Now()
                job.SchedAt = int64(now.Unix()) + int64(delay)
                cmd.SubmitJob(c.GlobalString("H"), job)
            },
        },
        {
            Name: "drop",
            Usage: "Drop func",
            Flags: []cli.Flag {
                cli.StringFlag{
                    Name: "f",
                    Value: "",
                    Usage: "function name",
                },
            },
            Action: func(c *cli.Context) {
                Func := c.String("f")
                if len(Func) == 0 {
                    cli.ShowCommandHelp(c, "drop")
                    log.Fatal("function name is required")
                }
                cmd.DropFunc(c.GlobalString("H"), Func)
            },
        },
    }
    app.Action = func(c *cli.Context) {
        if c.Bool("d") {
            var st sch.StoreDriver
            if c.String("driver") == "redis" {
                st = drivers.NewRedisDriver(c.String("redis"))
            } else {
                st = drivers.NewLevelDBDriver(c.String("dbpath"))
            }

            sched := sch.NewSched(c.String("H"), st)
            go sched.Serve()
            s := make(chan os.Signal, 1)
            signal.Notify(s, os.Interrupt, os.Kill)
            <-s
            if c.String("driver") == "leveldb" {
                err := st.Close()
                if err != nil {
                    log.Fatal(err)
                }
            }
        } else {
            cli.ShowAppHelp(c)
        }
    }

    app.Run(os.Args)
}
