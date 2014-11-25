package main

import (
    "os"
    "log"
    "time"
    "runtime"
    "os/signal"
    "github.com/Lupino/periodic/driver"
    "github.com/Lupino/periodic/driver/redis"
    "github.com/Lupino/periodic/driver/leveldb"
    "github.com/Lupino/periodic"
    "github.com/Lupino/periodic/cmd/periodic/subcmd"
    "github.com/codegangsta/cli"
    "runtime/pprof"
)


func main() {
    app := cli.NewApp()
    app.Name = "periodic"
    app.Usage = "Periodic task system"
    app.Version = periodic.Version
    app.Flags = []cli.Flag {
        cli.StringFlag{
            Name: "H",
            Value: "unix:///tmp/periodic.sock",
            Usage: "the server address eg: tcp://127.0.0.1:5000",
            EnvVar: "PERIODIC_PORT",
        },
        cli.StringFlag{
            Name: "redis",
            Value: "tcp://127.0.0.1:6379",
            Usage: "The redis server address, required for driver redis",
        },
        cli.StringFlag{
            Name: "driver",
            Value: "memstore",
            Usage: "The driver [memstore, leveldb, redis]",
        },
        cli.StringFlag{
            Name: "dbpath",
            Value: "leveldb",
            Usage: "The db path, required for driver leveldb",
        },
        cli.BoolFlag{
            Name: "d",
            Usage: "Enable daemon mode",
        },
        cli.IntFlag{
            Name: "timeout",
            Value: 0,
            Usage: "The socket timeout",
        },
        cli.IntFlag{
            Name: "cpus",
            Value: runtime.NumCPU(),
            Usage: "The runtime.GOMAXPROCS",
            EnvVar: "GOMAXPROCS",
        },
        cli.StringFlag{
            Name: "cpuprofile",
            Value: "",
            Usage: "write cpu profile to file",
        },
    }
    app.Commands = []cli.Command{
        {
            Name: "status",
            Usage: "Show status",
            Action: func(c *cli.Context) {
                subcmd.ShowStatus(c.GlobalString("H"))
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
                    Value: 0,
                    Usage: "job running timeout",
                },
                cli.IntFlag{
                    Name: "sched_later",
                    Value: 0,
                    Usage: "job sched_later",
                },
            },
            Action: func(c *cli.Context) {
                var job = driver.Job{
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
                subcmd.SubmitJob(c.GlobalString("H"), job)
            },
        },
        {
            Name: "remove",
            Usage: "Remove job",
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
            },
            Action: func(c *cli.Context) {
                var job = driver.Job{
                    Name: c.String("n"),
                    Func: c.String("f"),
                }
                if len(job.Name) == 0 || len(job.Func) == 0 {
                    cli.ShowCommandHelp(c, "remove")
                    log.Fatal("Job name and func is require")
                }
                subcmd.RemoveJob(c.GlobalString("H"), job)
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
                subcmd.DropFunc(c.GlobalString("H"), Func)
            },
        },
        {
            Name: "run",
            Usage: "Run func",
            Flags: []cli.Flag {
                cli.StringFlag{
                    Name: "f",
                    Value: "",
                    Usage: "function name required",
                },
                cli.StringFlag{
                    Name: "exec",
                    Value: "",
                    Usage: "command required",
                },
            },
            Action: func(c *cli.Context) {
                Func := c.String("f")
                exec := c.String("exec")
                if len(Func) == 0 {
                    cli.ShowCommandHelp(c, "run")
                    log.Fatal("function name is required")
                }
                if len(exec) == 0 {
                    cli.ShowCommandHelp(c, "run")
                    log.Fatal("command is required")
                }
                subcmd.Run(c.GlobalString("H"), Func, exec)
            },
        },
    }
    app.Action = func(c *cli.Context) {
        if c.Bool("d") {
            if c.String("cpuprofile") != "" {
                f, err := os.Create(c.String("cpuprofile"))
                if err != nil {
                    log.Fatal(err)
                }
                pprof.StartCPUProfile(f)
                defer pprof.StopCPUProfile()
            }
            var store driver.StoreDriver
            switch c.String("driver") {
                case "memstore":
                    store = driver.NewMemStroeDriver()
                    break
                case "redis":
                    store = redis.NewRedisDriver(c.String("redis"))
                    break
                case "leveldb":
                    store = leveldb.NewLevelDBDriver(c.String("dbpath"))
                    break
                default:
                    store = driver.NewMemStroeDriver()
                    break
            }

            runtime.GOMAXPROCS(c.Int("cpus"))
            timeout := time.Duration(c.Int("timeout"))
            periodicd := periodic.NewSched(c.String("H"), store, timeout)
            go periodicd.Serve()
            s := make(chan os.Signal, 1)
            signal.Notify(s, os.Interrupt, os.Kill)
            <-s
            periodicd.Close()
        } else {
            cli.ShowAppHelp(c)
        }
    }

    app.Run(os.Args)
}
