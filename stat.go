package periodic

import (
    "fmt"
    "sync"
    "strconv"
)

type Counter struct {
    c int
    locker *sync.Mutex
}


func NewCounter(c int) *Counter {
    var counter = new(Counter)
    counter.c = c
    counter.locker = new(sync.Mutex)
    return counter
}


func (c *Counter) Incr() {
    defer c.locker.Unlock()
    c.locker.Lock()
    c.c = c.c + 1
}


func (c *Counter) Decr() {
    defer c.locker.Unlock()
    c.locker.Lock()
    c.c = c.c - 1
    if c.c < 0 {
        c.c = 0
    }
}


func (c *Counter) String() string {
    defer c.locker.Unlock()
    c.locker.Lock()
    return strconv.Itoa(c.c)
}


func (c *Counter) Int() int {
    defer c.locker.Unlock()
    c.locker.Lock()
    return c.c
}


type FuncStat struct {
    Name       string
    Worker     *Counter
    Job        *Counter
    Processing *Counter
}


func NewFuncStat(name string) *FuncStat {
    var stat = new(FuncStat)
    stat.Name = name
    stat.Worker = NewCounter(0)
    stat.Job = NewCounter(0)
    stat.Processing = NewCounter(0)
    return stat
}


func (stat FuncStat) String() string {
    return fmt.Sprintf("%s,%s,%s,%s", stat.Name, stat.Worker, stat.Job, stat.Processing)
}
