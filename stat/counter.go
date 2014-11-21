package stat

import (
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
