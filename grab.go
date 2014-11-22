package periodic

import (
    "fmt"
    "sync"
    "bytes"
    "container/list"
)

type grabItem struct {
    w     *worker
    msgId []byte
}


func (item grabItem) has(Func string) bool {
    for _, F := range item.w.funcs {
        if F == Func {
            return true
        }
    }
    return false
}


func (item grabItem) equal(item1 grabItem) bool {
    if item1.w == item.w && bytes.Equal(item.msgId, item1.msgId) {
        return true
    }
    return false
}


type grabQueue struct {
    list   *list.List
    locker *sync.Mutex
}


func (g *grabQueue) push(item grabItem) {
    defer g.locker.Unlock()
    g.locker.Lock()
    g.list.PushBack(item)
}


func (g *grabQueue) get(Func string) (item grabItem, err error) {
    defer g.locker.Unlock()
    g.locker.Lock()
    for e := g.list.Front(); e != nil; e = e.Next() {
        item = e.Value.(grabItem)
        if item.has(Func) {
            return
        }
    }
    err = fmt.Errorf("func name: %s not found.", Func)
    return
}


func (g *grabQueue) remove(item grabItem) {
    defer g.locker.Unlock()
    g.locker.Lock()
    for e := g.list.Front(); e != nil; e = e.Next() {
        item1 := e.Value.(grabItem)
        if item.equal(item1) {
            g.list.Remove(e)
        }
    }
}


func (g *grabQueue) removeWorker(w *worker) {
    defer g.locker.Unlock()
    g.locker.Lock()
    for e := g.list.Front(); e != nil; e = e.Next() {
        item := e.Value.(grabItem)
        if item.w == w {
            g.list.Remove(e)
        }
    }
}


func (g grabQueue) len() int {
    defer g.locker.Unlock()
    g.locker.Lock()
    return g.list.Len()
}


func newGrabQueue() *grabQueue {
    g := new(grabQueue)
    g.list = list.New()
    g.locker = new(sync.Mutex)
    return g
}
