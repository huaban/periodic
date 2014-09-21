package periodic

import (
    "fmt"
    "sync"
    "container/list"
)

// An Item is something we manage in a priority queue.
type Item struct {
    value    int64 // The value of the item; arbitrary.
    priority int64    // The priority of the item in the queue.
    // The index is needed by update and is maintained by the heap.Interface methods.
    index    int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
    // We want Pop to give us the lowest, not highest, priority so we use lesser than here.
    return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
    pq[i], pq[j] = pq[j], pq[i]
    pq[i].index = i
    pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
    n := len(*pq)
    item := x.(*Item)
    item.index = n
    *pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
    old := *pq
    n := len(old)
    item := old[n-1]
    item.index = -1 // for safety
    *pq = old[0 : n-1]
    return item
}


type GrabItem struct {
    w     *Worker
    msgId int64
}


func (item GrabItem) Has(Func string) bool {
    for _, F := range item.w.Funcs {
        if F == Func {
            return true
        }
    }
    return false
}


type GrabQueue struct {
    list   *list.List
    locker *sync.Mutex
}


func (g *GrabQueue) Push(item GrabItem) {
    defer g.locker.Unlock()
    g.locker.Lock()
    g.list.PushBack(item)
}


func (g *GrabQueue) Get(Func string) (item GrabItem, err error) {
    defer g.locker.Unlock()
    g.locker.Lock()
    for e := g.list.Front(); e != nil; e = e.Next() {
        item = e.Value.(GrabItem)
        if item.Has(Func) {
            return
        }
    }
    err = fmt.Errorf("func name: %s not found.", Func)
    return
}


func (g *GrabQueue) Remove(item GrabItem) {
    defer g.locker.Unlock()
    g.locker.Lock()
    for e := g.list.Front(); e != nil; e = e.Next() {
        item1 := e.Value.(GrabItem)
        if item1 == item {
            g.list.Remove(e)
        }
    }
}


func (g *GrabQueue) RemoveWorker(worker *Worker) {
    defer g.locker.Unlock()
    g.locker.Lock()
    for e := g.list.Front(); e != nil; e = e.Next() {
        item := e.Value.(GrabItem)
        if item.w == worker {
            g.list.Remove(e)
        }
    }
}


func (g GrabQueue) Len() int {
    return g.list.Len()
}


func NewGrabQueue() *GrabQueue {
    g := new(GrabQueue)
    g.list = list.New()
    g.locker = new(sync.Mutex)
    return g
}
