package queue

// An Item is something we manage in a priority queue.
type Item struct {
    Value    int64 // The value of the item; arbitrary.
    Priority int64    // The priority of the item in the queue.
    // The index is needed by update and is maintained by the heap.Interface methods.
    Index    int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
    // We want Pop to give us the lowest, not highest, priority so we use lesser than here.
    return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
    pq[i], pq[j] = pq[j], pq[i]
    pq[i].Index = i
    pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
    n := len(*pq)
    item := x.(*Item)
    item.Index = n
    *pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
    old := *pq
    n := len(old)
    item := old[n-1]
    item.Index = -1 // for safety
    *pq = old[0 : n-1]
    return item
}
