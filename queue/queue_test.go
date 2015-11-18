package queue

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestQueue(t *testing.T) {
	items := map[int64]int64{
		1: 2, 2: 3, 4: 2, 5: 1, 6: 5,
	}
	pq := make(PriorityQueue, len(items))
	i := 0
	for value, priority := range items {
		pq[i] = &Item{
			Value:    value,
			Priority: priority,
			Index:    i,
		}
		i++
	}
	heap.Init(&pq)
	item := &Item{
		Value:    7,
		Priority: 4,
	}
	heap.Push(&pq, item)
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		fmt.Printf("%d ", item.Priority)
	}
}
