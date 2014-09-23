package periodic

import (
    "fmt"
    "testing"
)


func TestCounter(t *testing.T) {
    var c Counter = 1
    c.Incr()
    fmt.Printf("c: %d\n", c)
    if c != 2 {
        t.Fatalf("counter: except: 2, got: %d\n", c)
    }
    c.Decr()
    c.Decr()
    c.Decr()
    if c != 0 {
        t.Fatalf("counter: except: 0, got: %d\n", c)
    }
    c.Decr()
    if c != 0 {
        t.Fatalf("counter: except: 0, got: %d\n", c)
    }
    c.Decr()
    if c != 0 {
        t.Fatalf("counter: except: 0, got: %d\n", c)
    }
}
