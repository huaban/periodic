package periodic

import (
    "fmt"
    "testing"
)


func TestCounter(t *testing.T) {
    var c = NewCounter(1)
    c.Incr()
    fmt.Printf("c: %s\n", c)
    if c.c != 2 {
        t.Fatalf("counter: except: 2, got: %d\n", c)
    }
    c.Decr()
    c.Decr()
    c.Decr()
    if c.c != 0 {
        t.Fatalf("counter: except: 0, got: %d\n", c)
    }
    c.Decr()
    if c.c != 0 {
        t.Fatalf("counter: except: 0, got: %d\n", c)
    }
    c.Decr()
    if c.c != 0 {
        t.Fatalf("counter: except: 0, got: %d\n", c)
    }
}


func TestFuncStat(t *testing.T) {
    var stat = NewFuncStat("test")
    stat.Worker.Incr()
    if stat.String() != "test,1,0,0" {
        t.Fatalf("FuncStat: except: test,1,0,0, got: %s\n", stat)
    }
}
