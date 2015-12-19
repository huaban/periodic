package stat

import (
	"testing"
)

func TestFuncStat(t *testing.T) {
	var stat = NewFuncStat("test")
	stat.Worker.Incr()
	if stat.String() != "test,1,0,0" {
		t.Fatalf("FuncStat: except: test,1,0,0, got: %s\n", stat)
	}
}
