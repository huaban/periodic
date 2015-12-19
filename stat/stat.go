package stat

import (
	"fmt"
)

// FuncStat defined func stat
type FuncStat struct {
	Name       string
	Worker     *Counter
	Job        *Counter
	Processing *Counter
}

// NewFuncStat create a func stat
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
