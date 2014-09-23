package periodic

type Counter uint

func (c *Counter) Incr() {
    *c = *c + 1
}


func (c *Counter) Decr() {
    *c = *c - 1
}


type FuncStat struct {
    Worker     Counter `json:"worker_count"`
    Job        Counter `json:"job_count"`
    Processing Counter `json:"processing"`
}
