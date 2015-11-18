package driver

import (
	"encoding/json"
)

// Define job status.
const (
	JOB_STATUS_READY = "ready"
	JOB_STATUS_PROC  = "processing"
)

// Job workload.
type Job struct {
	ID int64 `json:"job_id"`
	// The job name, this is unique.
	Name string `json:"name"`
	// The job function reffer on worker function
	Func string `json:"func"`
	// Job args
	Args string `json:"workload"`
	// Job processing timeout
	Timeout int64 `json:"timeout"`
	// When to sched the job.
	SchedAt int64 `json:"sched_at"`
	// The job is start at
	RunAt  int64  `json:"run_at"`
	Status string `json:"status"`
}

// NewJob create a job from json bytes
func NewJob(payload []byte) (job Job, err error) {
	err = json.Unmarshal(payload, &job)
	return
}

// Bytes encode job to json bytes
func (job Job) Bytes() (data []byte) {
	data, _ = json.Marshal(job)
	return
}
