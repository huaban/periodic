package driver

import (
	"fmt"
	"strings"
	"sync"
)

// MemStoreDriver defined a memory store driver
type MemStoreDriver struct {
	data      map[int64]*Job
	nameIndex map[string]int64
	lastID    int64
	locker    *sync.Mutex
}

// NewMemStroeDriver create a memory store driver
func NewMemStroeDriver() *MemStoreDriver {
	mem := new(MemStoreDriver)
	mem.locker = new(sync.Mutex)
	mem.nameIndex = make(map[string]int64)
	mem.data = make(map[int64]*Job)
	mem.lastID = 0
	return mem
}

// Save job. when job is exists update it, other create one.
func (m *MemStoreDriver) Save(job *Job, force ...bool) (err error) {
	defer m.locker.Unlock()
	m.locker.Lock()
	if job.ID > 0 && (len(force) == 0 || !force[0]) {
		old, ok := m.data[job.ID]
		if !ok {
			return fmt.Errorf("Update Job %d fail, the job is not exists.", job.ID)
		}
		if old.Name != job.Name {
			delete(m.nameIndex, old.Func+":"+old.Name)
			m.nameIndex[job.Func+":"+job.Name] = job.ID
		}
	} else {
		m.lastID++
		job.ID = m.lastID
		m.nameIndex[job.Func+":"+job.Name] = job.ID
	}
	m.data[job.ID] = job
	return
}

// Delete a job with job id.
func (m *MemStoreDriver) Delete(jobID int64) (err error) {
	defer m.locker.Unlock()
	m.locker.Lock()
	var job Job
	job, err = m.Get(jobID)
	if err != nil {
		return
	}
	delete(m.data, job.ID)
	delete(m.nameIndex, job.Func+":"+job.Name)
	return
}

// Get a job with job id.
func (m *MemStoreDriver) Get(jobID int64) (job Job, err error) {
	j, ok := m.data[jobID]
	if !ok {
		err = fmt.Errorf("Job %d not exists.", jobID)
		return
	}
	job = *j
	return
}

// GetOne get a job with func and name.
func (m *MemStoreDriver) GetOne(Func, name string) (job Job, err error) {
	jobID, ok := m.nameIndex[Func+":"+name]
	if !ok {
		err = fmt.Errorf("Job %s:%s not exists.", Func, name)
		return
	}
	job, err = m.Get(jobID)
	return
}

// NewIterator create a job Iterator with func or nil.
func (m *MemStoreDriver) NewIterator(Func []byte) Iterator {
	m.locker.Lock()
	var data = make([]int64, 0)
	if Func == nil {
		for jobID := range m.data {
			data = append(data, jobID)
		}
	} else {
		prefix := string(Func)
		for key, jobID := range m.nameIndex {
			if strings.HasPrefix(key, prefix) {
				data = append(data, jobID)
			}
		}
	}
	return &MemIterator{
		data:   data,
		m:      m,
		cursor: 0,
	}
}

// Close the driver
func (m *MemStoreDriver) Close() error {
	return nil
}

// MemIterator define a memory store driver iterator
type MemIterator struct {
	data   []int64
	m      *MemStoreDriver
	cursor int
	job    Job
	err    error
}

// Next advances the iterator to the next value, which will then be available through
// then the Value method. It returns false if no further advancement is possible.
func (iter *MemIterator) Next() bool {
	if len(iter.data) <= iter.cursor {
		return false
	}
	iter.job, iter.err = iter.m.Get(iter.data[iter.cursor])
	if iter.err != nil {
		return false
	}
	iter.cursor++
	return true
}

// Value returns the current job.
func (iter *MemIterator) Value() Job {
	return iter.job
}

// Error returns the current error.
func (iter *MemIterator) Error() error {
	return iter.err
}

// Close the iterator
func (iter *MemIterator) Close() {
	iter.m.locker.Unlock()
}
