package driver

import (
	"fmt"
	"strings"
	"sync"
)

type MemStoreDriver struct {
	data      map[int64]*Job
	nameIndex map[string]int64
	lastId    int64
	locker    *sync.Mutex
}

func NewMemStroeDriver() *MemStoreDriver {
	mem := new(MemStoreDriver)
	mem.locker = new(sync.Mutex)
	mem.nameIndex = make(map[string]int64)
	mem.data = make(map[int64]*Job)
	mem.lastId = 0
	return mem
}

func (m *MemStoreDriver) Save(job *Job, force ...bool) (err error) {
	defer m.locker.Unlock()
	m.locker.Lock()
	if job.Id > 0 && (len(force) == 0 || !force[0]) {
		old, ok := m.data[job.Id]
		if !ok {
			return fmt.Errorf("Update Job %d fail, the job is not exists.", job.Id)
		}
		if old.Name != job.Name {
			delete(m.nameIndex, old.Func+":"+old.Name)
			m.nameIndex[job.Func+":"+job.Name] = job.Id
		}
	} else {
		m.lastId++
		job.Id = m.lastId
		m.nameIndex[job.Func+":"+job.Name] = job.Id
	}
	m.data[job.Id] = job
	return
}

func (m *MemStoreDriver) Delete(jobId int64) (err error) {
	defer m.locker.Unlock()
	m.locker.Lock()
	var job Job
	job, err = m.Get(jobId)
	if err != nil {
		return
	}
	delete(m.data, job.Id)
	delete(m.nameIndex, job.Func+":"+job.Name)
	return
}

func (m *MemStoreDriver) Get(jobId int64) (job Job, err error) {
	j, ok := m.data[jobId]
	if !ok {
		err = fmt.Errorf("Job %d not exists.", jobId)
		return
	}
	job = *j
	return
}

func (m *MemStoreDriver) GetOne(Func, name string) (job Job, err error) {
	jobId, ok := m.nameIndex[Func+":"+name]
	if !ok {
		err = fmt.Errorf("Job %s:%s not exists.", Func, name)
		return
	}
	job, err = m.Get(jobId)
	return
}

func (m *MemStoreDriver) NewIterator(Func []byte) JobIterator {
	m.locker.Lock()
	var data = make([]int64, 0)
	if Func == nil {
		for jobId, _ := range m.data {
			data = append(data, jobId)
		}
	} else {
		prefix := string(Func)
		for key, jobId := range m.nameIndex {
			if strings.HasPrefix(key, prefix) {
				data = append(data, jobId)
			}
		}
	}
	return &MemIterator{
		data:   data,
		m:      m,
		cursor: 0,
	}
}

func (m *MemStoreDriver) Close() error {
	return nil
}

type MemIterator struct {
	data   []int64
	m      *MemStoreDriver
	cursor int
	job    Job
	err    error
}

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

func (iter *MemIterator) Value() Job {
	return iter.job
}

func (iter *MemIterator) Error() error {
	return iter.err
}

func (iter *MemIterator) Close() {
	iter.m.locker.Unlock()
}
