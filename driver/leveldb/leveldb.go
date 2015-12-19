package leveldb

import (
	"fmt"
	"github.com/Lupino/periodic/driver"
	"github.com/golang/groupcache/lru"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"log"
	"os"
	"strconv"
	"sync"
)

// PREJOB prefix job key
const PREJOB = "job:"

// PREFUNC perfix func key
const PREFUNC = "func:"

// PRESEQUENCE prefix sequence key
const PRESEQUENCE = "sequence:"

// Driver define leveldb store driver
type Driver struct {
	db       *leveldb.DB
	RWLocker *sync.Mutex
	cache    *lru.Cache
}

// NewDriver create a leveldb store driver
func NewDriver(dbpath string) Driver {
	var db *leveldb.DB
	var err error
	var cache *lru.Cache

	_, err = os.Stat(dbpath)

	if err == nil || os.IsExist(err) {
		db, err = leveldb.RecoverFile(dbpath, nil)
	} else {
		db, err = leveldb.OpenFile(dbpath, nil)
	}
	if err != nil {
		log.Fatal(err)
	}
	cache = lru.New(1000)
	var RWLocker = new(sync.Mutex)
	return Driver{
		db:       db,
		cache:    cache,
		RWLocker: RWLocker,
	}
}

// Save job. when job is exists update it, other create one.
func (l Driver) Save(job *driver.Job, force ...bool) (err error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	batch := new(leveldb.Batch)
	var isNew = true
	if job.ID > 0 {
		isNew = false
	} else {
		lastID, e := l.db.Get([]byte(PRESEQUENCE+"JOB"), nil)
		if e != nil || lastID == nil {
			job.ID = 1
		} else {
			id, _ := strconv.ParseInt(string(lastID), 10, 64)
			job.ID = id + 1
		}
	}
	var strID = strconv.FormatInt(job.ID, 10)
	if isNew {
		batch.Put([]byte(PRESEQUENCE+"JOB"), []byte(strID))
		batch.Put([]byte(PREFUNC+job.Func+":"+job.Name), []byte(strID))
	} else if len(force) == 0 || !force[0] {
		old, e := l.get(job.ID)
		if e != nil || old.ID == 0 {
			err = fmt.Errorf("Update Job %d fail, the old job is not exists.", job.ID)
			return
		}
		l.cache.Remove(PREJOB + strID)
		if old.Name != job.Name {
			batch.Delete([]byte(PREFUNC + job.Func + ":" + old.Name))
			batch.Put([]byte(PREFUNC+job.Func+":"+job.Name), []byte(strID))
		}
	}
	batch.Put([]byte(PREJOB+strID), job.Bytes())
	err = l.db.Write(batch, nil)
	return
}

// Delete a job with job id.
func (l Driver) Delete(jobID int64) (err error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	var job driver.Job
	batch := new(leveldb.Batch)
	job, err = l.get(jobID)
	if err != nil {
		return
	}
	var strID = strconv.FormatInt(job.ID, 10)
	batch.Delete([]byte(PREFUNC + job.Func + ":" + job.Name))
	batch.Delete([]byte(PREJOB + strID))
	err = l.db.Write(batch, nil)
	l.cache.Remove(PREJOB + strID)
	return
}

// Get a job with job id.
func (l Driver) Get(jobID int64) (driver.Job, error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	return l.get(jobID)
}

func (l Driver) get(jobID int64) (job driver.Job, err error) {
	var data []byte
	var key = PREJOB + strconv.FormatInt(jobID, 10)
	if val, hit := l.cache.Get(key); hit {
		return val.(driver.Job), nil
	}
	data, err = l.db.Get([]byte(key), nil)
	if err != nil {
		return
	}
	job, err = driver.NewJob(data)
	if err == nil {
		l.cache.Add(key, job)
	}
	return
}

// GetOne get a job with func and name.
func (l Driver) GetOne(Func, name string) (job driver.Job, err error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	var data []byte
	var key = PREFUNC + Func + ":" + name
	data, err = l.db.Get([]byte(key), nil)
	if err != nil {
		return
	}
	key = PREJOB + string(data)
	if val, hit := l.cache.Get(key); hit {
		return val.(driver.Job), nil
	}
	data, err = l.db.Get([]byte(key), nil)
	if err != nil {
		return
	}
	job, err = driver.NewJob(data)
	if err == nil {
		l.cache.Add(key, job)
	}
	return
}

// NewIterator create a job Iterator with func or nil.
func (l Driver) NewIterator(Func []byte) driver.Iterator {
	var prefix []byte
	if Func == nil {
		prefix = []byte(PREJOB)
	} else {
		prefix = []byte(PREFUNC + string(Func))
	}
	l.RWLocker.Lock()
	iter := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	return &Iterator{
		l:    l,
		iter: iter,
		Func: Func,
	}
}

// Close the driver
func (l Driver) Close() error {
	err := l.db.Close()
	return err
}

// Iterator define the job iterator
type Iterator struct {
	l    Driver
	iter iterator.Iterator
	Func []byte
}

// Next advances the iterator to the next value, which will then be available through
// then the Value method. It returns false if no further advancement is possible.
func (iter *Iterator) Next() bool {
	return iter.iter.Next()
}

// Value returns the current job.
func (iter *Iterator) Value() (job driver.Job) {
	data := iter.iter.Value()
	if iter.Func == nil {
		job, _ = driver.NewJob(data)
		return
	}
	jobID, _ := strconv.ParseInt(string(data), 10, 64)
	job, _ = iter.l.get(jobID)
	return
}

// Error returns the current error.
func (iter *Iterator) Error() error {
	return iter.iter.Error()
}

// Close the iterator
func (iter *Iterator) Close() {
	iter.iter.Release()
	iter.l.RWLocker.Unlock()
}
