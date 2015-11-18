package leveldb

import (
	"errors"
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

const PRE_JOB = "job:"
const PRE_JOB_FUNC = "func:"
const PRE_SEQUENCE = "sequence:"

type LevelDBDriver struct {
	db       *leveldb.DB
	RWLocker *sync.Mutex
	cache    *lru.Cache
}

func NewLevelDBDriver(dbpath string) LevelDBDriver {
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
	return LevelDBDriver{
		db:       db,
		cache:    cache,
		RWLocker: RWLocker,
	}
}

func (l LevelDBDriver) Save(job *driver.Job, force ...bool) (err error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	batch := new(leveldb.Batch)
	var isNew = true
	if job.Id > 0 {
		isNew = false
	} else {
		last_id, e := l.db.Get([]byte(PRE_SEQUENCE+"JOB"), nil)
		if e != nil || last_id == nil {
			job.Id = 1
		} else {
			id, _ := strconv.ParseInt(string(last_id), 10, 64)
			job.Id = id + 1
		}
	}
	var strId = strconv.FormatInt(job.Id, 10)
	if isNew {
		batch.Put([]byte(PRE_SEQUENCE+"JOB"), []byte(strId))
		batch.Put([]byte(PRE_JOB_FUNC+job.Func+":"+job.Name), []byte(strId))
	} else if len(force) == 0 || !force[0] {
		old, e := l.get(job.Id)
		if e != nil || old.Id == 0 {
			err = errors.New(fmt.Sprintf("Update Job %d fail, the old job is not exists.", job.Id))
			return
		}
		l.cache.Remove(PRE_JOB + strId)
		if old.Name != job.Name {
			batch.Delete([]byte(PRE_JOB_FUNC + job.Func + ":" + old.Name))
			batch.Put([]byte(PRE_JOB_FUNC+job.Func+":"+job.Name), []byte(strId))
		}
	}
	batch.Put([]byte(PRE_JOB+strId), job.Bytes())
	err = l.db.Write(batch, nil)
	return
}

func (l LevelDBDriver) Delete(jobId int64) (err error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	var job driver.Job
	batch := new(leveldb.Batch)
	job, err = l.get(jobId)
	if err != nil {
		return
	}
	var strId = strconv.FormatInt(job.Id, 10)
	batch.Delete([]byte(PRE_JOB_FUNC + job.Func + ":" + job.Name))
	batch.Delete([]byte(PRE_JOB + strId))
	err = l.db.Write(batch, nil)
	l.cache.Remove(PRE_JOB + strId)
	return
}

func (l LevelDBDriver) Get(jobId int64) (driver.Job, error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	return l.get(jobId)
}

func (l LevelDBDriver) get(jobId int64) (job driver.Job, err error) {
	var data []byte
	var key = PRE_JOB + strconv.FormatInt(jobId, 10)
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

func (l LevelDBDriver) GetOne(Func, name string) (job driver.Job, err error) {
	defer l.RWLocker.Unlock()
	l.RWLocker.Lock()
	var data []byte
	var key = PRE_JOB_FUNC + Func + ":" + name
	data, err = l.db.Get([]byte(key), nil)
	if err != nil {
		return
	}
	key = PRE_JOB + string(data)
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

func (l LevelDBDriver) NewIterator(Func []byte) driver.JobIterator {
	var prefix []byte
	if Func == nil {
		prefix = []byte(PRE_JOB)
	} else {
		prefix = []byte(PRE_JOB_FUNC + string(Func))
	}
	l.RWLocker.Lock()
	iter := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	return &LevelDBIterator{
		l:    l,
		iter: iter,
		Func: Func,
	}
}

func (l LevelDBDriver) Close() error {
	err := l.db.Close()
	return err
}

type LevelDBIterator struct {
	l    LevelDBDriver
	iter iterator.Iterator
	Func []byte
}

func (iter *LevelDBIterator) Next() bool {
	return iter.iter.Next()
}

func (iter *LevelDBIterator) Value() (job driver.Job) {
	data := iter.iter.Value()
	if iter.Func == nil {
		job, _ = driver.NewJob(data)
		return
	}
	jobId, _ := strconv.ParseInt(string(data), 10, 64)
	job, _ = iter.l.get(jobId)
	return
}

func (iter *LevelDBIterator) Error() error {
	return iter.iter.Error()
}

func (iter *LevelDBIterator) Close() {
	iter.iter.Release()
	iter.l.RWLocker.Unlock()
}
