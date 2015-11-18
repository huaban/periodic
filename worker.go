package periodic

import (
	"bytes"
	"github.com/Lupino/periodic/driver"
	"github.com/Lupino/periodic/protocol"
	"io"
	"log"
	"strconv"
	"sync"
)

type worker struct {
	jobQueue map[int64]driver.Job
	conn     protocol.Conn
	sched    *Sched
	alive    bool
	funcs    []string
	locker   *sync.Mutex
}

func newWorker(sched *Sched, conn protocol.Conn) (w *worker) {
	w = new(worker)
	w.conn = conn
	w.jobQueue = make(map[int64]driver.Job)
	w.sched = sched
	w.funcs = make([]string, 0)
	w.alive = true
	w.locker = new(sync.Mutex)
	return
}

func (w *worker) IsAlive() bool {
	return w.alive
}

func (w *worker) handleJobAssign(msgID []byte, job driver.Job) (err error) {
	defer w.locker.Unlock()
	w.locker.Lock()
	w.jobQueue[job.Id] = job
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	buf.Write(protocol.NULL_CHAR)
	buf.Write(protocol.JOB_ASSIGN.Bytes())
	buf.Write(protocol.NULL_CHAR)
	buf.WriteString(strconv.FormatInt(job.Id, 10))
	buf.Write(protocol.NULL_CHAR)
	buf.Write(job.Bytes())
	err = w.conn.Send(buf.Bytes())
	return
}

func (w *worker) handleCanDo(Func string) error {
	for _, f := range w.funcs {
		if f == Func {
			return nil
		}
	}
	w.funcs = append(w.funcs, Func)
	w.sched.incrStatFunc(Func)
	return nil
}

func (w *worker) handleCanNoDo(Func string) error {
	newFuncs := make([]string, 0)
	for _, f := range w.funcs {
		if f == Func {
			continue
		}
		newFuncs = append(newFuncs, f)
	}
	w.funcs = newFuncs
	return nil
}

func (w *worker) handleDone(jobID int64) (err error) {
	w.sched.done(jobID)
	defer w.locker.Unlock()
	w.locker.Lock()
	if _, ok := w.jobQueue[jobID]; ok {
		delete(w.jobQueue, jobID)
	}
	return nil
}

func (w *worker) handleFail(jobID int64) (err error) {
	w.sched.fail(jobID)
	defer w.locker.Unlock()
	w.locker.Lock()
	if _, ok := w.jobQueue[jobID]; ok {
		delete(w.jobQueue, jobID)
	}
	return nil
}

func (w *worker) handleCommand(msgID []byte, cmd protocol.Command) (err error) {
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	buf.Write(protocol.NULL_CHAR)
	buf.Write(cmd.Bytes())
	err = w.conn.Send(buf.Bytes())
	return
}

func (w *worker) handleSchedLater(jobID, delay int64) (err error) {
	w.sched.schedLater(jobID, delay)
	defer w.locker.Unlock()
	w.locker.Lock()
	if _, ok := w.jobQueue[jobID]; ok {
		delete(w.jobQueue, jobID)
	}
	return nil
}

func (w *worker) handleGrabJob(msgID []byte) (err error) {
	item := grabItem{
		w:     w,
		msgID: msgID,
	}
	w.sched.grabQueue.push(item)
	w.sched.notifyJobTimer()
	return nil
}

func (w *worker) handle() {
	var payload []byte
	var err error
	var conn = w.conn
	var msgID []byte
	var cmd protocol.Command
	defer func() {
		if x := recover(); x != nil {
			log.Printf("[worker] painc: %v\n", x)
		}
	}()
	defer w.Close()
	for {
		payload, err = conn.Receive()
		if err != nil {
			if err != io.EOF {
				log.Printf("workerError: %s\n", err.Error())
			}
			break
		}

		msgID, cmd, payload = protocol.ParseCommand(payload)

		switch cmd {
		case protocol.GRAB_JOB:
			err = w.handleGrabJob(msgID)
			break
		case protocol.WORK_DONE:
			jobID, _ := strconv.ParseInt(string(payload), 10, 0)
			err = w.handleDone(jobID)
			break
		case protocol.WORK_FAIL:
			jobID, _ := strconv.ParseInt(string(payload), 10, 0)
			err = w.handleFail(jobID)
			break
		case protocol.SCHED_LATER:
			parts := bytes.SplitN(payload, protocol.NULL_CHAR, 2)
			if len(parts) != 2 {
				log.Printf("Error: invalid format.")
				break
			}
			jobID, _ := strconv.ParseInt(string(parts[0]), 10, 0)
			delay, _ := strconv.ParseInt(string(parts[1]), 10, 0)
			err = w.handleSchedLater(jobID, delay)
			break
		case protocol.SLEEP:
			err = w.handleCommand(msgID, protocol.NOOP)
			break
		case protocol.PING:
			err = w.handleCommand(msgID, protocol.PONG)
			break
		case protocol.CAN_DO:
			err = w.handleCanDo(string(payload))
			break
		case protocol.CANT_DO:
			err = w.handleCanNoDo(string(payload))
			break
		default:
			err = w.handleCommand(msgID, protocol.UNKNOWN)
			break
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("workerError: %s\n", err.Error())
			}
			break
		}

		if !w.alive {
			break
		}
	}
}

func (w *worker) Close() {
	defer w.sched.notifyJobTimer()
	defer w.conn.Close()
	w.sched.grabQueue.removeWorker(w)
	w.alive = false
	for k, _ := range w.jobQueue {
		w.sched.fail(k)
	}
	w.jobQueue = nil
	for _, Func := range w.funcs {
		w.sched.decrStatFunc(Func)
	}
	w = nil
}
