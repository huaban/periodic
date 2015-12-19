package protocol

import (
	"bytes"
	"strconv"
)

// Command defined command type.
type Command int

const (
	// NOOP do nothing
	NOOP Command = iota // server
	// GRABJOB client ask a job
	GRABJOB // client
	// SCHEDLATER tell server sched later the job
	SCHEDLATER // client
	// WORKDONE tell server the work is done
	WORKDONE // client
	// WORKFAIL tell server work is fail
	WORKFAIL // client
	// JOBASSIGN assign a job for client
	JOBASSIGN // server
	// NOJOB tell client job is empty
	NOJOB // server
	// CANDO tell server the client can do some func
	CANDO // client
	// CANTDO tell server the client can not do some func
	CANTDO // client
	// PING test ping
	PING // client
	// PONG reply pong
	PONG // server
	// SLEEP tell the client to sleep
	SLEEP // client
	// UNKNOWN command unknow
	UNKNOWN // server
	// SUBMITJOB submit a job for server
	SUBMITJOB // client
	// STATUS ask the server status
	STATUS // client
	// DROPFUNC drop an empty worker func
	DROPFUNC // client
	// SUCCESS reply client success
	SUCCESS // server
	// REMOVEJOB remove a job
	REMOVEJOB // client
	// DUMP dump the data
	DUMP // client
	// LOAD load data to database
	LOAD // client
)

// Bytes convert command to byte
func (c Command) Bytes() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(c))
	return buf.Bytes()
}

func (c Command) String() string {
	switch c {
	case NOOP:
		return "NOOP"
	case GRABJOB:
		return "GRABJOB"
	case SCHEDLATER:
		return "SCHEDLATER"
	case WORKDONE:
		return "WORKDONE"
	case WORKFAIL:
		return "WORKFAIL"
	case JOBASSIGN:
		return "JOBASSIGN"
	case NOJOB:
		return "NOJOB"
	case CANDO:
		return "CANDO"
	case CANTDO:
		return "CANTDO"
	case PING:
		return "PING"
	case PONG:
		return "PONG"
	case SLEEP:
		return "SLEEP"
	case UNKNOWN:
		return "UNKNOWN"
	case SUBMITJOB:
		return "SUBMITJOB"
	case STATUS:
		return "STATUS"
	case DROPFUNC:
		return "DROPFUNC"
	case SUCCESS:
		return "SUCCESS"
	case REMOVEJOB:
		return "REMOVEJOB"
	case DUMP:
		return "DUMP"
	}
	panic("Unknow Command " + strconv.Itoa(int(c)))
}
