package protocol

import (
    "bytes"
    "strconv"
)

// Define command type.
type Command int

const (
    NOOP Command = iota // server
    // for job
    GRAB_JOB    // client
    SCHED_LATER // client
    WORK_DONE   // client
    WORK_FAIL   // client
    JOB_ASSIGN  // server
    NO_JOB      // server
    // for func
    CAN_DO      // client
    CANT_DO     // client
    // for test
    PING        // client
    PONG        // server
    // other
    SLEEP       // client
    UNKNOWN     // server
    // client command
    SUBMIT_JOB  // client
    STATUS      // client
    DROP_FUNC   // client
    SUCCESS     // server
    REMOVE_JOB  // client
    DUMP        // client
    LOAD        // client
)


func (c Command) Bytes() []byte {
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(c))
    return buf.Bytes()
}


func (c Command) String() string {
    switch c {
        case NOOP:
            return "NOOP"
        case GRAB_JOB:
            return "GRAB_JOB"
        case SCHED_LATER:
            return "SCHED_LATER"
        case WORK_DONE:
            return "WORK_DONE"
        case WORK_FAIL:
            return "WORK_FAIL"
        case JOB_ASSIGN:
            return "JOB_ASSIGN"
        case NO_JOB:
            return "NO_JOB"
        case CAN_DO:
            return "CAN_DO"
        case CANT_DO:
            return "CANT_DO"
        case PING:
            return "PING"
        case PONG:
            return "PONG"
        case SLEEP:
            return "SLEEP"
        case UNKNOWN:
            return "UNKNOWN"
        case SUBMIT_JOB:
            return "SUBMIT_JOB"
        case STATUS:
            return "STATUS"
        case DROP_FUNC:
            return "DROP_FUNC"
        case SUCCESS:
            return "SUCCESS"
        case REMOVE_JOB:
            return "REMOVE_JOB"
        case DUMP:
            return "DUMP"
    }
    panic("Unknow Command " + strconv.Itoa(int(c)))
}
