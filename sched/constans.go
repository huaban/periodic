package sched

import (
    "bytes"
    "strconv"
)


type Command int


func (c Command) Bytes() []byte {
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(c))
    return buf.Bytes()
}


func (c Command) String() string {
    switch c {
        case 0:
            return "NOOP"
        case 1:
            return "GRAB_JOB"
        case 2:
            return "SCHED_LATER"
        case 3:
            return "JOB_DONE"
        case 4:
            return "JOB_FAIL"
        case 5:
            return "WAIT_JOB"
        case 6:
            return "NO_JOB"
        case 7:
            return "CAN_DO"
        case 8:
            return "CANT_DO"
        case 9:
            return "PING"
        case 10:
            return "PONG"
        case 11:
            return "SLEEP"
        case 12:
            return "UNKNOWN"
        case 13:
            return "SUBMIT_JOB"
        case 14:
            return "STATUS"
        case 15:
            return "DROP_FUNC"
    }
    panic("Unknow Command " + strconv.Itoa(int(c)))
}


const (
    NOOP Command = iota // server
    // for job
    GRAB_JOB    // client
    SCHED_LATER // client
    JOB_DONE    // client
    JOB_FAIL    // client
    WAIT_JOB    // server
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
)

type ClientType int


func (c ClientType) Bytes() []byte {
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(c))
    return buf.Bytes()
}


func (c ClientType) String() string {
    switch c {
        case 1:
            return "TYPE_CLIENT"
        case 2:
            return "TYPE_WORKER"
    }
    panic("Unknow ClientType " + strconv.Itoa(int(c)))
}

const (
    TYPE_CLIENT ClientType = iota + 1
    TYPE_WORKER
)


var NULL_CHAR = []byte("\x01")
