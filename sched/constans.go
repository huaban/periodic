package sched

const (
    NOOP = iota // server
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


const (
    TYPE_CLIENT = 1
    TYPE_WORKER = 2
)


var NULL_CHAR = []byte("\x01")
