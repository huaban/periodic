package cmd


import (
    "net"
    "strings"
    "encoding/json"
    "periodic/sched"
    "fmt"
    "log"
    "bytes"
    "errors"
    "os/exec"
    "strconv"
)


func Run(entryPoint, Func, cmd string) {
    parts := strings.SplitN(entryPoint, "://", 2)
    c, err := net.Dial(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    conn := sched.Conn{Conn: c}
    defer conn.Close()
    err = conn.Send(sched.PackCmd(sched.TYPE_WORKER))
    if err != nil {
        log.Fatal(err)
    }
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(sched.CAN_DO))
    buf.Write(sched.NULL_CHAR)
    buf.WriteString(Func)
    err = conn.Send(buf.Bytes())
    if err != nil {
        log.Fatal(err)
    }

    var payload []byte
    var job sched.Job
    var jobHandle []byte
    for {
        err = conn.Send(sched.PackCmd(sched.GRAB_JOB))
        if err != nil {
            log.Fatal(err)
        }
        payload, err = conn.Receive()
        if err != nil {
            log.Fatal(err)
        }
        job, jobHandle, err = extraJob(payload)
        realCmd := strings.Split(cmd, " ")
        realCmd = append(realCmd, job.Name)
        c := exec.Command(realCmd[0], realCmd[1:]...)
        c.Stdin = strings.NewReader(job.Args)
        var out bytes.Buffer
        c.Stdout = &out
        err = c.Run()
        var schedLater int
        for {
            line, err := out.ReadString([]byte("\n")[0])
            if err != nil {
                break
            }
            if strings.HasPrefix(line, "SCHED_LATER") {
                parts = strings.SplitN(line[:len(line) - 1], " ", 2)
                later := strings.Trim(parts[1], " ")
                schedLater, _ = strconv.Atoi(later)
            } else {
                fmt.Print(line)
            }
        }
        buf := bytes.NewBuffer(nil)
        if err != nil {
            buf.WriteByte(byte(sched.JOB_FAIL))
        } else if schedLater > 0 {
            buf.WriteByte(byte(sched.SCHED_LATER))
        } else {
            buf.WriteByte(byte(sched.JOB_DONE))
        }
        buf.Write(sched.NULL_CHAR)
        buf.Write(jobHandle)
        if schedLater > 0 {
            buf.Write(sched.NULL_CHAR)
            buf.WriteString(strconv.Itoa(schedLater))
        }
        err = conn.Send(buf.Bytes())
    }
}


func extraJob(payload []byte) (job sched.Job, jobHandle []byte, err error) {
    parts := bytes.SplitN(payload, sched.NULL_CHAR, 2)
    if len(parts) != 2 {
        err = errors.New("Invalid payload " + string(payload))
        return
    }
    err = json.Unmarshal(parts[0], &job)
    jobHandle = parts[1]
    return
}
