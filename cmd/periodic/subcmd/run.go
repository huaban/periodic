package subcmd

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Lupino/periodic/driver"
	"github.com/Lupino/periodic/protocol"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func Run(entryPoint, Func, cmd string) {
	parts := strings.SplitN(entryPoint, "://", 2)
	for {
		c, err := net.Dial(parts[0], parts[1])
		if err != nil {
			if err != io.EOF {
				log.Printf("Error: %s\n", err.Error())
			}
			log.Printf("Wait 5 second to reconnecting")
			time.Sleep(5 * time.Second)
			continue
		}
		conn := protocol.NewClientConn(c)
		err = handleWorker(conn, Func, cmd)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error: %s\n", err.Error())
			}
		}
		conn.Close()
	}
}

func handleWorker(conn protocol.Conn, Func, cmd string) (err error) {
	err = conn.Send(protocol.TYPE_WORKER.Bytes())
	if err != nil {
		return
	}
	var msgId = []byte("100")
	buf := bytes.NewBuffer(nil)
	buf.Write(msgId)
	buf.Write(protocol.NULL_CHAR)
	buf.WriteByte(byte(protocol.CAN_DO))
	buf.Write(protocol.NULL_CHAR)
	buf.WriteString(Func)
	err = conn.Send(buf.Bytes())
	if err != nil {
		return
	}

	var payload []byte
	var job driver.Job
	var jobHandle []byte
	for {
		buf = bytes.NewBuffer(nil)
		buf.Write(msgId)
		buf.Write(protocol.NULL_CHAR)
		buf.Write(protocol.GRAB_JOB.Bytes())
		err = conn.Send(buf.Bytes())
		if err != nil {
			return
		}
		payload, err = conn.Receive()
		if err != nil {
			return
		}
		job, jobHandle, err = extraJob(payload)
		realCmd := strings.Split(cmd, " ")
		realCmd = append(realCmd, job.Name)
		c := exec.Command(realCmd[0], realCmd[1:]...)
		c.Stdin = strings.NewReader(job.Args)
		var out bytes.Buffer
		c.Stdout = &out
		c.Stderr = os.Stderr
		err = c.Run()
		var schedLater int
		var fail = false
		for {
			line, err := out.ReadString([]byte("\n")[0])
			if err != nil {
				break
			}
			if strings.HasPrefix(line, "SCHED_LATER") {
				parts := strings.SplitN(line[:len(line)-1], " ", 2)
				later := strings.Trim(parts[1], " ")
				schedLater, _ = strconv.Atoi(later)
			} else if strings.HasPrefix(line, "FAIL") {
				fail = true
			} else {
				fmt.Print(line)
			}
		}
		buf = bytes.NewBuffer(nil)
		buf.Write(msgId)
		buf.Write(protocol.NULL_CHAR)
		if err != nil || fail {
			buf.WriteByte(byte(protocol.WORK_FAIL))
		} else if schedLater > 0 {
			buf.WriteByte(byte(protocol.SCHED_LATER))
		} else {
			buf.WriteByte(byte(protocol.WORK_DONE))
		}
		buf.Write(protocol.NULL_CHAR)
		buf.Write(jobHandle)
		if schedLater > 0 {
			buf.Write(protocol.NULL_CHAR)
			buf.WriteString(strconv.Itoa(schedLater))
		}
		err = conn.Send(buf.Bytes())
		if err != nil {
			return
		}
	}
}

func extraJob(payload []byte) (job driver.Job, jobHandle []byte, err error) {
	parts := bytes.SplitN(payload, protocol.NULL_CHAR, 4)
	if len(parts) != 4 {
		err = errors.New("Invalid payload " + string(payload))
		return
	}
	job, err = driver.NewJob(parts[3])
	jobHandle = parts[2]
	return
}
