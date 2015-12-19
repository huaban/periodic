package subcmd

import (
	"bytes"
	"fmt"
	"github.com/Lupino/periodic/protocol"
	"log"
	"net"
	"strings"
)

// DropFunc cli drop
func DropFunc(entryPoint, Func string) {
	parts := strings.SplitN(entryPoint, "://", 2)
	c, err := net.Dial(parts[0], parts[1])
	if err != nil {
		log.Fatal(err)
	}
	conn := protocol.NewClientConn(c)
	defer conn.Close()
	err = conn.Send(protocol.TYPECLIENT.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	var msgID = []byte("100")
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	buf.Write(protocol.NullChar)
	buf.WriteByte(byte(protocol.DROPFUNC))
	buf.Write(protocol.NullChar)
	buf.WriteString(Func)
	err = conn.Send(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	payload, err := conn.Receive()
	if err != nil {
		log.Fatal(err)
	}
	_, cmd, _ := protocol.ParseCommand(payload)
	fmt.Printf("%s\n", cmd)
}
