package subcmd

import (
	"bytes"
	"fmt"
	"github.com/Lupino/periodic/protocol"
	"log"
	"net"
	"os"
	"strings"
)

// Dump cli dump
func Dump(entryPoint, output string) {
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
	buf.Write(protocol.DUMP.Bytes())
	err = conn.Send(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	var fp *os.File
	if fp, err = os.Create(output); err != nil {
		log.Fatal(err)
	}

	defer fp.Close()

	for {
		payload, err := conn.Receive()
		if err != nil {
			log.Fatal(err)
		}
		_parts := bytes.SplitN(payload, protocol.NullChar, 2)
		if len(_parts) != 2 {
			err := fmt.Sprint("ParseCommand InvalID %v\n", payload)
			panic(err)
		}
		payload = _parts[1]
		if bytes.Equal(payload, []byte("EOF")) {
			break
		}
		header, _ := protocol.MakeHeader(payload)
		fp.Write(header)
		fp.Write(payload)
	}
}
