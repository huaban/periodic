package subcmd

import (
	"bytes"
	"github.com/Lupino/periodic/protocol"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

func Load(entryPoint, input string) {
	parts := strings.SplitN(entryPoint, "://", 2)
	c, err := net.Dial(parts[0], parts[1])
	if err != nil {
		log.Fatal(err)
	}
	conn := protocol.NewClientConn(c)
	defer conn.Close()
	err = conn.Send(protocol.TYPE_CLIENT.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	var fp *os.File
	if fp, err = os.Open(input); err != nil {
		log.Fatal(err)
	}

	defer fp.Close()

	var msgID = []byte("100")
	for {
		payload, err := readPatch(fp)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		buf := bytes.NewBuffer(nil)
		buf.Write(msgID)
		buf.Write(protocol.NULL_CHAR)
		buf.Write(protocol.LOAD.Bytes())
		buf.Write(protocol.NULL_CHAR)
		buf.Write(payload)

		err = conn.Send(buf.Bytes())
		if err != nil {
			log.Fatal(err)
		}
	}
}

func readPatch(fp *os.File) (payload []byte, err error) {
	var header = make([]byte, 4)
	nRead := uint32(0)
	for nRead < 4 {
		n, err := fp.Read(header[nRead:])
		if err != nil {
			return nil, err
		}
		nRead = nRead + uint32(n)
	}

	length := protocol.ParseHeader(header)
	payload = make([]byte, length)
	nRead = uint32(0)
	for nRead < length {
		n, err := fp.Read(payload[nRead:])
		if err != nil {
			return nil, err
		}
		nRead = nRead + uint32(n)
	}
	return
}
