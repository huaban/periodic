package protocol

import (
    "net"
)

type Conn struct {
    net.Conn
}

// Receive waits for a new message on conn, and receives its payload.
func (conn *Conn) Receive() (rdata []byte, rerr error) {

	// Read header
	header := make([]byte, 4)
	nRead := uint32(0)

	for nRead < 4 {
		n, err := conn.receive(header[nRead:])
		if err != nil {
			return nil, err
		}
		nRead = nRead + uint32(n)
	}

	length := parseHeader(header)

	rdata = make([]byte, length)

	nRead = 0
	for nRead < length {
		n, err := conn.receive(rdata[nRead:])
		if err != nil {
			return nil, err
		}
		nRead = nRead + uint32(n)
	}

	return
}

func (conn *Conn) receive(buf []byte) (int, error) {
	bufn, err := conn.Read(buf)
	if err != nil {
		return 0, err
	}

	return bufn, nil
}

// Send a new message.
func (conn *Conn) Send(data []byte) error {
	header, err := makeHeader(data)
	if err != nil {
		return err
	}

	written := 0

	for written < len(header) {
		wrote, err := conn.Write(header[written:])
		if err != nil {
			return err
		}
		written = written + wrote
	}

	written = 0
	for written < len(data) {
		wrote, err := conn.Write(data[written:])
		if err != nil {
			return err
		}
		written = written + wrote
	}

	return nil
}
