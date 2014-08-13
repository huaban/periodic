package main

import (
    "net"
)

type Conn struct {
    net.Conn
}


// Receive waits for a new message on conn, and receives its payload
// and attachment, or an error if any.
//
// If more than 1 file descriptor is sent in the message, they are all
// closed except for the first, which is the attachment.
// It is legal for a message to have no attachment or an empty payload.
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

func (conn *Conn) Send(data []byte) error {
	header, err := makeHeader(data, fds)
	if err != nil {
		return err
	}

	// There is a bug in conn.WriteMsgUnix where it doesn't correctly return
	// the number of bytes writte (http://code.google.com/p/go/issues/detail?id=7645)
	// So, we can't rely on the return value from it. However, we must use it to
	// send the fds. In order to handle this we only write one byte using WriteMsgUnix
	// (when we have to), as that can only ever block or fully suceed. We then write
	// the rest with conn.Write()
	// The reader side should not rely on this though, as hopefully this gets fixed
	// in go later.
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
