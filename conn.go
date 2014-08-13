package main

import (
    "net"
)

type Conn struct {
    net.Conn
}


func makeHeader(data []byte) ([]byte, error) {
    header := make([]byte, 4)

    length := uint32(len(data))

    if length > 0x7fffffff {
        return nil, errors.New("Data to large")
    }

    header[0] = byte((length >> 24) & 0xff)
    header[1] = byte((length >> 16) & 0xff)
    header[2] = byte((length >> 8) & 0xff)
    header[3] = byte((length >> 0) & 0xff)

    return header, nil
}


func parseHeader(header []byte) (uint32) {
    length := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])
    length = length & ^uint32(0x80000000)

    return length
}


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
