package main

import (
    "github.com/docker/libchan/data"
    "github.com/docker/libchan/unix"
    "log"
    "net"
)


func main() {
    listen, err := net.Listen("unix", "libchan.sock")
    if err != nil {
        log.Fatal(err)
    }

    defer listen.Close()

    for {
        fd, err := listen.Accept()
        if err != nil {
            log.Fatal(err)
        }

        ufd := fd.(*net.UnixConn)
        file, err := ufd.File()
        fileConn, err := unix.FileConn(file)
        fd.Close()
        if err := fileConn.Send(data.Empty().Set("foo", "bar").Bytes(), nil); err != nil {
            log.Fatal(err)
        }
        fileConn.Close()
    }
}
