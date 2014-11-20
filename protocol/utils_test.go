package protocol

import (
    "fmt"
    "testing"
)


func TestHeader(t *testing.T) {
    var data = []byte("data")
    var length = uint32(len(data))
    var header, err = makeHeader(data)
    if err != nil {
        t.Fatal(err)
    }
    fmt.Printf("%s\n", header)
    var lengthGot = parseHeader(header)

    if lengthGot != length {
        t.Fatalf("Header: except: %d, got: %d", length, lengthGot)
    }
}


func testPanic() {
    x := recover()
    if x == nil {
        panic("there no panic")
    }
    fmt.Printf("panic %s\n", x)
}


func TestParseCommand(t *testing.T) {
    var pack = []byte("100\x00\x01\x01\x00\x01hhcc")
    var msgId, cmd, data = ParseCommand(pack)
    fmt.Printf("%d, %d, %s\n", msgId, cmd, data)
}


func TestParseCommandPanic1(t *testing.T) {
    defer testPanic()
    var pack = []byte("100\x00\x01")
    ParseCommand(pack)
}


func TestParseCommandPanic2(t *testing.T) {
    defer testPanic()
    var pack = []byte("100")
    ParseCommand(pack)
}
