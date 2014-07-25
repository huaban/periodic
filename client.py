import socket
import data
from time import sleep

def parseHeader(head):
    length = head[0] << 24 | head[1] << 16 | head[2] << 8 | head[3]
    hasFd = length & 0x80000000 != 0
    length = length & ~0x80000000

    return length, hasFd

def makeHeader(data):
    header = [0, 0, 0, 0]
    length = len(data)
    header[0] = chr(length >> 24 & 0xff)
    header[1] = chr(length >> 16 & 0xff)
    header[2] = chr(length >> 8 & 0xff)
    header[3] = chr(length >> 0 & 0xff)
    return bytes(''.join(header), 'utf-8')


def main():
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("libchan.sock")

    head = sock.recv(4)
    length, hasFd = parseHeader(head)

    payload = sock.recv(length)
    print(payload)
    payload = data.Decode(str(payload, 'utf-8'))
    if payload["type"][0] != "connection":
        return

    payload = {"cmd": ["ask"]}
    payload = data.Encode(payload)
    payload = bytes(payload, 'utf-8')
    header = makeHeader(payload)
    sock.send(header)
    sock.send(payload)

    head = sock.recv(4)
    length, hasFd = parseHeader(head)

    payload = sock.recv(length)
    payload = data.Decode(str(payload, 'utf-8'))

    print(payload)
    sleep(10)

main()
