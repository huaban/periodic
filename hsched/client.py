import socket
from . import data
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


class ConnectionError(Exception):
    pass


class Client(object):
    def __init__(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)


    def connect(self, sock_file):
        self.sock.connect(sock_file)
        payload = self.recive()
        if payload["type"][0] != "connection":
            raise ConnectionError("error on connection")
        return True


    def recive(self):
        head = self.sock.recv(4)
        length, hasFd = parseHeader(head)

        payload = self.sock.recv(length)
        payload = data.Decode(str(payload, 'utf-8'))
        return payload


    def send(self, payload):
        payload = data.Encode(payload)
        payload = bytes(payload, 'utf-8')
        header = makeHeader(payload)
        self.sock.send(header)
        self.sock.send(payload)


def main():
    client = Client()
    client.connect("huabot-sched.sock")

    payload = {"cmd": ["ask"]}
    client.send(payload)

    payload = client.recive()
    print(payload)
    sleep(10)

if __name__ == "__main__":
    main()
