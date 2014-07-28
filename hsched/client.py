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
        self.connected = False
        self._sock_file = None


    def _connect(self):
        self.sock.connect(self._sock_file)
        payload = self.recive()
        if payload["type"][0] != "connection":
            raise ConnectionError("error on connection")

        self.connected = True
        return True


    def connect(self, sock_file):
        self._sock_file = sock_file
        return self._connect()


    def reconnect(self):
        self.sock.close()
        while True:
            try:
                connected = self._connect()
                if connected:
                    break
                sleep(0.5)
            except:
                pass


    def ping(self):
        self.send({"cmd": ["ping"]})
        payload = self.recive()
        if payload.get('message') == 'pong':
            return True
        return False


    def ask(self):
        self.send({"cmd": ["ask"]})
        payload = self.recive()
        if payload.get("msg") == 'no_job' or payload.get("msg") == 'wait_for_job':
                return None

        return payload.get("job")


    def done(self, job):
        self.send({"cmd": ["done"], "job": [job]})


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
