from .job import Job
import asyncio

NULL_CHAR = b"\x01"

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


class BaseClient(object):
    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer


    @asyncio.coroutine
    def recive(self):
        head = yield from self._reader.read(4)
        length, hasFd = parseHeader(head)

        payload = yield from self._reader.read(length)
        return payload


    @asyncio.coroutine
    def send(self, payload):
        if isinstance(payload, list):
            payload = [bytes(p, "utf-8") for p in payload]
            payload = NULL_CHAR.join(payload)
        elif isinstance(payload, str):
            payload = bytes(payload, 'utf-8')
        header = makeHeader(payload)
        self._writer.write(header)
        self._writer.write(payload)
        yield from self._writer.drain()


class Client(object):
    def __init__(self):
        self._agent = None
        self.connected = False
        self._conn_lock = asyncio.Lock()


    def _connect(self):
        reader, writer = yield from asyncio.open_unix_connection(self._sock_file)
        self._agent = BaseClient(reader, writer)
        payload = yield from self._agent.recive()
        if payload!= b"connection":
            raise ConnectionError("error on connection")

        self.connected = True
        return True


    def connect(self, sock_file):
        self._sock_file = sock_file
        yield from self._connect()


    def reconnect(self):
        try:
            ret = yield from self.ping()
            if ret:
                return True
        except Exception:
            pass

        print("Try to reconnecting %s"%(self._sock_file))
        connected = yield from self._connect()
        return connected


    def ping(self):
        yield from self._agent.send("ping")
        payload = yield from self._agent.recive()
        if payload == b'pong':
            return True
        return False


    def ask(self):
        yield from self._agent.send("ask")
        payload = yield from self._agent.recive()
        if payload == b'no_job' or payload == b'wait_for_job':
            return None

        return Job(payload, self._agent)
