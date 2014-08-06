from . import data
import asyncio

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
        payload = data.Decode(str(payload, 'utf-8'))
        return payload


    @asyncio.coroutine
    def send(self, payload):
        payload = data.Encode(payload)
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
        if payload["type"][0] != "connection":
            raise ConnectionError("error on connection")

        self.connected = True
        return True


    def connect(self, sock_file):
        self._sock_file = sock_file
        yield from self.reconnect()


    def reconnect(self):
        count = 0
        with (yield from self._conn_lock):
            while True:
                try:
                    count += 1
                    try:
                        ret = yield from self.ping()
                        if ret:
                            break
                    except Exception:
                        pass

                    print("Try to reconnect %s %s times"%(self._sock_file, count))
                    connected = yield from self._connect()
                    if connected:
                        break
                    yield from asyncio.sleep(5)
                except Exception:
                    pass


    def ping(self):
        yield from self._agent.send({"cmd": ["ping"]})
        payload = yield from self._agent.recive()
        if payload.get('message') == 'pong':
            return True
        return False


    def ask(self):
        yield from self._agent.send({"cmd": ["ask"]})
        payload = yield from self._agent.recive()
        if payload.get("msg") == 'no_job' or payload.get("msg") == 'wait_for_job':
                return None

        return payload.get("job")


    def done(self, job):
        yield from self._agent.send({"cmd": ["done"], "job": [job]})
