from .job import Job
from .utils import to_bytes
from . import utils
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
        self._rlock = asyncio.Lock()
        self._wlock = asyncio.Lock()


    @asyncio.coroutine
    def recive(self):
        with (yield from self._rlock):
            head = yield from self._reader.read(4)
            length, hasFd = parseHeader(head)

            payload = yield from self._reader.read(length)
            return payload


    @asyncio.coroutine
    def send(self, payload):
        if isinstance(payload, list):
            payload = [to_bytes(p) for p in payload]
            payload = NULL_CHAR.join(payload)
        elif isinstance(payload, str):
            payload = bytes(payload, 'utf-8')
        header = makeHeader(payload)
        with (yield from self._wlock):
            self._writer.write(header)
            self._writer.write(payload)
            yield from self._writer.drain()


    def close(self):
        self._writer.close()


class Client(object):
    def __init__(self):
        self._agent = None
        self.connected = False
        self._conn_lock = asyncio.Lock()


    def _connect(self):
        if self._entryPoint.startswith("unix://"):
            reader, writer = yield from asyncio.open_unix_connection(self._entryPoint.split("://")[1])
        else:
            host_port = self._entryPoint.split("://")[1].split(":")
            reader, writer = yield from asyncio.open_connection(host_port[0], host_port[1])

        if self._agent:
            try:
                self._agent.close()
            except Exception:
                pass
        self._agent = BaseClient(reader, writer)
        self.connected = True
        return True


    def add_server(self, entryPoint):
        self._entryPoint = entryPoint


    def connect(self):
        try:
            ret = yield from self.ping()
            if ret:
                self.connected = True
                return True
        except Exception:
            pass

        print("Try to reconnecting %s"%(self._sock_file))
        connected = yield from self._connect()
        return connected


    def ping(self):
        yield from self._agent.send(utils.PING)
        payload = yield from self._agent.recive()
        if payload == utils.PONG:
            return True
        return False


    def grabJob(self):
        yield from self._agent.send(utils.GRAB_JOB)
        payload = yield from self._agent.recive()
        if payload == utils.NO_JOB or payload == utils.WAIT_JOB:
            return None

        return Job(payload, self._agent)


    def add_func(self, func):
        yield from self._agent.send([utils.CAN_DO, func])


    def remove_func(self, func):
        yield from self._agent.send(utils.CANT_DO, func)
