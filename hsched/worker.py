from .job import Job
from .utils import BaseClient
from . import utils
import asyncio

class Worker(object):
    def __init__(self):
        self._agent = None
        self.connected = False
        self._locker = asyncio.Lock()


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
        with (yield from self._locker):
            yield from self._agent.send(utils.TYPE_WORKER)
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

        print("Try to reconnecting %s"%(self._entryPoint))
        connected = yield from self._connect()
        return connected


    def ping(self):
        with (yield from self._locker):
            yield from self._agent.send(utils.PING)
            payload = yield from self._agent.recive()
        if payload == utils.PONG:
            return True
        return False


    def grabJob(self):
        with (yield from self._locker):
            yield from self._agent.send(utils.GRAB_JOB)
            payload = yield from self._agent.recive()
        if payload == utils.NO_JOB or payload == utils.WAIT_JOB:
            return None

        return Job(payload, self._agent, self._locker)


    def add_func(self, func):
        with (yield from self._locker):
            yield from self._agent.send([utils.CAN_DO, func])


    def remove_func(self, func):
        with (yield from self._locker):
            yield from self._agent.send(utils.CANT_DO, func)


    def close(self):
        self._agent.close()
