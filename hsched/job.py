import json
from . import utils


class Job(object):

    def __init__(self, payload, client, locker):
        payload = payload.split(utils.NULL_CHAR)
        self.payload = json.loads(str(payload[0], "UTF-8"))
        self.job_handle = str(payload[1], "UTF-8")
        self.client = client
        self._locker = locker


    def get(self, key, default=None):
        return self.payload.get(key, default)


    def done(self):
        with (yield from self._locker):
            yield from self.client.send([utils.JOB_DONE, self.job_handle])


    def sched_later(self, delay):
        with (yield from self._locker):
            yield from self.client.send([utils.SCHED_LATER, self.job_handle, str(delay)])


    def fail(self):
        with (yield from self._locker):
            yield from self.client.send([utils.JOB_FAIL, self.job_handle])


    @property
    def func_name(self):
        return self.payload['func']


    @property
    def name(self):
        return self.payload.get("name")


    @property
    def sched_at(self):
        return self.payload["sched_at"]


    @property
    def timeout(self):
        return self.payload.get("timeout", 0)


    @property
    def run_at(self):
        return self.payload.get("run_at", self.sched_at)


    @property
    def workload(self):
        return self.payload.get("workload")
