import json

NULL_CHAR = b"\x01"

class Job(object):

    def __init__(self, payload, client):
        payload = payload.split(NULL_CHAR)
        self.payload = json.loads(str(payload[0], "UTF-8"))
        self.job_handle = str(payload[1], "UTF-8")
        self.client = client


    def get(self, key, default=None):
        return self.payload.get(key, default)


    def done(self):
        yield from self.client.send(["done", self.job_handle])


    def sched_later(self, delay):
        yield from self.client.send(["sched_later", self.job_handle,str(delay)])


    def fail(self):
        yield from self.client.send(["fail", self.job_handle])


    @property
    def func_name(self):
        return self.payload['name']


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
