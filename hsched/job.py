import json

NULL_CHAR = b"\x01"

class Job(object):

    def __init__(self, workload, client):
        workload = workload.split(NULL_CHAR)
        self.workload = json.loads(str(workload[0], "UTF-8"))
        self.job_handle = str(workload[1], "UTF-8")
        self.client = client


    def get(self, key, default=None):
        return self.workload.get(key, default)


    def done(self):
        yield from self.client.send(["done", self.job_handle])


    def sched_later(self, delay):
        yield from self.client.send(["sched_later", self.job_handle,str(delay)])


    def fail(self):
        yield from self.client.send(["fail", self.job_handle])


    @property
    def func_name(self):
        return self.workload['name']


    @property
    def name(self):
        return self.workload.get("name")


    @property
    def sched_at(self):
        return self.workload["sched_at"]


    @property
    def timeout(self):
        return self.workload.get("timeout", 0)


    @property
    def run_at(self):
        return self.workload.get("run_at", self.sched_at)
