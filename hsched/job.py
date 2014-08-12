import json

class Job(object):

    def __init__(self, workload, client):
        self.workload = json.loads(workload[0])
        self.job_handle = str(self.workload["job_id"])
        self.client = client


    def __getattr__(self, key):
        return self.workload.get(key)


    def get(self, key, default=None):
        return self.workload.get(key, default)


    def done(self):
        yield from self.client.send({"cmd": ["done"], "job_handle": [self.job_handle]})


    def sched_later(self, delay):
        yield from self.client.send({
            "cmd": ["sched_later"],
            "job_handle": [self.job_handle],
            "delay": [str(delay)]
        })


    def fail(self):
        yield from self.client.send({
            "cmd": ["fail"],
            "job_handle": [self.job_handle]
        })
