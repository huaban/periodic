Periodic task system
====================
[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/Lupino/periodic?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

What is Periodic?
-----------------

Periodic is a [Gearman](http://gearman.org) like task system,
provides a generic application framework to do the periodic work.

How Does Periodic work?
-----------------------
A Periodic powered application consists of three parts: a client, a worker, and a periodic server.
The client is responsible for creating a job to be run and sending it to a periodic server.
The periodic server will find a suitable worker that can run the job when then job time is up.
The worker performs the work requested by the client and sends a stat to the periodic server.
Periodic provides client and worker APIs that your applications call to talk with the periodic server (also known as `periodic -d`) so you don’t need to deal with networking or mapping of jobs.

![Periodic](https://raw.githubusercontent.com/Lupino/periodic/master/resources/periodic.jpg)

Internally, the periodic client and worker APIs communicate with the periodic server using TCP sockets.
To explain how Periodic works in more detail, lets look at a simple application that will print a string on a random delay.
The example is given in [Python3](http://python.org), although other APIs will look quite similar.


We start off by writing a client application that is responsible for sending off the job.
It does this by using the Periodic client API to send some data associated with a function name, in this case the function `random_print`. The code for this is:

    from aio_periodic import Client
    from time import time
    client = Client()
    client.add_server("unix:///tmp/periodic.sock")
    yield from client.connect()
    job = {
        "name": "Hello world!",
        "func": "random_print",
        "sched_at": int(time()),
    }
    yield from client.submitJob(job)

This code initializes a client class, configures it to use a periodic server with `add_server`,
and then tells the client API to run the `random_print` function with the workload “Hello world!”.
The function name and arguments are completely arbitrary as far as Periodic is concerned,
so you could send any data structure that is appropriate for your application (text or binary).
At this point the Periodic client API will package up the job into a Periodic protocol packet
and send it to the periodic server to find an appropriate worker that can run the `random_print` function.
Let’s now look at the worker code:

    from aio_periodic import Worker
    import random

    worker = Worker()
    worker.add_server("unix:///tmp/periodic.sock")
    yield from worker.connect()
    yield from worker.add_func("random_print")
    count = 0
    while True:
        job = yield from worker.grabJob()
        print(count, job.name)
        count += 1
        if count > 10:
            yield from job.done()
            break

        yield from job.sched_later(random.randint(5, 10))

    print('Finish!')

This code defines a function `random_print` that takes a string and print the string on a random delay for 10 times.
It is used by a worker object to register a function named `random_print`
after it is setup to connect to the same periodic server as the client.
When the periodic server receives the job to be run,
it looks at the list of workers who have registered the function name `random_print`
and forwards the job on to one of the free workers.
The Periodic worker API then takes this request,
runs the function `random_print`, and sends the job stat of that function back to the periodic server.

![Random print](https://raw.githubusercontent.com/Lupino/periodic/master/resources/random_print.png)

As you can see, the client and worker APIs (along with the periodic server) deal with the job management and network communication so you can focus on the application parts.


Quick start
----------

### Install

    go get -v github.com/Lupino/periodic/cmd/periodic
    periodic -h

### Start periodic server

    $ periodic -d

### A worker to ls a dirctory every five second.

    $ vim ls-every-five-second.sh
    #!/usr/bin/env bash
    ls -lrth $@
    echo "SCHED_LATER 5" # tell periodic do the job 5 second later
    # echo "DONE" # tell periodic the job is done
    # echo "FAIL" # tell periodic the job is fail

    $ chmod +x ls-every-five-second.sh

    $ periodic run -f ls5 --exec `pwd`/ls-every-five-second.sh


### Submit a job

    $ periodic submit -f ls5 -n /tmp/


Depends
-------

[go](http://golang.org)


Periodic clients
----------------

* [node-periodic](https://github.com/Lupino/node-periodic)
* [python-aio-periodic](https://github.com/Lupino/python-aio-periodic)
