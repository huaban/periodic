Periodic task system
====================

Install
-------

    $ go get -v github.com/Lupino/periodic/cmd/periodic
    $ $GOPATH/bin/periodic -h # show the help

    NAME:
       periodic - Periodic task system

    USAGE:
       periodic [global options] command [command options] [arguments...]

    VERSION:
       0.1.1

    COMMANDS:
       status   Show status
       submit   Submit job
       drop     Drop func
       run      Run func
       help, h  Shows a list of commands or help for one command

    GLOBAL OPTIONS:
       -H 'unix:///tmp/periodic.sock'   the server address eg: tcp://127.0.0.1:5000 [$PERIODIC_PORT]
       --redis 'tcp://127.0.0.1:6379'   The redis server address, required for driver redis
       --driver 'leveldb'           The driver [leveldb, redis]
       --dbpath 'leveldb'           The db path, required for driver leveldb
       -d                   Enable daemon mode
       --timeout '0'            The socket timeout
       --cpus '4'               The runtime.GOMAXPROCS [$GOMAXPROCS]
       --help, -h               show help
       --version, -v            print the version


Quick start
----------

### Start periodic server

    $ $GOPATH/bin/periodic -d

### A worker to ls a dirctory every five second.

    $ vim ls-every-five-second.sh
    #!/usr/bin/env bash
    ls -lrth $@
    echo "SCHED_LATER 5" # tell periodic do the job 5 second later
    # echo "DONE" # tell periodic the job is done
    # echo "FAIL" # tell periodic the job is fail

    $ chmod +x ls-every-five-second.sh

    $ $GOPATH/bin/periodic run -f ls5 --exec `pwd`/ls-every-five-second.sh


### Submit job

    $ $GOPATH/bin/periodic submit -f ls5 -n /tmp/


Depends
-------

[go](http://golang.org)


Periodic clients
----------------

* [node-periodic](https://github.com/Lupino/node-periodic)
* [python-aio-periodic](https://github.com/Lupino/python-aio-periodic)
