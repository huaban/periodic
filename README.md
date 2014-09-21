Periodic task system
====================

Install
-------

    go get -v github.com/Lupino/periodic/cmd/periodic
    $GOPATH/bin/periodic -h # show the help


Quick start
----------

### Start periodic server

    $GOPATH/bin/periodic -d

### A worker to ls a dirctory every five second.

    $ vim ls-every-five-second.sh
    #!/usr/bin/env bash
    ls -lrth $@
    echo "SCHED_LATER 5"

    $ chmod +x ls-every-five-second.sh

    $GOPATH/bin/periodic run -f ls5 --exec `pwd`/ls-every-five-second.sh


### Submit job

    $GOPATH/bin/periodic submit -f ls5 -n /tmp/


Depends
-------

[go](http://golang.org)


Periodic clients
----------------

* [node-periodic](https://github.com/Lupino/node-periodic)
* [python-aio-periodic](https://github.com/Lupino/python-aio-periodic)
