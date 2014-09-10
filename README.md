Periodic task system
====================

Build
-----

    cd $GOPATH/src
    git clone url/to/periodic.git
    cd periodic
    go get -v -d
    go install
    $GOAPTH/bin/periodic -h


Quick start
----------

### Start periodic server

    $GOAPTH/bin/periodic -d

### A worker to ls a dirctory every five second.

    $ vim ls-every-five-second.sh
    #!/usr/bin/env bash
    ls -lrth $@
    echo "SCHED_LATER 5"

    chmod +x ls-every-five-second.sh

    $GOAPTH/bin/periodic run -f ls5 --exec `pwd`/ls-every-five-second.sh


### Submit job

    $GOAPTH/bin/periodic submit -f ls5 -n /tmp/


Depends
-------

[go](http://golang.org)
