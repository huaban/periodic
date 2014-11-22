/*
# Periodic Protocol

The Periodic protocol operates over TCP on port 5000 by default,
or unix socket on unix:///tmp/periodic.sock.
Communication happens between either a client and periodic server,
or between a worker and periodic server.
In either case, the protocol consists of packets
containing requests and responses. All packets sent to a periodic server
are considered requests, and all packets sent from a periodic server are
considered responses. A simple configuration may look like:

    ----------     ----------     ----------     ----------
    | Client |     | Client |     | Client |     | Client |
    ----------     ----------     ----------     ----------
         \             /              \             /
          \           /                \           /
       -------------------          -------------------
       | Periodic Server |          | Periodic Server |
       -------------------          -------------------
                |                            |
        ----------------------------------------------
        |              |              |              |
    ----------     ----------     ----------     ----------
    | Worker |     | Worker |     | Worker |     | Worker |
    ----------     ----------     ----------     ----------

Initially, the workers register functions they can perform with each
job server. Clients will then connect to a job server and issue a
request to a job to be run. The job server then notifies each worker
that can perform that job (based on the function it registered) that
a new job is ready. The first worker to wake up and retrieve the job
will then execute it.

All communication between workers or clients and the periodic server
are binary.


## Binary Packet

Requests and responses are encapsulated by a binary packet. A binary
packet consists of a header which is optionally followed by data. The
header is:

    4 byte size         - A big-endian (network-order) integer containing
                          the size of the data being sent.

    ? byte  message id  - A client unique message id.
    1 byte  command     - A big-endian (network-order) integer containing
                          an enumerated packet command. Possible values are:

                        #   Name          Type
                        0   NOOP          Client/Worker
                        1   GRAB_JOB      Worker
                        2   SCHED_LATER   Worker
                        3   WORK_DONE     Worker
                        4   WORK_FAIL     Worker
                        5   JOB_ASSIGN    Worker
                        6   NO_JOB        Worker
                        7   CAN_DO        Worker
                        8   CANT_DO       Worker
                        9   PING          Client/Worker
                        10  PONG          Client/Worker
                        11  SLEEP         Worker
                        12  UNKNOWN       Client/Worker
                        13  SUBMIT_JOB    Client
                        14  STATUS        Client
                        15  DROP_FUNC     Client
                        16  SUCCESS       Client/Worker


Arguments given in the data part are separated by a NULL byte.


## Client/Worker Requests

These request types may be sent by either a client or a worker:

    PING

        When a periodic server receives this request, it simply generates a
        PONG packet. This is primarily used for testing
        or debugging.

        Arguments:
        - None.


## Client/Worker Responses

These response types may be sent to either a client or a worker:

    PONG

        This is sent in response to a PING request.

        Arguments:
        - None.

## Client Requests

These request types may only be sent by a client:

    SUBMIT_JOB

        A client issues one of these when a job needs to be run. The
        server will then assign a job handle and respond with a SUCCESS
        packet.

        Arguments:
        - JSON byte job object.

    STATUS

        This sends back a list of all registered functions.  Next to
        each function is the number of jobs in the queue, the number of
        running jobs, and the number of capable workers. The format is:

        FUNCTION,TOTAL_WORKER,TOTAL_JOB,PROCESSING_JOB

        Arguments:
        - None.

    DROP_FUNC

        Drop the function when there is not worker registered, and respond with
        a SUCCESS packet.

        Arguments:
        - Function name.


## Client Responses

These response types may only be sent to a client:

    SUCCESS

        This is sent in response to one of the SUBMIT_JOB* packets. It
        signifies to the client that a the server successfully received
        the job and queued it to be run by a worker.

        Arguments:
        - None.


## Worker Requests

These request types may only be sent by a worker:

    CAN_DO

        This is sent to notify the server that the worker is able to
        perform the given function. The worker is then put on a list to be
        woken up whenever the job server receives a job for that function.

        Arguments:
        - Function name.

    CANT_DO

         This is sent to notify the server that the worker is no longer
         able to perform the given function.

         Arguments:
         - Function name.

    SLEEP

        This is sent to notify the server that the worker is about to
        sleep, and that it should be woken up with a NOOP packet if a
        job comes in for a function the worker is able to perform.

        Arguments:
        - None.

    GRAB_JOB

        This is sent to the server to request any available jobs on the
        queue. The server will respond with either NO_JOB or JOB_ASSIGN,
        depending on whether a job is available.

        Arguments:
        - None.

    WORK_DONE

        This is to notify the server that the job completed successfully.

        Arguments:
        - NULL byte terminated job handle.
        - Opaque data that is returned to the client as a response.

    WORK_FAIL

        This is to notify the server that the job failed.

        Arguments:
        - Job handle.

    SCHED_LATER

        This is to notify the server to do the job on next time.

        Arguments:
        - Job handle.
        - Time delay.


## Worker Responses

These response types may only be sent to a worker:

    NOOP

        This is used to wake up a sleeping worker so that it may grab a
        pending job.

        Arguments:
        - None.

    NO_JOB

        This is given in response to a GRAB_JOB request to notify the
        worker there are no pending jobs that need to run.

        Arguments:
        - None.

    JOB_ASSIGN

        This is given in response to a GRAB_JOB request to give the worker
        information needed to run the job. All communication about the
        job (such as status updates and completion response) should use
        the handle, and the worker should run the given function with
        the argument.

        Arguments:
        - JSON byte job object.

*/
package protocol
