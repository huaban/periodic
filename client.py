import socket

def parseHeader(head):
    length = head[0] << 24 | head[1] << 16 | head[2] << 8 | head[3]
    hasFd = length & 0x80000000 != 0
    length = length & ~0x80000000

    return length, hasFd


def main():
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect("libchan.sock")

    head = sock.recv(4)
    length, hasFd = parseHeader(head)
    print(length, hasFd)

    payload = sock.recv(length)
    print(payload)

main()
