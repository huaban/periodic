def Encode(obj):
    msg = ""

    msg += encodeHeader(0)
    for k, values in obj.items():
        msg += encodeNameList(k, values)

    return msg

def encodeHeader(msgtype):
    return "{0:0>3};".format(msgtype)

def encodeString(s):
    return "{}:{},".format(len(s), s)

def encodeList(l):
    values = []
    for s in l:
        values.append(encodeString(s))

    return encodeString("".join(values))

def encodeNameList(name, l):
    return encodeString(name) + encodeList(l)


class DecodeError(Exception):
    pass

def Decode(msg):
    msgtype, skip = decodeHeader(msg)

    if msgtype != 0:
        raise DecodeError("unknown message type: %d"%msgtype)

    msg = msg[skip:]

    obj = dict()

    while len(msg) > 0:
        k, skip = decodeString(msg)
        msg = msg[skip:]

        values, skip = decodeList(msg)
        msg = msg[skip:]
        obj[k] = values

    return obj

def decodeList(msg):
    blob, skip = decodeString(msg)

    l = []
    while len(blob) > 0:
        v, skipv = decodeString(blob)
        l.append(v)
        blob = blob[skipv:]

    return l, skip


def decodeString(msg):
    parts = msg.split(":", 1)

    if len(parts) != 2:
        raise DecodeError("invalid format: no column")

    length = int(parts[0])

    if len(parts[1]) < length + 1:
        raise DecodeError("message '%s' is %d bytes, expected at least %d"%(parts[1], len(parts[1]), length+1))

    payload = parts[1][:length + 1]
    if payload[length] != ',':
        raise DecodeError("message is not comma-terminated")

    return payload[:length], len(parts[0]) + 1 + length + 1


def decodeHeader(msg):
    if len(msg) < 4:
        raise DecodeError("message too small")

    return int(msg[:3]), 4

EncodeString = encodeString
EncodeList = encodeList
DecodeString = decodeString
DecodeList = decodeList
