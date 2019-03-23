import sys
import json
import random
import socket
import time
import threading

ports = [6001, 6002]

# Two processes sending message at the same time for peer to peer
msg = {
    "data": 'a',
    "conn_type": "p2p",
    "type": "app",
    "timestamp": 0,
    "ack_flag": 0,
    "msg_id": 0,
    "ports": [6002]
}

# Two processes sending message at the same time for peer to peer
msg1 = {
    "data": 'a',
    "conn_type": "tom",
    "type": "app",
    "timestamp": 0,
    "ack_flag": 0,
    "msg_id": 0,
    "ports": [6002]
}


def sendmsg(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    msg["data"] = port % 10
    msg["msg_id"] = random.randint(1, 10000)
    s.send(json.dumps(msg))
    s.close()


lis = []
count = 0
for pid in range(len(ports)):
    lis.append(threading.Thread(
        target=sendmsg, args=(ports[pid],)))

for i in range(len(ports)):
    lis[i].start()
for i in range(len(ports)):
    lis[i].join()
