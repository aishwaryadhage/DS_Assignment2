import zmq
import sys
import json
import random
import socket
import time
import threading

ports = [6001, 6002, 6003]

msg = {
    "data": 'a',
    "conn_type": "tom",
    "type": "app",
    "timestamp": 0,
    "ack_flag": 0,
    "msg_id": 0,
    "ports": [6002]
}
val = 0


def sendmsg(port):
    # global val
    # val = val+1
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    msg["data"] = port
    msg["msg_id"] = random.randint(1, 10000)
    # time.sleep(3)
    s.send(json.dumps(msg))
    # print "msg sent"
    s.close()


# threading.Thread(
#     target=sendmsg, args=(6001,)).start()

lis = []
count = 0
for pid in range(3):
    lis.append(threading.Thread(
        target=sendmsg, args=(ports[pid],)))
    # if count == 3:
    #     break
    # lis.append(threading.Thread(
    #     target=sendmsg, args=(ports[pid],)))
for i in range(3):
    lis[i].start()
for i in range(3):
    lis[i].join()
