import zmq
import sys
import json
import random
import socket
import time

msg = {
    "data": 'b',
    "type": 'app',
    "conn_type": "tom",
    "type": 'app',
    "timestamp": 0,
    "ack_flag": 0,
    "msg_id": random.randint(1, 10000),
    "ports": [6001]
}

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 6002))
# time.sleep(2)
s.send(json.dumps(msg))
print "msg sent"
s.close()
