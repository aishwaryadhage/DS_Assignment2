import sys
import json
import random
import socket
import time
import threading
import json


def sendmsg(msg, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print port
    s.connect(("127.0.0.1", port))
    msg["data"] = port % 10
    msg["msg_id"] = random.randint(1, 10000)
    s.send(json.dumps(msg))
    s.close()


def test_tom(msg, ports):
    lis = []
    count = 0
    for pid in range(len(ports)):
        lis.append(threading.Thread(
            target=sendmsg, args=(msg, ports[pid])))

    for i in range(len(ports)):
        lis[i].start()
    for i in range(len(ports)):
        lis[i].join()


if __name__ == "__main__":
    msg = {}
    ports = []
    with open('config.json') as json_data_file:
        msg = json.load(json_data_file)
        ports = msg["ports"]
    test_tom(msg, ports)
