import zmq
import threading
import heapq
import json
from multiprocessing import Process
import time
import socket
import Queue

ports = [6001, 6002]


class Server():
    def __init__(self, port):
        self.port = port
        self.test_flag = 0
        self.pid = port % 10
        self.timestamp = 0
        self.queue = []
        self.output = []
        self.ack = {}
        self.sent_ack = {}
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind(("127.0.0.1", port))
        self.s.listen(1)

    def start(self):
        while True:
            conn, addr = self.s.accept()
            threading.Thread(target=self.on_new_msg, args=(conn, addr)).start()

    def on_new_msg(self, conn, addr):
        msg = conn.recv(1024)
        msg = json.loads(msg)
        import os
        msg["pid"] = os.getpid()
        if msg["conn_type"] == "p2p":
            self.peer2peer(msg)
        elif msg["conn_type"] == "tom":
            self.tom(msg)
        else:
            return

    def peer2peer(self, msg):
        if msg["type"] == "app":
            port = msg["ports"][0]
            msg["type"] = "data"
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", port))
            s.send(json.dumps(msg))
            s.close()
        elif msg["type"] == "data":
            print "msg on port: {0} received msg: {1}".format(self.port, msg)
            return

    def tom(self, msg):
        if msg["type"] == "app":
            thread = threading.Thread(
                target=self.process_app, args=(msg, ))
            thread.start()
        elif msg["type"] == "data":
            thread = threading.Thread(
                target=self.process_received_data, args=(msg, ))
            thread.start()
        elif msg["type"] == "ack":
            thread = threading.Thread(
                target=self.process_ack, args=(msg, ))
            thread.start()
        else:
            print "Invalid message type: ", msg["type"]

    def process_app(self, msg):
        self.timestamp = max(int(self.timestamp),
                             int(msg["timestamp"])) + 1
        for port in ports:
            thread = threading.Thread(
                target=self.send_msg, args=(msg, port))
            thread.start()

    def send_msg(self, msg, port):
        msg["type"] = "data"
        msg["timestamp"] = self.timestamp+0.1*self.pid
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", (port)))
        s.send(json.dumps(msg))
        s.close()

    def process_received_data(self, msg):
        self.queue.append(msg)
        self.queue = sorted(self.queue, key=lambda x: x["timestamp"])
        self.timestamp = max(int(self.timestamp),
                             int(msg["timestamp"])) + 1
        while self.queue[0] and msg["msg_id"] not in self.sent_ack:
            self.sent_ack[msg["msg_id"]] = 1
            self.queue[0]["ack_flag"] = 1

            for port in ports:
                thread = threading.Thread(
                    target=self.send_ack, args=(self.queue[0], port))
                thread.start()

    def send_ack(self, msg, port):
        msg["type"] = "ack"
        msg["port"] = self.port
        msg["timestamp"] = self.timestamp+0.1*self.pid
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", (port)))
        s.send(json.dumps(msg))
        s.close()

    def process_ack(self, msg):
        self.timestamp = max(int(self.timestamp),
                             int(msg["timestamp"])) + 1
        if msg["port"] in self.ack:
            self.ack[msg["msg_id"]] += 1
        else:
            self.ack[msg["msg_id"]] = 1
        if self.ack[msg["msg_id"]] == len(ports):
            self.output.append(msg["data"])
            self.ack = {}


def waitforreply(p):
    while True:
        if p.output:
            # print "yes"
            print "output", p.output
            p.output = []


def application_layer(ip, pid):
    print "started server", pid
    p = Server(ports[pid])
    threading.Thread(target=waitforreply, args=(p,)).start()
    threading.Thread(target=p.start(), args=(ip, pid)).start()


def init_cluster(n):
    for pid in range(n):
        try:
            p = Process(target=application_layer, args=(
                "127.0.0.1", pid))
            p.start()
        except:
            print "process could not be instantiated"


init_cluster(2)
