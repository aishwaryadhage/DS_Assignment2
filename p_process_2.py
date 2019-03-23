import threading
import json
from multiprocessing import Process
import time
import socket
from threading import Lock

ports = [6001, 6002]


class Server():
    def __init__(self, port):
        self.lock = Lock()
        self.port = port
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
            time.sleep(1)
            threading.Thread(target=self.on_new_msg, args=(conn, addr)).start()

    def on_new_msg(self, conn, addr):
        msg = conn.recv(1024)
        # print "process with port {0} listening".format(self.port)
        msg = json.loads(msg)
        if msg["conn_type"] == "p2p":
            self.peer2peer(msg)
        elif msg["conn_type"] == "tom":
            self.tom(msg)
        else:
            print "message type not recognized"
            return

    def peer2peer(self, msg):
        if msg["type"] == "app":
            port = msg["ports"][0]
            msg["type"] = "data"
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", port))
            s.send(json.dumps(msg))
            # s.close()
        elif msg["type"] == "data":
            print "msg on port: {0} received msg: {1}".format(
                self.port, msg["data"])
            return

    def tom(self, msg):
        if msg["type"] == "app":
            self.process_app(msg)
        elif msg["type"] == "data":
            self.process_received_data(msg)
        elif msg["type"] == "ack":
            self.process_ack(msg)
        else:
            print "Invalid message type: ", msg["type"]

    def process_app(self, msg):
        self.lock.acquire()
        self.timestamp = max(int(self.timestamp),
                             int(msg["timestamp"])) + 1
        self.lock.release()
        print "timestamp of port: {0}  is: {1} and msg: {2}".format(
            self.port, self.timestamp, msg["data"])
        for port in ports:
            self.send_msg(msg, port)

    def send_msg(self, msg, port):
        msg["type"] = "data"
        msg["timestamp"] = self.timestamp+0.1*self.pid
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", (port)))
        s.send(json.dumps(msg))

    def process_received_data(self, msg):
        self.lock.acquire()
        self.queue.append(msg)
        print "msg_id", self.queue[0]["msg_id"]
        self.queue = sorted(self.queue, key=lambda x: x["timestamp"])
        # time.sleep(1)
        self.timestamp = max(int(self.timestamp),
                             int(msg["timestamp"])) + 1

        if self.queue[0]["msg_id"] not in self.sent_ack:
            self.sent_ack[self.queue[0]["msg_id"]] = 1
            for port in ports:
                self.send_ack(self.queue[0], port)
        self.lock.release()

    def send_ack(self, msg, port):
        # print "send ack test msg {0} on port {1}".format(msg, self.port)
        msg["type"] = "ack"
        msg["timestamp"] = self.timestamp+0.1*self.pid
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", (port)))
        s.send(json.dumps(msg))

    def process_ack(self, msg):
        # print "process ack {0} on port {1}".format(msg, self.port)
        self.timestamp = max(int(self.timestamp),
                             int(msg["timestamp"])) + 1
        if msg["msg_id"] in self.ack:
            self.ack[msg["msg_id"]] += 1
        else:
            self.ack[msg["msg_id"]] = 1
        # print self.ack
        # time.sleep(2)
        # print self.ack[self.queue[0]["msg_id"]]
        while len(self.queue) > 0:
            self.lock.acquire()
            # time.sleep(1)
            if len(self.queue) > 0 and self.queue[0]["msg_id"] in self.ack and self.ack[self.queue[0]["msg_id"]] == 2:
                m1 = self.queue.pop(0)
                # time.sleep(1)
                self.queue = sorted(self.queue, key=lambda x: x["timestamp"])
                # time.sleep(1)
                self.output.append(m1["data"])
                print "Consume message with data {0} on port {1} ".format(
                    msg["data"], self.port)
            if len(self.queue) > 0 and self.queue[0]["msg_id"] not in self.sent_ack:
                # print "hello"
                self.sent_ack[self.queue[0]["msg_id"]] = 1
                self.queue[0]["ack_flag"] = 1
                for port in ports:
                    # time.sleep(1)
                    if len(self.queue) > 0:
                        self.send_ack(self.queue[0], port)
                break
            self.lock.release()


def waitforreply(p):
    output = []
    while True:
        if p.output:
            # time.sleep(1)
            # print "output", p.output
            output.append(p.output)
            p.output = []
            print output


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


init_cluster(len(ports))
