import threading
import json
from multiprocessing import Process
import time
import socket
from threading import Lock

# ports = [6001, 6002, 6003]
from signal import signal, SIGPIPE, SIG_DFL


class Server():
    def __init__(self, port, pid):
        self.ports = []
        self.lock = Lock()
        self.port = port
        self.pid = pid
        self.timestamp = 0
        self.queue = []
        self.output = []
        self.ack = {}
        self.sent_ack = {}
        self.s = socket.socket()
        self.s.bind(("127.0.0.1", port))
        self.s.listen(1)

    def start(self):
        while True:
            conn, addr = self.s.accept()
            t = threading.Thread(target=self.on_new_msg, args=(conn, addr))
            t.start()

    def on_new_msg(self, conn, addr):
        msg = conn.recv(1024)
        msg = json.loads(msg)
        if msg["conn_type"] == "p2p":
            self.peer2peer(msg)
        elif msg["conn_type"] == "tom":
            self.tom(msg)
        else:
            print "message type not recognized"
            return
        conn.close()

    def peer2peer(self, msg):
        if msg["type"] == "app":
            port = msg["p2p_destination"]
            msg["sendingport"] = self.port
            msg["type"] = "data"
            s = socket.socket()
            s.connect(("127.0.0.1", port))
            s.send(json.dumps(msg))
            s.close()
        elif msg["type"] == "data":
            print "PEER2PEER msg on port: {0} received msg: {1} by port {2}".format(
                self.port, msg["data"], msg["sendingport"])
            return

    def tom(self, msg):
        # self.lock.acquire()
        self.timestamp = max(int(self.timestamp),
                             int(msg["timestamp"])) + 1
        # self.lock.release()
        if msg["type"] == "app":
            self.process_app(msg)
        elif msg["type"] == "data":
            self.process_received_data(msg)
        elif msg["type"] == "ack":
            self.process_ack(msg)
        else:
            print "Invalid message type: ", msg["type"]

    def process_app(self, msg):
        # print "process app for msg {0} on port {1}".format(msg, self.port)
        msg["type"] = "data"
        msg["timestamp"] = self.timestamp+0.1*self.pid
        for port in msg["ports"]:
            self.send_msg(msg, port)

    def send_msg(self, msg, port):
        s = socket.socket()
        s.connect(("127.0.0.1", (port)))
        time.sleep(2)
        s.send(json.dumps(msg))

    def process_received_data(self, msg):
        self.queue.append(msg)
        self.queue = sorted(self.queue, key=lambda x: x["timestamp"])
        self.lock.acquire()
        if len(self.queue) > 0:
            if self.queue[0]["msg_id"] not in self.sent_ack:
                self.sent_ack[self.queue[0]["msg_id"]] = 1
                for port in msg["ports"]:
                    msg["type"] = "ack"
                    msg["ack_timestamp"] = self.timestamp+0.1*self.pid
                    s = socket.socket()
                    s.connect(("127.0.0.1", (port)))
                    s.send(json.dumps(msg))
        self.lock.release()

    def send_ack(self, msg, port):
        msg["type"] = "ack"
        msg["ack_timestamp"] = self.timestamp+0.1*self.pid
        s = socket.socket()
        s.connect(("127.0.0.1", (port)))
        s.send(json.dumps(msg))

    def process_ack(self, msg):
        if msg["msg_id"] in self.ack:
            self.ack[msg["msg_id"]] += 1
        else:
            self.ack[msg["msg_id"]] = 1
        self.lock.acquire()
        while len(self.queue) > 0:
            if len(self.queue) > 0 and self.queue[0]["msg_id"] in self.ack and self.ack[self.queue[0]["msg_id"]] == len(msg["ports"]):
                m1 = self.queue.pop(0)
                self.queue = sorted(self.queue, key=lambda x: x["timestamp"])
                self.output.append(m1["data"])
                print "Consume message with data {0} on port {1} ".format(
                    msg["data"], self.port)
                if len(self.queue) > 0 and self.queue[0]["msg_id"] not in self.sent_ack:
                    self.sent_ack[self.queue[0]["msg_id"]] = 1
                    self.queue[0]["ack_flag"] = 1
                    for port in msg["ports"]:
                        try:
                            self.send_ack(self.queue[0], port)
                        except:
                            "queue empty"
        self.lock.release()
        return 1


def waitforreply(p):
    lock = Lock()
    output = []
    while True:
        if p.output:
            lock.acquire()
            time.sleep(1)
            output.append(p.output)
            p.output = []
            print output
            lock.release()


def application_layer(ip, port, pid):
    print "started server", port
    p = Server(port, pid)
    threading.Thread(target=waitforreply, args=(p,)).start()
    threading.Thread(target=p.start(), args=()).start()
