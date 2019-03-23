import zmq
import threading
import heapq
import json
from multiprocessing import Process
import time
import socket


ports = [6001, 6002]


class Server():
    def __init__(self, port):
        self.port = port
        self.pid = port % 10
        self.timestamp = self.pid * 0.1
        self.queue = []
        self.output = None
        self.ack = {}
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind(("127.0.0.1", port))
        self.s.listen(1)

        self.conn, self.addr = self.s.accept()

        # while 1:
#       data = conn.recv(BUFFER_SIZE)
        # self.context = zmq.Context()
        # self.sock = self.context.socket(zmq.REP)
        # self.sock.bind("tcp://127.0.0.1:"+str(port))

    def start(self):
        print "process with port {0} started".format(self.port)
        while True:
            # time.sleep(1)
            msg = self.conn.recv(1024)
            if not msg:
                break
            print "received data:", msg
            print "process with port {0} listening".format(self.port)
            # data = self.sock.recv()
            msg = json.loads(msg)
            if msg["type"] == "app":
                # call app method
                self.process_app(msg)
            elif msg["type"] == "data":
                # call consume data message method
                self.process_received_data(msg)
            elif msg["type"] == "ack":
                print "received ack msg from a process"
                # acknowledge the message
                self.process_ack(msg)
            else:
                print "Invalid message type"

    def process_app(self, msg):
        print "processing msg {0} on port {1}".format(msg, self.port)
        # self.queue.append(msg)
        # self.queue = sorted(self.queue, key=lambda x: x["timestamp"])

        self.timestamp += 1
        # self.ack[msg["msg_id"]] = 1
        thread = threading.Thread(
            target=self.send_msg, args=(msg,))

        thread.start()
        # msg["ack_flag"] = 1
        thread.join()

    def send_msg(self, msg):
        # continue for self.port else send to other ports
        msg["type"] = "data"
        for port in ports:
            msg["timestamp"] = self.timestamp
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", (port)))

            s.send(json.dumps(msg))
            # data = s.recv(1024)
            s.close()
            # context = zmq.Context()
            # sock = context.socket(zmq.REQ)
            # sock.connect("tcp://127.0.0.1:"+str(port))
            print "sending msg {0} to port {1}".format(msg, port)
            # sock.send(json.dumps(msg))

    def process_received_data(self, msg):
        print "received msg {0} on port {1}".format(msg, self.port)
        self.queue.append(msg)
        self.queue = sorted(self.queue, key=lambda x: x["timestamp"])

        received_time = msg["timestamp"]
        self.timestamp = max(int(self.timestamp),
                             int(received_time)) + 1  # + 0.1*self.pid
        head = self.queue[0]
        print "head of queue {0} on port {1}".format(head, self.port)
        while self.queue:
            if (head["msg_id"] == msg["msg_id"] and head["ack_flag"] == 0) or head["ack_flag"] == 0:
                head["ack_flag"] = 1
                print "inside if condition"
                thread = threading.Thread(
                    target=self.send_ack, args=(msg,))
                thread.start()
                thread.join()

    def send_ack(self, msg):
        # print "send ack test msg {0} on port {1}".format(msg, self.port)
        print "timestamp on port {0} is {1}".format(self.port, self.timestamp)
        msg["type"] = "ack"
        for port in ports:
            # if port == self.port:
            #     print "same port for send ack"
            #     continue
            # else:
            print "trying to send ack to port: ", port
            msg["timestamp"] = self.timestamp+0.1*self.pid
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(("127.0.0.1", (port)))
                # time.sleep(2)
                s.send(json.dumps(msg))
                # time.sleep(2)
                # data = s.recv(1024)
                s.close()
            except socket.error, exc:
                print "Caught exception socket.error : %s" % exc
            # context = zmq.Context()
            # sock = context.socket(zmq.REQ)
            # sock.connect("tcp://127.0.0.1:"+str(port))
            print "sending ack {0} to port {1}".format(msg, port)
            # sock.send(json.dumps(msg))

    def process_ack(self, msg):
        print "process ack {0} on port {1}".format(msg, self.port)

        if msg["msg_id"] in self.ack:
            self.ack[msg["msg_id"]] += 1
        else:
            self.ack[msg["msg_id"]] = 1

        print "ack dictionary {0} on port {1}".format(self.ack, self.port)

        if self.ack[msg["msg_id"]] == len(ports):
            print "Consume message with id ", msg["msg_id"]


def start_thread_server(ip, pid):
    print "started server", pid
    p = Server(ports[pid])
    thread1 = threading.Thread(
        target=p.start(), args=(ip, pid))
    thread1.start()
    # thread1.join()


def init_cluster(n):
    for pid in range(n):
        # try:
        p = Process(target=start_thread_server, args=(
            "127.0.0.1", pid))
        p.start()
        # except:
        #     print "process could not be instantiated"


init_cluster(2)
