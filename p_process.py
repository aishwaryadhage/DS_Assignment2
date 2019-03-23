timestamp = 0
holdbackqueue = []
p_id = 0
ack = {}
output = []


def start_pserver(ip, port):
    import zmq
    import threading
    import heapq
    import json

    def start_server(ip, port, ports):
        global timestamp
        global holdbackqueue
        global p_id
        global output
        global ack
        p_id = port % 10

        def send_ack(ports, msg, timestamp, port):
            print "in send ack", ports
            for i, p in enumerate(ports):
                print "multicasting ack to port:", p
                temp = msg
                context = zmq.Context()
                sock = context.socket(zmq.REQ)
                sock.connect("tcp://127.0.0.1:"+str(ports[i]))
                temp["type"] = "ack"
                temp["msg_id"] = str(timestamp+0.1*(port % 10))
                print "port", p
                print "send ack ", temp
                sock.send(json.dumps(temp))

        def send_msg(ports, msg, timestamp, port):
            print "in send message", ports
            for i, p in enumerate(ports):

                print "multicasting msg to port: ", p
                temp = msg
                context = zmq.Context()
                sock = context.socket(zmq.REQ)
                sock.connect("tcp://127.0.0.1:"+str(ports[i]))
                temp["type"] = "data"
                temp["msg_id"] = str(timestamp+0.1*(port % 10))
                print "port", p
                print "send ", temp
                sock.send(json.dumps(temp))

        context = zmq.Context()
        sock = context.socket(zmq.REP)
        sock.bind("tcp://"+ip+":"+str(port))
        while True:
            msg = json.loads(sock.recv())

            print "timestamp of current process", timestamp

            if "msg_id" in msg:
                print "timestamp received from the message", msg["msg_id"]

            if msg["type"] == "app":
                tp = 0
                if "msg_id" in msg:
                    tp = msg["msg_id"].split(".")[0]
                timestamp = max(timestamp, float(tp))+1
                # msg["msg_id"] = str(timestamp+0.1*(port % 10))
                print "multicasting msg to port: ", port

                holdbackqueue.append(
                    [msg["conn_type"], msg["data"], str(timestamp+0.1*(port % 10)), 1])
                print "queue for port: ", port, holdbackqueue
                ack[str(timestamp+0.1*(port % 10))] = 1
                holdbackqueue = sorted(holdbackqueue, key=lambda x: x[2])
                t_ports = list(ports)
                t_ports.remove(port)
                thread = threading.Thread(
                    target=send_msg, args=(t_ports, msg, timestamp, port))
                thread.start()
                thread.join()

            if msg["type"] == "data":
                if "msg_id" in msg:
                    tp = msg["msg_id"].split(".")[0]
                timestamp = max(timestamp, float(tp))+1
                print "timestamp in msg data", port, timestamp
                holdbackqueue.append(
                    [msg["conn_type"], msg["data"], float(msg["msg_id"]), 0])  # timestamp+0.1*p_id])
                holdbackqueue = sorted(holdbackqueue, key=lambda x: x[2])
                print "queue: ", port,  holdbackqueue
                if float(msg["msg_id"]) == holdbackqueue[0][2] and holdbackqueue[0][3] == 0:
                    print "top of queue", holdbackqueue[0], port
                    # t_ports = list(ports)
                    # t_ports.remove(port)
                    thread = threading.Thread(
                        target=send_ack, args=(ports, msg, timestamp, port))
                    thread.start()
                    thread.join()
                    holdbackqueue[0][3] == 1
            elif msg["type"] == "ack":
                if "msg_id" in msg:
                    tp = msg["msg_id"].split(".")[0]
                timestamp = max(timestamp, float(tp))+1

                print "ack dictionary ", ack
                if msg["msg_id"] in ack:
                    ack[msg["msg_id"]] += 1
                else:
                    ack[msg["msg_id"]] = 1
                if ack[msg["msg_id"]] == len(ports):
                    print "message is processed", port
                    toprocess_msg = holdbackqueue.pop(0)
                    output.append(msg["data"])
            sock.send_string(msg["type"])

    thread1 = threading.Thread(
        target=start_server, args=(ip, port, [6001, 6002]))
    thread1.start()
    thread1.join()


if __name__ == "__main__":
    # thread = threading.Thread(target=sender_thread, args=(6001, [6001, 6002]))
    # thread.start()
    # thread.join()
    print "thread finished...exiting"
