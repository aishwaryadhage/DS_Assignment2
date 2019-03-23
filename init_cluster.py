from p_process import start_pserver
from multiprocessing import Process


def init_cluster(n, ports):
    p_pids = []
    p_ports = ports
    for p_thread in range(n):
        try:
            p = Process(target=start_pserver, args=(
                "127.0.0.1", p_ports[p_thread]))
            p.start()
        except:
            print "process could not be instantiated"


init_cluster(2, [6001, 6002])
