from p_process import application_layer
from multiprocessing import Process


def init_cluster(ports):
    n = len(ports)
    p_pids = []
    p_ports = ports
    for p_thread in range(n):
        try:
            p = Process(target=application_layer, args=(
                "127.0.0.1", p_ports[p_thread], p_thread))
            p.start()
        except:
            print "process could not be instantiated"


init_cluster([4212, 4213])
