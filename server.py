import socket
import hmac
import hashlib
from threading import Thread, Condition, Lock
from multiprocessing import Process
import sys
import time
import random
import pickle
import signal

SECRET = b"this is a very secret secret"
MSGLEN = 4 + 4 + 32 + 64
S_DELAY = 0.0
all_sockets = {}

# Calculate the number of clusters
def calculate_num_clusters(start_port, stop_port, num_faulty_nodes):
    num_nodes = stop_port - start_port + 1
    num_clusters = num_nodes // (3 * num_faulty_nodes + 1)
    return num_clusters

# Cluster randomly into 3f+1 nodes
def clustering(start_port, stop_port, num_faulty_nodes):
    num_nodes = stop_port - start_port + 1
    num_clusters = num_nodes // (3 * num_faulty_nodes + 1)
    all_ports = list(range(start_port, stop_port + 1))
    clusters = []
    for _ in range(num_clusters):
        # Randomly choose 3f+1 nodes
        current_cluster = random.sample(all_ports, 3 * num_faulty_nodes + 1)
        all_ports = list(set(all_ports) - set(current_cluster))
        clusters.append(current_cluster)

    return clusters

# Generate the clusterlist.py file with correct permission 
def generate_clusterlist_file(clusters):
    with open("clusterlist.py", "wb") as fp:   # Pickling
        pickle.dump(clusters, fp)

# Range partitioner for distributing keys among shards
class KeyRangePartitioner:
    def __init__(self, num_shards):
        self.num_shards = num_shards
        self.shard_size = 10000 // self.num_shards
        self.shard_partitions = [i * self.shard_size for i in range(self.num_shards + 1)]

    def get_shard_id(self, key):
        key_hash = hashlib.sha1(key.encode('utf-8')).hexdigest()
        key_index = int(key_hash, 16)
        shard_id = key_index % self.num_shards
        return shard_id


# Sharded Key-value store where transactions are insert, delete, update, or get
class KeyValueStore:
    def __init__(self, start_port, stop_port, num_faulty_nodes):
        self.num_clusters = calculate_num_clusters(start_port, stop_port, num_faulty_nodes)
        self.num_shards = self.num_clusters
        self.clusters = {i: {} for i in range(self.num_clusters)}
        self.key_partitioner = KeyRangePartitioner(self.num_shards)

    def get_shard_id(self, key):
        return self.key_partitioner.get_shard_id(key)

    def insert(self, key, value):
        shard_id = self.get_shard_id(key)
        if shard_id in self.clusters:
            self.clusters[shard_id][key] = value
        else:
            print("Invalid shard_id:", shard_id)

    def delete(self, key):
        shard_id = self.get_shard_id(key)
        if key in self.clusters[shard_id]:
            del self.clusters[shard_id][key]

    def update(self, key, value):
        shard_id = self.get_shard_id(key)
        if key in self.clusters[shard_id]:
            self.clusters[shard_id][key] = value

    def get(self, key):
        shard_id = self.get_shard_id(key)
        if key in self.clusters[shard_id]:
            return self.clusters[shard_id][key]
        return None


def initial_sharding(start_port, stop_port, num_faulty_nodes):
    kvs = KeyValueStore(start_port, stop_port, num_faulty_nodes)

    for i in range(10000):
        key = f"key{i+1}"
        value = f"value{i+1}"
        kvs.insert(key, value)

    return kvs

class PBFTSim:
    def __init__(self, listen_port, replica_addr):
        self.listen_port = listen_port
        self.replica_addr = replica_addr

        self.awaiting_prepare = {}
        self.awaiting_prepare_cond = Condition()

        self.awaiting_preprepare = []
        self.awaiting_preprepare_cond = Condition()

        self.awaiting_commit = {}
        self.awaiting_commit_cond = Condition()

        self.committed = set()
        self.committed_cond = Condition()

        self.conns_lock = Lock()
        self.conns = {}

    def log(self, *msg):
        # print(self.listen_port, *msg)
        pass

    def start(self):
        try:
            self.ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.ss.bind(("localhost", self.listen_port))
            self.ss.listen(5)

            print("Listening on port", self.listen_port)
            while True:
                (cs, _addr) = self.ss.accept()
                t = Thread(target=self.handle_client, args=(cs,))
                t.start()
        except OSError as e:
            print("Error:", e)
            sys.exit(1)

    def handle_client(self, cs):
        with cs:
            while True:
                bytes_recvd = 0
                buf = []
                while bytes_recvd < MSGLEN:
                    chunk = cs.recv(MSGLEN - bytes_recvd)
                    if not chunk:
                        break
                    buf += chunk
                    bytes_recvd += len(chunk)

                if not buf:
                    break
                buf = bytes(buf)
                assert len(buf) == MSGLEN

                # validate the hmac -- note we have no nounce, we aren't trying to stop
                # replay attacks, just simulate the CPU cost of the hmac
                code = buf[0:4].decode("UTF-8")
                client_id = int(buf[4:8].decode("UTF-8"))
                msg_hmac = buf[8:4+4+32]
                msg = buf[4+4+32:]
                computed_hmac = hmac.new(SECRET, msg=msg, digestmod="sha256").digest()
                msg = msg.decode("UTF-8")
                if not hmac.compare_digest(msg_hmac, computed_hmac):
                    raise RuntimeError("HMAC error")

                if msg := self.process_msg(code, client_id, msg):
                    with self.committed_cond:
                        while msg not in self.committed:
                            self.committed_cond.wait()
                        self.committed.remove(msg)
                    cs.sendall("COMM".encode("UTF-8"))

    def send_msg(self, dest, code, data):
        code = code.encode("UTF-8")
        client_id = str(self.listen_port).encode("UTF-8")
        data = data.encode("UTF-8")
        msg_hmac = hmac.new(SECRET, data, digestmod="sha256").digest()

        with self.conns_lock:
            self.log("Sending", code.decode("UTF-8"), "message to", dest)
            if dest not in self.conns:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(("localhost", dest))
                self.conns[dest] = s
            s = self.conns[dest]
            s.sendall(code + client_id + msg_hmac + data)

    def process_msg(self, code, client_id, msg):
        self.log("Got", code, "message from", client_id)
        if code == "REQP":
            # request to primary from client -- send a preprepare to all replics
            assert client_id == 0
            for other_port in self.replica_addr:
                if other_port == self.listen_port:
                    continue
                self.send_msg(other_port, "PPRE", msg)

            with self.awaiting_prepare_cond:
                self.awaiting_prepare[msg] = set(self.replica_addr) - set([self.listen_port])
                self.awaiting_prepare_cond.notify_all()
            return msg
        elif code == "REQR":
            # request to replica
            assert client_id == 0
            with self.awaiting_preprepare_cond:
                self.awaiting_preprepare.append(msg)
                self.awaiting_preprepare_cond.notify_all()
            return msg

        elif code == "PPRE":
            # pre-prepare message, send a PREP to all
            assert client_id != 0

            # wait to make sure we have the request from the client
            with self.awaiting_preprepare_cond:
                while msg not in self.awaiting_preprepare:
                    self.log("waiting on REQR")
                    self.awaiting_preprepare_cond.wait()
                self.awaiting_preprepare.remove(msg)

            for other_port in self.replica_addr:
                if other_port == self.listen_port:
                    continue
                self.send_msg(other_port, "PREP", msg)

            with self.awaiting_prepare_cond:
                self.awaiting_prepare[msg] = set(self.replica_addr) - set([self.listen_port, client_id])
                self.awaiting_prepare_cond.notify_all()

        elif code == "PREP":
            with self.awaiting_prepare_cond:
                while msg not in self.awaiting_prepare:
                    self.log("waiting on PPRE or REQP")
                    self.awaiting_prepare_cond.wait()
                assert client_id in self.awaiting_prepare[msg]
                self.awaiting_prepare[msg].remove(client_id)
                if not self.awaiting_prepare[msg]:
                    # commit
                    del self.awaiting_prepare[msg]

                    for other_port in self.replica_addr:
                        if other_port == self.listen_port:
                            continue
                        self.send_msg(other_port, "COMM", msg)

                    with self.awaiting_commit_cond:
                        self.awaiting_commit[msg] = set(self.replica_addr) - set([self.listen_port])
                        self.awaiting_commit_cond.notify_all()

        elif code == "COMM":
            with self.awaiting_commit_cond:
                while msg not in self.awaiting_commit:
                    self.log(f"waiting on PREP (COMM from {client_id})")
                    self.awaiting_commit_cond.wait()
                assert client_id in self.awaiting_commit[msg], f"duplicate commit from {client_id}"
                self.awaiting_commit[msg].remove(client_id)

                if not self.awaiting_commit[msg]:
                    # it's committed!
                    del self.awaiting_commit[msg]

                    with self.committed_cond:
                        self.committed.add(msg)
                        self.committed_cond.notify_all()
        else:
            print("got unknown code", code)
        return None


def signal_handler(sig, frame):
    #kill all the processes
    print("Exiting")
    sys.exit(0)

start_port = int(sys.argv[1])
stop_port = int(sys.argv[2])
num_faulty_nodes = int(sys.argv[3])
clusters = clustering(start_port, stop_port, num_faulty_nodes)
generate_clusterlist_file(clusters)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTSTP, signal_handler)
processes = []

for cluster in clusters:
    all_ports = cluster
    for port in all_ports:
        try:
            server = PBFTSim(port, all_ports)
            p = Process(target=server.start)
            p.start()
            processes.append(p)
        except OSError as e:
            print("Error:", e)
            sys.exit(1)

for p in processes:
    p.join()
