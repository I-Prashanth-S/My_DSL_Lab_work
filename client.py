import pickle
import socket
import hmac
import random
from tqdm import tqdm
import time
import numpy as np
import sys


SECRET = b"this is a very secret secret"
NUM_TRIALS = 1000

client_id = "0000".encode("UTF-8")
data = "".join([random.choice("ABCDEF") for _ in range(64)]).encode("UTF-8")
msg_hmac = hmac.new(SECRET, data, digestmod="sha256").digest()

start_port = int(sys.argv[1])
stop_port = int(sys.argv[2])
num_faulty_nodes = int(sys.argv[3])

with open("clusterlist.py", "rb") as fp:   #Pickling
    clusters = pickle.load(fp)

#print(clusters)
durations = []
for i in tqdm(range(NUM_TRIALS)):
    start = time.time()
    all_sockets = []

    all_ports = list(random.choice(clusters))

    primary = random.choice(all_ports)
    all_ports.remove(primary)

    code = "REQP".encode("UTF-8")
    to_send = code + client_id + msg_hmac + data

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", primary))
    s.sendall(code + client_id + msg_hmac + data)
    all_sockets.append(s)

    code = "REQR".encode("UTF-8")
    to_send = code + client_id + msg_hmac + data
    for p in all_ports:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("localhost", p))
        s.sendall(code + client_id + msg_hmac + data)
        all_sockets.append(s)

    for s in all_sockets:
        assert s.recv(4).decode("UTF-8") == "COMM"
        s.close()
    
    duration = time.time() - start
    durations.append(duration)

    for s in all_sockets:
        s.close()


print("P50:", 1 / np.quantile(durations, 0.5), "tx/s")
print("P90:", 1 / np.quantile(durations, 0.9), "tx/s")
print("Mean:", 1 / np.mean(durations), "tx/s")
