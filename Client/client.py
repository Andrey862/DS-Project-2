import socket
from threading import Condition, Thread
import time
import os
import tqdm
import sys

def recv_word(sock):
    word = b""
    for _ in range(20):
        rcv = sock.recv(1)
        if rcv == b'\n':
            break
        word += rcv
    return word


def send_chank_to_dn(dn_ip, chank, version, deleted, content, port=10005):
    with socket.socket() as s:
        s.connect((dn_ip, port))
        d = {True: 't', False: 'f'}
        s.sendall(
            f'write\n{chank}\n{version}\n{d[deleted]}\n{len(content)}\n'.encode('UTF-8'))
        res = recv_word(s)
        if (res != b'ACK'):
            print('fatal after sending params: ', res)
            return res
        if (not deleted):
            s.sendall(content)
        res = recv_word(s)
        if (res != b'ACK'):
            print('fatal during sending/deleting file: ', res)
            return res


def read_chank_from_dn(dn_ip, chank, version, port=10005):
    with socket.socket() as s:
        s.connect((dn_ip, port))
        s.sendall(f'read\n{chank}\n{version}\n'.encode('UTF-8'))
        res = recv_word(s)
        if (res != b'ACK'):
            print('fatal after sending params: ', res)
            return res
        return s.recv(5000)


CHUNK_SIZE = 4096

action, filename, ip, port = sys.argv[1:5]
port = int(port)
filesize = os.path.getsize(filename)


socket = socket.socket()
print(f"Connecting to {ip}:{port}...")
socket.connect((ip, port))
print("Connected.")
socket.send(f"{action}?{filename}?{filesize}".encode())

storage_server = socket.recv(CHUNK_SIZE).decode()
print("storage_server", storage_server)


# progress = tqdm.tqdm(range(
#     filesize), f"Sending {filename}...", unit="B", unit_scale=True, unit_divisor=1024)


# with open(filename, "rb") as file:
#     while True:
#         content = file.read(1024)
#         if not content:
#             break

#         socket.sendall(content)
#         progress.update(len(content))

socket.close()
# print("Successfully sent.")
