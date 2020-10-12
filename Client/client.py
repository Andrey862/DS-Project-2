import socket
from threading import Condition, Thread
import time
import json
import os
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

while 1:
    argc = input().split(' ')
    ip, port, action = argc[0:3]
    port = int(port)

    socket = socket.socket()
    socket.connect((ip, port))

    if action == 'write':
        filename = argc[3]
        filesize = os.path.getsize(filename)
        argc = [action, filename, filesize]
    else:
        argc = argc[2:]
        if action == 'ls' and len(argc) < 3:
            argc.append("")

    argc = [str(a) for a in argc]
    send = '\n'.join(argc) + '\n'
    socket.send(send.encode())

    received = socket.recv(CHUNK_SIZE).decode()
    print(received)

    socket.close()
