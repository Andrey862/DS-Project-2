import socket
from threading import Condition, Thread
import time
import json
import tqdm
import os
import sys
import random


def recv_word(sock, split=b'\n', max_len=256, check_dead=False):
    word = b""
    dead = False
    for _ in range(max_len):
        rcv = sock.recv(1)
        if rcv == b'':
            dead = True
        if rcv == split:
            break
        word += rcv
    if (check_dead):
        return word.decode(), dead
    else:
        return word.decode()


def recv_stream(sock, length):
    content = b''
    part = b' '
    while (len(content) < length and part != b''):
        part = sock.recv(CHUNK_SIZE)
        content += part
    return content.decode()


def send_chank_to_dn(dn_ip, chank, version, deleted, content, port=8803):
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


def read_chank_from_dn(dn_ip, chank, version, port=8803):
    with socket.socket() as s:
        s.connect((dn_ip, port))
        s.sendall(f'read\n{chank}\n{version}\n'.encode('UTF-8'))
        res = recv_word(s)
        if (res != b'ACK'):
            print('fatal after sending params: ', res)
            return res
        return s.recv(CHUNK_SIZE)


CHUNK_SIZE = 4096

while 1:
    argc = input().split(' ')
    ip, port, action = argc[0:3]
    port = int(port)

    sock = socket.socket()
    sock.connect((ip, port))

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
    sock.send(send.encode())

    received = recv_word(sock)

    if action == 'write':
        chunks = recv_stream(sock, int(received))
        chunks = json.loads(chunks)

        progress = tqdm.tqdm(range(
            filesize), f"Sending {filename}...", unit="B", unit_scale=True, unit_divisor=1024)
        with open(filename, 'rb') as f:
            for chunk in chunks:
                if chunk['del']:
                    content = b''
                else:
                    content = f.read(CHUNK_SIZE)

                send_chank_to_dn(random.choice(
                    chunk['ips']), chunk['id'], chunk['ver'], chunk['del'], content)

                progress.update(len(content))
    elif action == 'read':
        chunks = recv_stream(sock, int(received))
        chunks = json.loads(chunks)

        progress = tqdm.tqdm(range(
            filesize), f"Receiving {filename}...", unit="B", unit_scale=True, unit_divisor=1024)
        with open(filename, 'wb') as f:
            for chunk in chunks:
                if chunk['del']:
                    content = b''
                else:
                    content = read_chank_from_dn(random.choice(
                        chunk['ips']), chunk['id'], chunk['ver'])

                f.write(content)
                progress.update(len(content))
    else:
        print(received)

    sock.close()
