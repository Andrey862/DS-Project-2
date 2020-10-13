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


def send_chank_to_dn(dn_ip, chank: str, version: int, deleted: bool, content: bytes, port=8803):
    with socket.socket() as s:
        s.connect((dn_ip, port))
        d = {True: 't', False: 'f'}
        s.sendall(
            f'write\n{chank}\n{version}\n{d[deleted]}\n{len(content)}\n'.encode('UTF-8'))
        res = recv_word(s)
        if (res != 'ACK'):
            print('fatal after sending params: ', res)
            return res
        if (not deleted):
            s.sendall(content)
        res = recv_word(s)
        if (res != 'ACK'):
            print('fatal during sending/deleting file: ', res)
            return res


def read_chank_from_dn(dn_ip, chank: str, version: int, port=8803):
    with socket.socket() as s:
        s.connect((dn_ip, port))
        s.sendall(f'read\n{chank}\n{version}\n'.encode('UTF-8'))
        res = recv_word(s)
        if (res != 'ACK'):
            print('fatal after sending params: ', res)
            return res
        return s.recv(CHUNK_SIZE)


CHUNK_SIZE = 4096

ip = input("Input ip:\n")
sock = socket.socket()
sock.connect((ip, 8800))
print("Connected\n")

while 1:
    args = input().split(' ')

    if args[0] == 'exit':
        sock.send(args[0].encode())
        break

    if args[0] == 'write':
        filename = args[1]
        filesize = os.path.getsize(filename)
        args = [args[0], filename, str(filesize)]
    else:
        if args[0] == 'ls' and len(args) < 3:
            args.append("")

    send = '\n'.join(args) + '\n'
    sock.send(send.encode())
    received = recv_word(sock)

    if args[0] == 'write':
        recv_word(sock)
        chunks = recv_stream(sock, int(received))
        chunks = json.loads(chunks)

        print(f"Sending {filename}...")
        with open(filename, 'rb') as f:
            for chunk in chunks:
                if chunk['del']:
                    content = b''
                else:
                    content = f.read(CHUNK_SIZE)

                dip = random.choice(chunk['ips'])
                send_chank_to_dn(
                    dip, chunk['id'], chunk['ver'], chunk['del'], content)
        print(f"Sent {filename}")
    elif args[0] == 'read':
        filesize = int(received)
        jlen = int(recv_word(sock))
        chunks = recv_stream(sock, jlen)
        chunks = json.loads(chunks)
        filename = args[1]

        print(f"Receiving {filename}...")
        with open(filename, 'wb') as f:
            for chunk in chunks:
                if chunk['del']:
                    content = b''
                else:
                    content = read_chank_from_dn(random.choice(
                        chunk['ips']), chunk['id'], chunk['ver'])

                f.write(content)
        print(f"Received {filename}")
    elif args[0] == 'ls':
        ls = recv_stream(sock, int(received))
        print(ls)
    else:
        print(received)


sock.close()
