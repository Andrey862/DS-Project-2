import socket
from threading import Condition, Thread
import time
import os
import sys
import random

DN_CLIENT_PORT = 10005
DN_DN_PORT = 10006
connection_to_nn = None


class ContentTable():
    content_table__ = {}

    @classmethod
    def get(cls, chank: int):
        """ ver, del, len """
        if (chank in cls.content_table__):
            v = cls.content_table__[chank]
            return {'ver': v[0], 'del': v[1], 'len': v[2]}
        else:
            return {'ver': -1, 'del': True, 'len': 0}

    @classmethod
    def has(cls, chank, ver):
        return cls.get(chank)['ver'] == ver and not cls.get(chank)['del']

    @classmethod
    def newer(cls, chank, ver):
        return cls.get(chank)['ver'] >= ver

    @classmethod
    def set(cls, chank, ver, del_, len_):
        print(f'Content table: set ch={chank} ver={ver} del={del_}')
        if (chank in cls.content_table__):
            v = cls.content_table__[chank]
            if (v[0] > ver):
                raise Exception("new update is older than current")
        cls.content_table__[chank] = [ver, del_, len_]

    @classmethod
    def save_json(cls):
        pass

    @classmethod
    def load_json(cls):
        pass


class NextDN():
    to_send__ = set()
    next_ip = input("next_ip\n")  # "127.0.0.1"
    prev_ip = input("prev_ip\n")  # "127.0.0.1"

    @classmethod
    def add(cls, chank: bytes, sourse=None):
        cls.to_send__.add(chank)

    @classmethod
    def get(cls):
        if (cls.to_send__):
            return random.sample(cls.to_send__, 1)[0]
        else:
            return None

    @classmethod
    def remove(cls, chank: bytes):
        cls.to_send__.discard(chank)


def get_chank_name(chank: bytes):
    chank = chank.decode()
    return f'__files__/{chank}.chank'


to_bool = {
    b'True': True,
    b'true': True,
    b't': True,
    b'False': False,
    b'false': False,
    b'f': False,
}


def save_to_disk_and_tables(sock: socket.socket, chank: bytes, version: str, deleted: bool, length: int) -> None:
    if (deleted):
        f_name = get_chank_name(chank)
        if os.path.isfile(f_name):
            os.remove(f_name)
        sock.sendall(b'ACK\n')
    else:
        content = b''
        part = b' '
        while(len(content) < length and part != b''):
            part = sock.recv(1024)
            content += part
        with open(get_chank_name(chank), 'wb') as f:
            f.write(content)
        sock.sendall(b'ACK\n')
        length = len(content)

    NextDN.add(chank, sourse="client")
    ContentTable.set(chank, ver=version, del_=deleted, len_=length)


def recv_word(sock, split=b'\n', max_len=20, check_dead=False):
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
        return word, dead
    else:
        return word


class ClientServer(Thread):
    def __init__(self, sock: socket.socket, addr: str) -> None:
        super().__init__(daemon=True)
        self.sock = sock
        self.addr = addr

    def read(self, chank, version):
        if (ContentTable.has(chank, version)):
            with open(get_chank_name(chank), 'rb') as f:
                self.sock.sendall(b'ACK\n')
                self.sock.sendall(f.read())
        else:
            self.sock.sendall(b'No data\n')

    def write(self, chank, version, deleted, length):

        if (ContentTable.get(chank)['ver'] < version):
            self.sock.sendall(b'ACK\n')
        else:
            self.sock.sendall(b'Too old\n')
            return
        save_to_disk_and_tables(self.sock, chank, version, deleted, length)

    def serve(self):
        command = recv_word(self.sock)
        print("Client ", self.addr, command)
        if (command == b'read'):
            chank = recv_word(self.sock)
            version = int(recv_word(self.sock))
            self.read(chank, version)
        elif (command == b'write'):
            chank = recv_word(self.sock)
            version = int(recv_word(self.sock))
            deleted = to_bool[recv_word(self.sock)]
            length = int(recv_word(self.sock))
            self.write(chank, version, deleted, length)
        else:
            self.sock.sendall(b'unknown command "'+command+b'"\n')

    def run(self):
        try:
            self.serve()
        except Exception as e:
            self.sock.sendall(b'Error! ' + repr(e).encode('UTF-8')+b'\n')
            print('error during serving client', e)
        self.sock.close()
        print(f'serverd client {self.addr}')


class ClientListener(Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def action(self):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.bind(('', DN_CLIENT_PORT))
        client_sock.listen()
        print('Client Listener working')
        while(True):
            con, addr = client_sock.accept()
            print(f'connected to {addr}')
            ClientServer(con, addr).start()

    def run(self):
        while (True):
            try:
                self.action()
            except Exception as e:
                print(f'fatal error! Restarting Client Listener: {e}')


class DNListener(Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def action(self, sock: socket.socket, ip: str):
        while(True):
            command = b'heart'
            while (command == b'heart'):
                if (ip != NextDN.prev_ip):
                    print(f'prev ip changed! swithing')
                    return
                command = recv_word(sock)
                if (command == b''):
                    print(f'DNListener: connection closed!')
                    return
                print(f'recive from prev: {command}')
            if (command == b'push'):
                chank = recv_word(sock)
                version = int(recv_word(sock))
                deleted = to_bool[recv_word(sock)]
                length = int(recv_word(sock))
                if (ContentTable.newer(chank, version)):
                    sock.sendall(b'Have\n')
                else:
                    sock.sendall(b'ACK\n')
                    save_to_disk_and_tables(
                        sock, chank, version, deleted, length)

    def run(self):
        while (True):
            try:
                print('Init PrevDNListener; Waiting for connection')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(('', DN_DN_PORT))
                sock.listen()
                addr = ''
                while(addr != NextDN.prev_ip):
                    con, addr = sock.accept()
                    addr, port = addr
                    if (addr != NextDN.prev_ip):
                        print(f'{addr} connected, expected {NextDN.prev_ip}')
                        con.close()

                print('Accept PrevDNListener')
                con.settimeout(90.0)
                self.action(con, addr)
                sock.close()
            except Exception as e:
                print(f'fatal error! Restarting DN Listener: {e}')
                time.sleep(0.5)


class DNPusher(Thread):
    # exist only one to not overload network
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def action(self,  sock: socket.socket, ip):
        while(True):
            chank = NextDN.get()
            if (chank == None):
                #print(f'pushing heart to {ip}')
                print(f'nothing to push to {ip}, sleep 10sec')
                # sock.sendall(b'heart\n')
                time.sleep(10)
                continue
            meta = ContentTable.get(chank)
            print(f'pushing {chank} to {ip}')
            sock.sendall(
                f"push\n{chank.decode()}\n{meta['ver']}\n{meta['del']}\n{meta['len']}\n".encode())
            res = recv_word(sock)
            if (res == b'Have'):
                print(f"Pusher: next already has {chank}")
                NextDN.remove(chank)
            elif (res == b'ACK'):
                print(f"Pusher: sending {chank}")
                if (not meta['del']):
                    with open(get_chank_name(chank), 'rb') as f:
                        sock.sendall(f.read())
                res = recv_word(sock)
                if (res != b'ACK'):
                    print(f'Unexpected responce {res}, expacted ACK')
                NextDN.remove(chank)

            if (NextDN.next_ip != ip):
                print('Next DN changed swithing!')
                return

    def run(self):
        while (True):
            try:
                with socket.socket() as s:
                    ip = NextDN.next_ip
                    s.connect((ip, DN_DN_PORT))
                    print('Connected Pusher')
                    self.action(s, ip)
            except Exception as e:
                print(f'fatal error! Restarting DN Pusher: {repr(e)}')
                time.sleep(1.5)


if __name__ == "__main__":
    # connect_to_nn()
    # init_global_variables
    # read existing files
    # delete marked as del in content table
    # update content table for missing values
    # read lab6 https://gist.github.com/gordinmitya/349f4abdc6b16dc163fa39b55544fd34
    # my solution to lab6 https://github.com/Andrey862/repo

    push_only = to_bool[input("push only\n").encode()]
    read_only = to_bool[input("read only\n").encode()]
    print(push_only, read_only)
    cl = ClientListener()
    if (not push_only):
        dl = DNListener()
    if (not read_only):
        dp = DNPusher()

    cl.start()
    if (not push_only):
        dl.start()
    if (not read_only):
        dp.start()

    cl.join()
    if (not push_only):
        dl.join()
    if (not read_only):
        dp.join()
