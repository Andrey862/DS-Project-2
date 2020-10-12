from sys import version
import socket
from threading import Condition, Thread
import time
import os
import sys
import random
import json
from typing import List

DN_CLIENT_PORT = 8803
DN_DN_PORT = 8804
NS_PORT = 8801
BACKUP_PERIOD = 90


class NS():
    con = None

    @classmethod
    def connect(cls):
        try:
            NS_ip = os.environ['NN']
        except Exception:
            print('ns ip = 127.0.0.1')
            NS_ip = '127.0.0.1'
        print("NS ip is ", NS_ip)
        #NS_ip = '127.0.0.1'
        cls.con = socket.socket()
        cls.con.connect((NS_ip, NS_PORT))
        cls.con.sendall(b'conn\n')
        print('sent conn')
        cls.prev_ip, cls.next_ip = recv_word(cls.con), recv_word(cls.con)
        print('ips ', cls.prev_ip, cls.next_ip)
        for row in ContentTable.get_all():
            cls.send_update(row['chank'], row['ver'])
        Thread(target=cls.listen_for_updates).start()
        return None

    @classmethod
    def get_ips(cls):
        return cls.prev_ip.decode(), cls.next_ip.decode()

        # return [input("prev_ip\n"), input("next_ip\n")]

    @classmethod
    def send_update(cls, chank, version):
        print('sent ', f'upd\n{chank.decode()}\n{version}\n'.encode())
        cls.con.sendall(f'upd\n{chank.decode()}\n{version}\n'.encode())

    @classmethod
    def listen_for_updates(cls):
        while (True):
            try:
                cls.prev_ip, cls.next_ip = recv_word(
                    cls.con), recv_word(cls.con)
                print('rcv ips: ', cls.get_ips())
                NextDN.prev_ip, NextDN.next_ip = cls.get_ips()
            except Exception as e:
                print('exception during nn listaning', repr(e))


class ContentTable():
    content_table__ = {}
    BACKUP_PATH = '__files__/DN_backup.json'

    @classmethod
    def get_all(cls):
        for chank in cls.content_table__:
            yield cls.get(chank)

    @classmethod
    def get(cls, chank: bytes):
        """ ver, del, len """
        if (chank in cls.content_table__):
            v = cls.content_table__[chank]
            return {'ver': v[0], 'del': v[1], 'len': v[2], 'chank': chank}
        else:
            return {'ver': -1, 'del': True, 'len': 0, 'chank': chank}

    @classmethod
    def has(cls, chank, ver):
        return cls.get(chank)['ver'] == ver and not cls.get(chank)['del']

    @classmethod
    def set(cls, chank, version, deleted, lenngth):
        print(f'Content table: set ch={chank} ver={version} del={deleted}')
        if (chank in cls.content_table__):
            v = cls.content_table__[chank]
            if (v[0] > version):
                raise Exception("new update is older than current")
        cls.content_table__[chank] = [version, deleted, lenngth]

    @classmethod
    def remove(cls, chank):
        cls.content_table__.pop(chank)

    @classmethod
    def to_json(cls):
        table = {e[0].decode(): e[1] for e in cls.content_table__.items()}
        return json.dumps(table)

    @classmethod
    def from_json(cls, content):
        table = json.loads(content)
        cls.content_table__ = {e[0].encode(): e[1] for e in table.items()}


class NextDN():
    to_send__ = set()
    next_ip = None
    prev_ip = None

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
    ContentTable.set(chank, version=version, deleted=deleted, lenngth=length)
    NS.send_update(chank, version)


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
        return word, dead
    else:
        return word


class BackupDaemon(Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def run(self):
        print('Started Backup demon')
        while True:
            try:
                time.sleep(BACKUP_PERIOD)
                with open(ContentTable.BACKUP_PATH, 'w') as f:
                    f.write(ContentTable.to_json())
                print("Content table backup performed")
                
            except Exception as e: 
                print(f'Error during content table backup: {repr(e)}')


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
            print('error during serving client', e)
            #self.sock.sendall(b'Error! ' + repr(e).encode('UTF-8')+b'\n')
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
                print(f'Error! Restarting Client Listener: {e}')


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
                if (ContentTable.get(chank)['ver'] >= version):
                    sock.sendall(b'Have\n')
                else:
                    sock.sendall(b'ACK\n')
                    save_to_disk_and_tables(
                        sock, chank, version, deleted, length)
            elif (command == b'ct'):
                ct = ContentTable.to_json()
                sock.sendall(f'{len(ct)}\n{ct}'.encode())
                res = recv_word(sock)
                if (res != b'ACK'):
                    print('DNListener: unknown responce: ', res)

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
               # con.settimeout(120.0)
                self.action(con, addr)
                con.close()
                sock.close()
            except Exception as e:
                print(f'Error! Restarting DN Listener: {e}')
                time.sleep(0.5)
            finally:
                try:
                    con.close()
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass


class DNPusher(Thread):
    # exist only one to not overload network
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def request_content_table(self, sock: socket.socket):
        sock.sendall(b'ct\n')
        length = recv_word(sock).decode()
        print('length: ', length)
        length = int(length)
        content = b''
        part = b' '
        while(len(content) < length and part != b''):
            part = sock.recv(1024)
            content += part
        sock.sendall(b'ACK\n')
        print('recived ct: ', content[:100])
        table = json.loads(content.decode())
        for row in ContentTable.get_all():
            if row['chank'] not in table or table[row['chank'].decode()] < row['ver']:
                NextDN.add(row['chank'])

    def action(self,  sock: socket.socket, ip):
        while(True):
            if (ip != NextDN.next_ip):
                print('Pusher: ip rotated!')
                return
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
                    print('Connected Pusher, requesting content table')
                    self.request_content_table(s)
                    print('Got next DN table')
                    self.action(s, ip)
            except Exception as e:
                print(f'Error! Restarting DN Pusher: {repr(e)}')
                time.sleep(3)


def remove_inconsistency():
    files = [os.path.splitext(f) for f in os.listdir(
        '__files__') if os.path.isfile(os.path.join('__files__', f))]
    disk_chanks = {e[0].encode() for e in files if e[1] == '.chank'}

    # search for missing values
    for row in tuple(ContentTable.get_all()):
        if row['chank'] not in disk_chanks:
            print('inconsistency, missing chank: ', row['chank'])
            ContentTable.remove(row['chank'])
    # remove ghost chanks
    for file in disk_chanks:
        if ContentTable.get(file)['del']:
            print('inconsistency, ghost chank: ', file)
            os.remove(get_chank_name(file))


if __name__ == "__main__":
    if os.path.isfile(ContentTable.BACKUP_PATH):
        with open(ContentTable.BACKUP_PATH, 'r') as f:
            try:
                ContentTable.from_json(f.read())
            except json.decoder.JSONDecodeError as e:
                print('backup corrupted! Cleaning content')

    remove_inconsistency()
    NS.connect()
    NextDN.prev_ip, NextDN.next_ip = NS.get_ips()

    services = []
    services.append(BackupDaemon())
    services.append(ClientListener())
    services.append(DNListener())
    services.append(DNPusher())

    for s in services:
        s.start()

    for s in services:
        s.join()
