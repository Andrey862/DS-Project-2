import base64
import socket
from threading import Condition, Thread
import time
import os

connection_to_nn = None


class ContentTable():
    content_table__ = {1: [1, False]}

    @classmethod
    def get(cls, chank):
        if (chank in cls.content_table__):
            v = cls.content_table__[chank]
            return {'ver': v[0], 'del': v[1]}
        else:
            return {'ver': -1, 'del': True}

    @classmethod
    def has(cls, chank, ver):
        return cls.get(chank)['ver'] == ver and not cls.get(chank)['del']

    @classmethod
    def set(cls, chank, ver, del_):
        print(f'set {chank} {ver} {del_}')
        if (chank in cls.content_table__):
            v = cls.content_table__[chank]
            if (v[0] > ver):
                raise Exception("new update is older than current")
        cls.content_table__[chank] = [ver, del_]

    @classmethod
    def save_json(cls):
        pass

    @classmethod
    def load_json(cls):
        pass


dn_status_table = None

dn_client_port = 10005


def get_chank_name(chank):
    return f'__files__/{chank}.chank'


to_bool = {
    b'True': True,
    b'true': True,
    b't': True,
    b'False': False,
    b'false': False,
    b'f': False,
}


def recv_word(sock):
    word = b""
    for _ in range(20):
        rcv = sock.recv(1)
        if rcv == b'\n':
            break
        word += rcv
    return word


class ClientServer(Thread):
    def __init__(self, sock: socket.socket, addr) -> None:
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
        if (deleted):
            f_name = get_chank_name(chank)
            if os.path.isfile(f_name):
                os.remove(f_name)
            self.sock.sendall(b'ACK\n')
        else:
            content = b''
            while(len(content) < length):
                part = self.sock.recv(1024)
                content += part
            with open(get_chank_name(chank), 'wb') as f:
                f.write(content)
            self.sock.sendall(b'ACK\n')

        ContentTable.set(chank, ver=version, del_=deleted)

    def serve(self):
        command = recv_word(self.sock)
        print(command)
        if (command == b'read'):
            chank = int(recv_word(self.sock))
            version = int(recv_word(self.sock))
            self.read(chank, version)
        elif (command == b'write'):
            chank = int(recv_word(self.sock))
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
            self.sock.sendall(b'Error! '+repr(e).encode('UTF-8')+b'\n')
            print('error during serving client', e)
        self.sock.close()
        print(f'serverd {self.addr}')


class ClientListener(Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)

    def action(self):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.bind(('', dn_client_port))
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
                print(f'fuck! we have {e}')


class DNListener(Thread):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(daemon=True)
        self.sock = sock

    def action(self):
        pass

    def run(self):
        while (True):
            try:
                self.action()
            except Exception as e:
                print(f'fuck! we have {e}')


class DNPusher(Thread):
    # exist only one to not overload network
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(daemon=True)
        self.sock = sock

    def action(self):
        pass

    def run(self):
        while (True):
            try:
                self.action()
            except Exception as e:
                print(f'fuck! we have {e}')


if __name__ == "__main__":
    # connect_to_nn()
    # init_global_variables
    # read existing files
    # delete marked as del in content table
    # update content table for missing values
    # read lab6 https://gist.github.com/gordinmitya/349f4abdc6b16dc163fa39b55544fd34
    # my solution to lab6 https://github.com/Andrey862/repo
    cl = ClientListener()
    cl.start()
    cl.join()
