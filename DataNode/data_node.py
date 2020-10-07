import base64
import socket
from threading import Condition, Thread
import time
import os

connection_to_nn = None
content_table = {1: [1, False]}
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


class ClientServer(Thread):
    def __init__(self, sock: socket.socket, addr) -> None:
        super().__init__(daemon=True)
        self.sock = sock
        self.addr = addr

    def recv_word(self):
        word = b""
        for _ in range(20):
            word += self.sock.recv(1)
            if word[-1] == b'\n'[0]:
                break
        return word[:-1]

    def read(self, chank, version):
        if (chank in content_table and content_table[chank][0] == version and not content_table[chank][1]):
            with open(get_chank_name(chank), 'rb') as f:
                self.sock.sendall(f.read())
        else:
            self.sock.sendall(b'No data\n')

    def write(self, chank, version, deleted, length):
        if (chank not in content_table or content_table[chank][0] <= version):
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

        content_table[chank] = [version, deleted]

    def serve(self):
        command = self.recv_word()
        print(command)
        if (command == b'read'):
            chank = int(self.recv_word())
            version = int(self.recv_word())
            self.read(chank, version)
        elif (command == b'write'):
            chank = int(self.recv_word())
            version = int(self.recv_word())
            deleted = to_bool[self.recv_word()]
            length = int(self.recv_word())
            self.write(chank, version, deleted, length)
        else:
            self.sock.sendall(b'unknown command "'+command+b'"\n')

    def run(self):
        try:
            self.serve()
        except Exception as e:
            self.sock.sendall(b'Error! '+str(e).encode('UTF-8')+b'\n')
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
    # read lab6 https://gist.github.com/gordinmitya/349f4abdc6b16dc163fa39b55544fd34
    # my solution to lab6 https://github.com/Andrey862/repo
    cl = ClientListener()
    cl.start()
    cl.join()
