import base64
import socket
from threading import Condition, Thread
import time
import os

connection_to_nn = None
content_table = None
dn_status_table = None


class ClientListener(Thread):
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
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    nn_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    nn_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    dn_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    dn_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_sock.bind(('', 8800))
    client_sock.listen()
    nn_sock.bind(('', 8801))
    nn_sock.listen()
    dn_sock.bind(('', 8802))
    dn_sock.listen()
    while(True):
        condition = True
        if (condition):
            sock = client_sock
        elif (condition):
            sock = nn_sock
        elif (condition):
            sock = dn_sock

        con, addr = sock.accept()
        print('something happend')
        ClientListener(con).start()
