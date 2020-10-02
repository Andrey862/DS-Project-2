import base64
import socket
from threading import Condition, Thread
import time
import os

content = None
dn_status_table = None
file_system = None  # dictionary of dictionaries of dictionaries
dn_connections = None


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


class ContentTableSaver(Thread):
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


class NewDNListener(Thread):
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


class DNUpdatesListener(Thread):
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
    # read lab6 https://gist.github.com/gordinmitya/349f4abdc6b16dc163fa39b55544fd34
    # my solution to lab6 https://github.com/Andrey862/repo
    dn_connections = []
    pass
