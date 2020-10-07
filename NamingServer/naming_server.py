import os
import tqdm
import socket
import random
from threading import Thread

CHUNK_SIZE = 4096

clients = []
storage_servers = [("localhost", 8080), ("localhost", 8081)]
content_table = {}


# Thread to listen one particular client
class ClientListener(Thread):
    def __init__(self, name: str, sock: socket.socket):
        super().__init__(daemon=True)
        self.sock = sock
        self.name = name

    # add 'me> ' to sended message
    def _clear_echo(self, data):
        # \033[F – symbol to move the cursor at the beginning of current line (Ctrl+A)
        # \033[K – symbol to clear everything till the end of current line (Ctrl+K)
        self.sock.sendall('\033[F\033[K'.encode())
        data = 'me> '.encode() + data
        # send the message back to user
        self.sock.sendall(data)

    # broadcast the message with name prefix eg: 'u1> '
    def _broadcast(self, data):
        data = (self.name + '> ').encode() + data
        for u in clients:
            # send to everyone except current client
            if u == self.sock:
                continue
            u.sendall(data)

    # clean up
    def _close(self):
        clients.remove(self.sock)
        self.sock.close()
        print(self.name + ' disconnected')

    def handle_filename_collision(self, filename):
        base, ext = os.path.splitext(filename)
        result = filename
        i = 0
        while result in content_table:
            i += 1
            result = f"{base}_copy{i}{ext}"
        return result

    def run(self):
        received = self.sock.recv(CHUNK_SIZE).decode()
        filename, filesize = received.split(" ")
        filename = os.path.basename(filename)
        filename = self.handle_filename_collision(filename)
        filesize = int(filesize)

        storage_server = random.choice(storage_servers)
        content_table[filename] = [storage_server]
        self.sock.send(f"{storage_server[0]}:{storage_server[1]}".encode())

        # contents = ""
        # while True:
        #     # try to read CHUNK_SIZE bytes from user
        #     # this is blocking call, thread will be paused here
        #     content = self.sock.recv(CHUNK_SIZE)
        #     if not content:
        #         # if we got no data – client has disconnected
        #         self._close()
        #         # finish the thread
        #         return

        # with open(filename, "wb") as file:
        #     file.write(contents)


def main():
    next_name = 1

    # AF_INET – IPv4, SOCK_STREAM – TCP
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # reuse address; in OS address will be reserved after app closed for a while
    # so if we close and imidiatly start server again – we'll get error
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # listen to all interfaces at 8800 port
    sock.bind(('', 8800))
    sock.listen()
    while True:
        # blocking call, waiting for new client to connect
        con, addr = sock.accept()
        clients.append(con)
        name = 'u' + str(next_name)
        next_name += 1
        print(str(addr) + ' connected as ' + name)
        # start new thread to deal with client
        ClientListener(name, con).start()


if __name__ == "__main__":
    main()
