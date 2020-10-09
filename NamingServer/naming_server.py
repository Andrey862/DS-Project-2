import os
import math
import tqdm
import socket
import random
import uuid
import json
from threading import Thread

CHUNK_SIZE = 4096

clients = []
storage_servers = ["localhost:8810",
                   "localhost:8815",
                   "localhost:8838"]


filesystem = {}
chunks = {}


class StorageServerListener(Thread):
    def __init__(self, sock):
        super().__init__(daemon=True)
        self.sock = sock

    def run(self):
        self.sock.recv(CHUNK_SIZE).decode().split("?")

        # Thread to listen one particular client


class ClientListener(Thread):
    def __init__(self, sock):
        super().__init__(daemon=True)
        self.sock = sock

    def close(self):
        clients.remove(self.sock)
        self.sock.close()

    # def handle_filename_collision(self, folders):
        # base, ext = os.path.splitext(filename)
        # result = filename
        # i = 0
        # while result in content_table:
        #     i += 1
        #     result = f"{base}_copy{i}{ext}"
        # return result

    def split_path(self, path):
        folders = []
        while 1:
            path, folder = os.path.split(path)
            if folder != "":
                folders.append(folder)
            else:
                if path != "":
                    folders.append(path)
                break

        folders.reverse()
        return folders

    def access_filesystem(self, path):
        folders = self.split_path(path)

        fs = filesystem
        for f in folders[:-1]:
            if not f in fs:
                fs[f] = {"..": fs}
            fs = fs[f]

        return fs['..'], folders[-1]

    def run(self):
        action, filename, filesize = self.sock.recv(
            CHUNK_SIZE).decode().split("?")

        if (action == 'write'):
            fs_folder, fs_file = self.access_filesystem(filename)
            if fs_file not in fs_folder:
                fs_folder[fs_file] = []

            N = math.ceil(int(filesize) / CHUNK_SIZE)
            M = len(fs_folder[fs_file])

            for i in range(min(N, M)):
                chunks[fs_folder[fs_file][i]]['ver'] += 1

            if N > M:
                for _ in range(N - M):
                    chunk_id = str(uuid.uuid4())
                    storage_server = random.choice(storage_servers)

                    fs_folder[fs_file].append(chunk_id)
                    chunks[chunk_id] = {
                        "id": chunk_id, "ips": [storage_server], "ver": 1, "del": False
                    }
            elif N < M:
                for i in range(M - N):
                    chunks[fs_folder[fs_file][N + i]]['del'] = True
                    chunks[fs_folder[fs_file][N + i]]['ver'] += 1

            result = []
            for c in fs_folder[fs_file]:
                result.append(chunks[c])
            self.sock.sendall(json.dumps(result, indent=4).encode())
            # filename = os.path.basename(filename)
            # filename = self.handle_filename_collision(filename)
            # filesize = int(filesize)

            # print(clients)

            # if action == "read":
            #     if filename in content_table:
            #         address = random.choice(content_table[filename])
            #         self.sock.send(f"{address[0]}:{address[1]}".encode())
            # elif action == "write":
            #     client = random.choice(clients)
            #     if client:
            #         peer = random.choice(clients).getpeername()
            #         addr = f'{peer[0]}:{peer[1]}'
            #         content_table[filename] = [addr]
            #         self.sock.send(addr.encode())

            # print(content_table)
        print("chunks", chunks)
        print("filesystem", filesystem)
        self.close()

        # contents = ""
        # while True:
        #     # try to read CHUNK_SIZE bytes from user
        #     # this is blocking call, thread will be paused here
        #     content = self.sock.recv(CHUNK_SIZE)
        #     if not content:
        #         # if we got no data – client has disconnected


def main():
    types = [[8800, clients, ], [8801, storage_servers]]
    for t in types:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        t.append(sock)

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', t[0]))
        sock.listen()

    t = types[0]
    while True:
        # for t in types:
        con, _ = t[2].accept()
        t[1].append(con)

        if t[0] == 8800:
            ClientListener(con).start()
        else:
            StorageServerListener(con).start()


if __name__ == "__main__":
    main()
