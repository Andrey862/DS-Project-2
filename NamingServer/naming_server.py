import os
import copy
import time
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
current_file = filesystem


class BackupDaemon(Thread):
    def remove_circular_reference(self, fs):
        if type(fs) is dict:
            fs.pop('..', None)
            for k in list(fs.keys()):
                self.remove_circular_reference(fs[k])

    def run(self):
        while 1:
            time.sleep(300)

            path = os.path.dirname(os.path.realpath(__file__)) + '\\backup\\'
            if not os.path.exists(path):
                os.mkdir(path)

            print(filesystem)
            with open(path + "filesystem.json", "w") as f:
                fs = copy.deepcopy(filesystem)
                self.remove_circular_reference(fs)
                f.write(json.dumps(fs, indent=4))
            with open(path + "chunks.json", "w") as f:
                f.write(json.dumps(chunks, indent=4))


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

    def access_filesystem(self, path, add_missing=True):
        fs = current_file
        folders = self.split_path(path)

        for f in folders:
            if not f in fs:
                if add_missing:
                    fs[f] = {"..": fs}
                else:
                    return None
            fs = fs[f]

        return (fs, folders[-1])

    def write(self, filename, filesize):
        fl = self.access_filesystem(filename)
        if 'chunks' not in fl:
            fl['name'] = filename
            fl['size'] = filesize
            fl['chunks'] = []

        N = math.ceil(filesize / CHUNK_SIZE)
        M = len(fl['chunks'])

        for cid in fl['chunks'][:min(N, M)]:
            chunks[cid]['ver'] += 1

        if N > M:
            for _ in range(N - M):
                chunk_id = str(uuid.uuid4())
                storage_server = random.choice(storage_servers)

                fl['chunks'].append(chunk_id)
                chunks[chunk_id] = {
                    "id": chunk_id, "ips": [storage_server], "ver": 1, "del": False
                }
        elif N < M:
            for cid in fl['chunks'][N:]:
                chunks[cid]['del'] = True
                chunks[cid]['ver'] += 1

        self.sock.sendall(json.dumps(
            fl['chunks'], indent=4).encode())

    def delete(self, filename):
        if 'chunks' in filename:
            for cid in filename['chunks']:
                chunks[cid]['del'] = True
        else:
            for k in list(filename.keys()):
                self.delete(filename[k])

    def ls(self, fs, rec, tab=0):
        ls = ""
        for k in list(fs.keys()):
            ls = '-' * tab + ' ' if tab else ''
            ls += k
            if 'size' in fs[k]:
                ls += f" : {str(fs[k]['size'])}b"
            ls += '\n'

            if 'size' not in current_file[k]:
                ls += self.ls(rec, tab + 1)
        return ls

    def run(self):
        global current_file
        args = self.sock.recv(CHUNK_SIZE).decode().split("?")

        if args[0] == 'write':
            self.write(args[1], int(args[2]))
        elif args[0] == 'ls':
            fs, _ = self.access_filesystem(args[1], False)
            if fs:
                ls = self.ls(fs, args[2] == '-r').encode()
            else:
                ls = "Directory not found"
            self.sock.sendall(ls.encode())
        elif args[0] == 'cd':
            fs, _ = self.access_filesystem(args[1], False)
            if fs:
                current_file = fs['..'] if 'chunks' in fs else fs
            else:
                self.sock.sendall("Directory not found".encode())
        elif args[0] == 'mkdir':
            self.access_filesystem(args[1])
        elif args[0] == 'rm':
            fs, fn = self.access_filesystem(args[1])
            if fs:
                self.delete(fs)
                del fs['..'][fn]
            else:
                self.sock.sendall("Directory not found".encode())

        self.close()


def main():
    BackupDaemon().start()

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
