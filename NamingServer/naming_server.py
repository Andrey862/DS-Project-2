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


filesystem = {'type': 'folder', 'name': 'root', '..': None, 'content': {}}
chunks = {}
current_folder = filesystem


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
        fs = current_folder
        folders = self.split_path(path)

        for f in folders:
            if f == '.':
                continue
            if f == '..':
                fs = fs['..'] or fs
                continue
            if not f in fs['content']:
                if add_missing:
                    fs['content'][f] = {
                        "type": "folder", "name": f, "..": fs, "content": {}}
                else:
                    return None, ''
            elif fs['content'][f]['type'] == 'file':
                break
            fs = fs['content'][f]

        return fs

    def write(self, filename, filesize):
        fl = self.access_filesystem(filename)
        if fl['type'] != 'file':
            fl['type'] = 'file'
            fl['size'] = filesize
            fl['content'] = []

        N = math.ceil(filesize / CHUNK_SIZE)
        M = len(fl['content'])

        for cid in fl['content'][:min(N, M)]:
            chunks[cid]['ver'] += 1

        if N > M:
            for _ in range(N - M):
                chunk_id = str(uuid.uuid4())
                storage_server = random.choice(storage_servers)

                fl['content'].append(chunk_id)
                chunks[chunk_id] = {
                    "id": chunk_id, "ips": [storage_server], "ver": 1, "del": False
                }
        elif N < M:
            for cid in fl['content'][N:]:
                chunks[cid]['del'] = True
                chunks[cid]['ver'] += 1

        self.sock.sendall(json.dumps(
            fl['content'], indent=4).encode())

    def delete(self, filename):
        if filename['type'] == 'file':
            for cid in filename['content']:
                chunks[cid]['del'] = True
        else:
            for k in list(filename['content'].keys()):
                self.delete(filename['content'][k])

    def ls(self, fs, rec=False, tab=0):
        ls = ""
        for k in list(fs['content'].keys()):
            ls = '-' * tab + ' ' if tab else ''
            ls += k
            if fs['content'][k]['type'] == 'file':
                ls += f" : {str(fs['content'][k]['size'])}b"
            ls += '\n'

            if rec and fs['content'][k]['type'] == 'folder':
                print(k)
                ls += self.ls(fs['content'][k], rec, tab + 1)
        return ls

    def run(self):
        global current_folder
        args = self.sock.recv(CHUNK_SIZE).decode().split("?")
        print(args)
        if args[0] == 'write':
            self.write(args[1], int(args[2]))
        elif args[0] == 'ls':
            fs = self.access_filesystem(args[1], False)
            print(fs)
            if fs:
                ls = self.ls(fs, len(args) > 2)
            else:
                ls = "Directory not found"
            self.sock.sendall(ls.encode())
        elif args[0] == 'cd':
            fs = self.access_filesystem(args[1], False)
            if fs:
                current_folder = fs
            else:
                self.sock.sendall("Directory not found".encode())
        elif args[0] == 'mkdir':
            self.access_filesystem(args[1])
        elif args[0] == 'rm':
            fs = self.access_filesystem(args[1])
            if fs:
                self.delete(fs)
                del fs['..']['content'][fs['name']]
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
