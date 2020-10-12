import os
import copy
import time
import math
import socket
import random
import uuid
import json
from threading import Thread

CHUNK_SIZE = 4096

storage_servers = []

filesystem = {'type': 'folder', 'name': 'root', 'content': {}}
chunks = {}
current_folder = filesystem


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
        return word.decode(), dead
    else:
        return word.decode()


class BackupDaemon(Thread):
    def get_path(self):
        path = 'backup\\'
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    def remove_circular_reference(self, fs):
        if '..' in fs:
            del fs['..']
        if fs['type'] != 'file':
            for k in list(fs['content'].keys()):
                self.remove_circular_reference(fs['content'][k])

    def restore_circilar_reference(self, fs):
        if fs['type'] != 'file':
            for k in list(fs['content'].keys()):
                fs['content'][k]['..'] = fs
                self.restore_circilar_reference(fs['content'][k])

    def backup(self):
        path = self.get_path()

        with open(path + "filesystem.json", "w") as f:
            fs = copy.deepcopy(filesystem)
            self.remove_circular_reference(fs)
            f.write(json.dumps(fs, indent=4))
        with open(path + "chunks.json", "w") as f:
            f.write(json.dumps(chunks, indent=4))

    def run(self):
        while 1:
            time.sleep(3)
            self.backup()


class StorageServerListener(Thread):
    def __init__(self, sock, addr):
        super().__init__(daemon=True)
        self.sock = sock
        self.addr = addr

    def close(self):
        storage_servers.remove(self)
        self.sock.close()

    def get_adjacent_servers(self, i):
        addr = []
        ln = len(storage_servers)
        if i == 0:
            addr.append(storage_servers[ln - 1])
        else:
            addr.append(storage_servers[i - 1])
        if i == ln - 1:
            addr.append(storage_servers[0])
        else:
            addr.append(storage_servers[i + 1])
        return addr

    def ping_adjacent(self):
        i = storage_servers.index(self)
        srvr = self.get_adjacent_servers(i)
        addr = [s.addr.split(':')[0] for s in srvr]
        self.sock.sendall('\n'.join(addr) + '\n'.encode())

    def conn(self):
        if len(storage_servers) == 0:
            i = 0
            storage_servers.append(self)
        elif self.addr in storage_servers:
            i = storage_servers.index(self)
        else:
            i = random.randrange(len(storage_servers))
            storage_servers.insert(i, self)

        self.ping_adjacent()
        prev = (i - 1) % len(storage_servers)
        storage_servers[prev].ping_adjacent()
        next = (i + 1) % len(storage_servers)
        storage_servers[next].ping_adjacent()

    def run(self):
        while 1:
            comm = recv_word(self.sock)
            if comm == 'conn':
                self.conn()
            elif comm == 'upd':
                cid = recv_word(self.sock)
                ver = int(recv_word(self.sock))
                chunks[cid]['ips'].append(self.addr)
                chunks[cid]['ver'] = max(ver, chunks[cid]['ver'])

        self.close()


class ClientListener(Thread):
    def __init__(self, sock, addr):
        super().__init__(daemon=True)
        self.sock = sock
        self.addr = addr

    def close(self):
        self.sock.close()

    # def handle_filename_collision(self, fs, filename):
    #     base, ext = os.path.splitext(os.path.basename(filename))
    #     result = base + ext
    #     i = 0
    #     while result in fs['contents']:
    #         i += 1
    #         result = f'{base}_{i}{ext}'
    #     return result

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
                if '..' in fs:
                    fs = fs['..']
                    continue
                else:
                    return None
            if not f in fs['content']:
                if add_missing:
                    fs['content'][f] = {
                        "type": "folder", "name": f, "..": fs, "content": {}}
                else:
                    return None
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
                ls += self.ls(fs['content'][k], rec, tab + 1)
        return ls

    def run(self):
        global current_folder
        comm = recv_word(self.sock)
        if comm == 'write':
            if len(storage_servers):
                filename = recv_word(self.sock)
                filesize = int(recv_word(self.sock))
                self.write(filename, filesize)
            else:
                self.sock.sendall("No storate servers found\n".encode())
        elif comm == 'ls':
            filename = recv_word(self.sock)
            fs = self.access_filesystem(filename, False)
            if fs:
                rec = bool(recv_word(self.sock))
                ls = self.ls(fs, rec)
            else:
                ls = "Directory not found\n"
            self.sock.sendall(ls.encode())
        elif comm == 'cd':
            fs = self.access_filesystem(recv_word(self.sock), False)
            if fs:
                current_folder = fs
            else:
                self.sock.sendall("Directory not found\n".encode())
        elif comm == 'mkdir':
            filename = recv_word(self.sock)
            self.access_filesystem(filename)
        elif comm == 'rm':
            fs = self.access_filesystem(recv_word(self.sock))
            if fs:
                self.delete(fs)
                del fs['..']['content'][fs['name']]
            else:
                self.sock.sendall("Directory not found\n".encode())

        self.close()


class PortListener(Thread):
    def __init__(self, sock, obj):
        super().__init__(daemon=True)
        self.sock = sock
        self.obj = obj

    def run(self):
        while 1:
            conn, addr = self.sock.accept()
            addr = f'{addr[0]}:{addr[1]}'
            self.obj(conn, addr).start()


def main():
    BackupDaemon().start()

    for t in ((8800, ClientListener), (8801, StorageServerListener)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', t[0]))
        sock.listen()
        PortListener(sock, t[1]).start()


if __name__ == "__main__":
    main()
