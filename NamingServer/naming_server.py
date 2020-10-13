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
clients = []
chunks = {}


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
    def get_path(self, addr=""):
        path = 'backup\\'+addr
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    def remove_circular_reference(self, directory):
        if '..' in directory:
            del directory['..']
        if directory['type'] != 'file':
            for name in list(directory['content'].keys()):
                self.remove_circular_reference(directory['content'][name])

    def restore_circilar_reference(self, directory):
        if directory['type'] != 'file':
            for name in list(directory['content'].keys()):
                directory['content'][name]['..'] = directory
                self.restore_circilar_reference(directory['content'][name])

    def backup_client(self, client):
        path = self.get_path(client.addr)

        with open(path + "filesystem.json", "w") as file:
            filesystem = copy.deepcopy(client.filesystem)
            self.remove_circular_reference(filesystem)
            file.write(json.dumps(filesystem, indent=4))

    def backup_chunks(self):
        path = self.get_path()
        with open(path + "chunks.json", "w") as file:
            file.write(json.dumps(chunks, indent=4))

    def run(self):
        while 1:
            time.sleep(1)
            self.backup_chunks()
            for client in clients:
                self.backup_client(client)


class StorageServerListener(Thread):
    def __init__(self, sock, addr):
        super().__init__(daemon=True)
        self.sock = sock
        self.addr = addr

    def close(self):
        i = storage_servers.index(self)
        storage_servers.remove(self)

        if len(storage_servers):
            prev = (i - 1) % len(storage_servers)
            storage_servers[prev].ping_adjacent()
            next = (i + 1) % len(storage_servers)
            storage_servers[next].ping_adjacent()

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
        addr = [s.addr for s in srvr]
        self.sock.sendall(('\n'.join(addr) + '\n').encode())

    def conn(self):
        global storage_servers
        if len(storage_servers) == 0:
            i = 0
            storage_servers.append(self)
        elif self in storage_servers:
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
        try:
            while 1:
                comm = recv_word(self.sock)
                if comm == 'conn':
                    self.conn()
                elif comm == 'upd':
                    cid = recv_word(self.sock)
                    ver = int(recv_word(self.sock))
                    if self.addr not in chunks[cid]['ips']:
                        chunks[cid]['ips'].append(self.addr)
                    chunks[cid]['ver'] = max(ver, chunks[cid]['ver'])
        except Exception as e:
            print(repr(e))

        self.close()


class ClientListener(Thread):
    def __init__(self, sock, addr):
        super().__init__(daemon=True)

        self.sock = sock
        self.addr = addr
        self.filesystem = {'type': 'folder', 'name': 'root', 'content': {}}
        self.current_directory = self.filesystem

        clients.append(self)

    def close(self):
        clients.remove(self)
        self.sock.close()

    def open_directory(self, path, add_missing=True):
        directory = self.current_folder
        path = path.split('/')

        for child in directory:
            if child == '.':
                continue
            if child == '..':
                if '..' in directory:
                    directory = directory['..']
                    continue
                else:
                    return None
            if directory['type'] == 'file':
                break
            if child not in directory['content']:
                if add_missing:
                    directory['content'][child] = {
                        "type": "folder", "name": child, "..": directory, "content": {}}
                else:
                    return None
            directory = directory['content'][child]

        return directory

    def write(self, filename, filesize):
        file = self.open_directory(filename)
        if file['type'] != 'file':
            file['type'] = 'file'
            file['size'] = filesize
            file['content'] = []

        M = len(file['content'])
        N = math.ceil(filesize / CHUNK_SIZE)

        for chunk_id in file['content'][:min(N, M)]:
            chunks[chunk_id]['ver'] += 1

        if N > M:
            for _ in range(N - M):
                chunk_id = str(uuid.uuid4())
                addr = random.choice(storage_servers).addr

                file['content'].append(chunk_id)
                chunks[chunk_id] = {
                    "id": chunk_id, "ips": [addr], "ver": 1, "del": False
                }
        elif N < M:
            for chunk_id in file['content'][N:]:
                chunks[chunk_id]['del'] = True
                chunks[chunk_id]['ver'] += 1

        self.send_chunks(file)

    def delete(self, filename):
        if filename['type'] == 'file':
            for chunk_id in filename['content']:
                chunks[chunk_id]['del'] = True
        else:
            for name in list(filename['content'].keys()):
                self.delete(filename['content'][name])

    def ls(self, directory, recursive=False, tab_level=0):
        result = ""
        for name in list(directory['content'].keys()):
            if tab_level:
                result += '-' * tab_level + ' '

            result += name
            if directory['content'][name]['type'] == 'file':
                result += f" : {directory['content'][name]['size']} bytes"
            result += '\n'

            if recursive and directory['content'][name]['type'] == 'folder':
                result += self.ls(directory['content'][name],
                                  recursive, tab_level + 1)
        return result

    def send_chunks(self, file):
        contents = [chunks[cid] for cid in file['content']]
        text = json.dumps(contents)
        response = f"{file['size']}\n{len(text)}\n{text}\n"
        self.sock.sendall(response.encode())

    def run(self):
        action = recv_word(self.sock)
        if action == 'write':
            if len(storage_servers):
                filename = recv_word(self.sock)
                filesize = int(recv_word(self.sock))
                self.write(filename, filesize)
            else:
                self.sock.sendall("No storate servers found\n".encode())
        elif action == 'read':
            filename = recv_word(self.sock)
            file = self.open_directory(filename)
            self.send_chunks(file)
        elif action == 'ls':
            filename = recv_word(self.sock)
            directory = self.open_directory(filename, False)
            recursive = bool(recv_word(self.sock))
            result = self.ls(
                directory, recursive) if directory else "Directory not found\n"
            self.sock.sendall(f"{len(result)}\n{result}".encode())
        elif action == 'cd':
            directory = self.open_directory(recv_word(self.sock), False)
            if directory:
                self.current_folder = directory
            else:
                self.sock.sendall("Directory not found\n".encode())
        elif action == 'mkdir':
            filename = recv_word(self.sock)
            self.open_directory(filename)
        elif action == 'rm':
            directory = self.open_directory(recv_word(self.sock))
            if directory:
                self.delete(directory)
                del directory['..']['content'][directory['name']]
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
            addr = addr[0]
            self.obj(conn, addr).start()


def main():
    BackupDaemon().start()
    conn_types = ((8800, ClientListener), (8801, StorageServerListener))

    for (port, obj) in conn_types:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', port))
        sock.listen()
        PortListener(sock, obj).start()


if __name__ == "__main__":
    main()
