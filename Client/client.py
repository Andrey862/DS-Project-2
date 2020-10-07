import socket
import tqdm
import sys
import os

CHUNK_SIZE = 4096

action, filename, ip, port = sys.argv[1:5]
port = int(port)
filesize = os.path.getsize(filename)


socket = socket.socket()
print(f"Connecting to {ip}:{port}...")
socket.connect((ip, port))
print("Connected.")
socket.send(f"{action}?{filename}?{filesize}".encode())

storage_server = socket.recv(CHUNK_SIZE).decode()
print("storage_server", storage_server)


# progress = tqdm.tqdm(range(
#     filesize), f"Sending {filename}...", unit="B", unit_scale=True, unit_divisor=1024)


# with open(filename, "rb") as file:
#     while True:
#         content = file.read(1024)
#         if not content:
#             break

#         socket.sendall(content)
#         progress.update(len(content))

socket.close()
# print("Successfully sent.")
