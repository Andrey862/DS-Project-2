def send_file_to_dn(dn_ip, content, port=10005):
    socket = socket.socket()
    socket.connect((dn_ip, port))
    socket.sendall(b'write\n2\n2\nf\n'+str(len(content)).encode('UTF-8')+b'\n')
    socket.recv(1024)


def ask_client():
    pass


def send_request():
    # also print % in terminal
    pass


if __name__ == "__main__":
    while(True):
        request = ask_client()
        if (request == "exit"):
            break
        send_request(request)
