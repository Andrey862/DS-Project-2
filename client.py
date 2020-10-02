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
