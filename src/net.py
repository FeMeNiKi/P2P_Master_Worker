"""Minimal TCP helpers for demo purposes."""
import socket
import threading

def start_server(host, port, handler):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen()

    def accept_loop():
        while True:
            conn, addr = s.accept()
            t = threading.Thread(target=handler, args=(conn, addr), daemon=True)
            t.start()

    t = threading.Thread(target=accept_loop, daemon=True)
    t.start()
    return s

def send_line(host, port, line, timeout=5):
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall(line.encode('utf-8'))
        try:
            sock.settimeout(timeout)
            data = sock.recv(4096)
            return data.decode('utf-8')
        except socket.timeout:
            return None
