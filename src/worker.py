"""Minimal Worker client for demo purposes."""
import argparse
import logging
import socket
from .protocol import make_presentation, loads

logger = logging.getLogger('worker')

def run_worker(master_host, master_port, worker_uuid):
    line = make_presentation(worker_uuid)
    try:
        with socket.create_connection((master_host, master_port), timeout=5) as sock:
            sock.sendall(line.encode('utf-8'))
            try:
                sock.settimeout(5)
                data = sock.recv(4096)
                if data:
                    try:
                        msg = loads(data.decode('utf-8'))
                        logger.info('recv: %s', msg)
                    except Exception:
                        logger.exception('failed parse')
            except socket.timeout:
                logger.info('no response from master')
    except Exception:
        logger.exception('worker failed to connect')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--master', default='127.0.0.1:6000')
    p.add_argument('--worker-uuid', default='w-1')
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    host, port = args.master.split(':')
    run_worker(host, int(port), args.worker_uuid)

if __name__ == '__main__':
    main()
