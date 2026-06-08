"""Minimal Master server for demo purposes."""
import argparse
import logging
from .net import start_server
from .protocol import loads, dumps

logger = logging.getLogger('master')

def handle_conn(conn, addr):
    try:
        with conn:
            data = b''
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b'\n' in data:
                    line, _, rest = data.partition(b'\n')
                    data = rest
                    try:
                        msg = loads(line.decode('utf-8'))
                        logger.info('recv: %s from %s', msg, addr)
                    except Exception:
                        logger.exception('invalid message')
    except Exception:
        logger.exception('connection handler failed')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=6000)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    logger.info('starting master on %s:%d', args.host, args.port)
    start_server(args.host, args.port, handle_conn)
    # keep main thread alive
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('shutting down')

if __name__ == '__main__':
    main()
