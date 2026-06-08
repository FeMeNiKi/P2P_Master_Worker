"""Minimal Master server for demo purposes."""
import argparse
import logging
import queue
import time
from .net import start_server
from .protocol import loads, dumps

logger = logging.getLogger('master')

# simple in-memory task queue for demo
TASK_QUEUE = queue.Queue()
WORKERS = {}
MASTER_ID = 'master'

def handle_conn(conn, addr):
    try:
        with conn:
            data = b''
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
                while b'\n' in data:
                    line, _, data = data.partition(b'\n')
                    try:
                        msg = loads(line.decode('utf-8'))
                    except Exception:
                        logger.exception('invalid message from %s', addr)
                        continue

                    # heartbeat from worker
                    if msg.get('task') == 'heartbeat':
                        resp = {"server_uuid": MASTER_ID, "task": "heartbeat", "response": "alive"}
                        conn.sendall(dumps(resp).encode('utf-8'))
                        continue

                    # worker presentation
                    if msg.get('worker') == 'alive':
                        worker_uuid = msg.get('worker_uuid')
                        WORKERS[worker_uuid] = {'addr': addr, 'last_seen': time.time()}
                        # try to assign a task
                        try:
                            task = TASK_QUEUE.get_nowait()
                            conn.sendall(dumps(task).encode('utf-8'))
                        except queue.Empty:
                            conn.sendall(dumps({"task": "no_task"}).encode('utf-8'))
                        continue

                    # status report from worker
                    if 'status' in msg:
                        logger.info('status from worker: %s', msg)
                        ack = {"status": "ack", "worker_uuid": msg.get('worker_uuid')}
                        conn.sendall(dumps(ack).encode('utf-8'))
                        continue

                    logger.info('unhandled message: %s', msg)
    except Exception:
        logger.exception('connection handler failed')


def main():
    global MASTER_ID
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=6000)
    p.add_argument('--master-id', default='master_a')
    args = p.parse_args()
    MASTER_ID = args.master_id
    logging.basicConfig(level=logging.INFO)
    logger.info('starting master %s on %s:%d', MASTER_ID, args.host, args.port)

    # preload some demo tasks
    for i in range(3):
        TASK_QUEUE.put({"task": "query", "user": f"demo{i+1}"})

    start_server(args.host, args.port, handle_conn)
    # keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('shutting down')


if __name__ == '__main__':
    main()
