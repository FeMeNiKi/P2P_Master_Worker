"""Minimal Worker client for demo purposes."""
import argparse
import logging
import time
from .protocol import make_presentation, loads, dumps
import socket
import uuid


logger = logging.getLogger('worker')


def run_worker(master_host, master_port, worker_uuid, original_master=None):
    """Maintain a persistent connection to the master and handle commands.
    If redirected, connect to the new master and register as temporary.
    """
    current_master = (master_host, master_port)
    if original_master is None:
        original_master = f"{master_host}:{master_port}"
    orig_parts = original_master.split(':')
    orig_tuple = (orig_parts[0], int(orig_parts[1]))

    while True:
        try:
            with socket.create_connection(current_master, timeout=5) as sock:
                first_connect = True
                sock_file = sock
                # present
                # if connecting to a non-original master for the first time, register as temporary
                if first_connect and current_master != orig_tuple:
                    reg = {"type": "register_temporary_worker", "request_id": str(uuid.uuid4()), "payload": {"worker_id": worker_uuid, "original_master_address": original_master}}
                    sock.sendall(dumps(reg).encode('utf-8'))
                    first_connect = False
                else:
                    pres = make_presentation(worker_uuid, server_uuid=None)
                    sock.sendall(pres.encode('utf-8'))

                data = b''
                last_load_report = 0
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    while b'\n' in data:
                        line, _, data = data.partition(b'\n')
                        try:
                            msg = loads(line.decode('utf-8'))
                        except Exception:
                            logger.exception('invalid message from master')
                            continue

                        # task assignment
                        if msg.get('task') and msg.get('task') != 'no_task':
                            logger.info('received task: %s', msg)
                            time.sleep(1)
                            status = {"status": "ok", "task": msg.get('task'), "worker_uuid": worker_uuid}
                            sock.sendall(dumps(status).encode('utf-8'))
                            continue

                        # heartbeat responses
                        if msg.get('task') == 'heartbeat':
                            logger.info('heartbeat response: %s', msg)
                            continue

                        # command_redirect
                        if msg.get('type') == 'command_redirect':
                            payload = msg.get('payload', {})
                            new_addr = payload.get('new_master_address')
                            logger.info('received command_redirect to %s', new_addr)
                            # close current socket and connect to new master
                            host, port = new_addr.split(':')
                            current_master = (host, int(port))
                            first_connect = True
                            # break inner loop so outer loop reconnects
                            break

                        # command_release
                        if msg.get('type') == 'command_release':
                            payload = msg.get('payload', {})
                            orig = payload.get('original_master_address') or original_master
                            logger.info('received command_release, returning to %s', orig)
                            host, port = orig.split(':')
                            current_master = (host, int(port))
                            first_connect = True
                            break

                        # ack/status handling is passive here

                    # periodic load report
                    if time.time() - last_load_report > 10:
                        load_msg = {"type": "load_report", "payload": {"worker_uuid": worker_uuid, "load": 0}}
                        try:
                            sock.sendall(dumps(load_msg).encode('utf-8'))
                        except Exception:
                            logger.exception('failed to send load report')
                        last_load_report = time.time()

        except Exception:
            logger.exception('worker connection loop failed')
            time.sleep(2)


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
