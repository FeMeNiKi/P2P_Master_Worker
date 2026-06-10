import socket
import json
import time
import uuid
import random
import os

HOST = os.getenv('MASTER_HOST', '127.0.0.1')
PORT = int(os.getenv('MASTER_PORT', '9000'))

WORKER_UUID = f"W-{uuid.uuid4()}"

def send_json(s, obj):
    data = json.dumps(obj, ensure_ascii=False) + '\n'
    s.sendall(data.encode('utf-8'))

def recv_line(f, timeout=5):
    start = time.time()
    while True:
        line = f.readline()
        if line:
            try:
                return json.loads(line.decode('utf-8').strip())
            except Exception:
                return None
        if time.time() - start > timeout:
            return None

def connect_and_run(master_host, master_port, original_master=None):
    """Connect to a master and run the worker loop. Returns (should_redirect_to, should_return_to)."""
    addr = (master_host, master_port)
    borrowed_from = original_master
    try:
        s = socket.create_connection(addr, timeout=5)
        f = s.makefile('rwb')
        # If borrowed, register as temporary worker
        if borrowed_from:
            send_json(s, {"type": "register_temporary_worker", "request_id": str(uuid.uuid4()), "payload": {"worker_id": WORKER_UUID, "original_master_address": borrowed_from}})
        else:
            send_json(s, {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID})

        while True:
            resp = recv_line(f, timeout=10)
            if resp is None:
                # timeout or broken
                break

            # Master instructs redirect (Master -> Worker)
            if resp.get('type') == 'command_redirect':
                payload = resp.get('payload', {})
                new_addr = payload.get('new_master_address')
                if new_addr:
                    # request to redirect: close and return new address to connect
                    s.close()
                    return (new_addr, None)

            # Master instructs release (Master -> Worker)
            if resp.get('type') == 'command_release':
                payload = resp.get('payload', {})
                orig = payload.get('original_master_address')
                if orig:
                    s.close()
                    return (None, orig)

            # TASK handling
            if resp.get('TASK') == 'QUERY' and 'USER' in resp:
                duration = random.uniform(0.5, 2.0)
                time.sleep(duration)
                status = random.choice(['OK', 'NOK'])
                send_json(s, {"STATUS": status, "TASK": "QUERY", "WORKER_UUID": WORKER_UUID})
                # wait for ACK
                ack = recv_line(f, timeout=5)
                if ack and ack.get('STATUS') == 'ACK':
                    continue
                else:
                    break

            if resp.get('TASK') == 'NO_TASK':
                # send heartbeat and re-present
                time.sleep(3)
                send_json(s, {"TASK": "HEARTBEAT", "SERVER_UUID": "Master"})
                send_json(s, {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID, "SERVER_UUID": borrowed_from} if borrowed_from else {"WORKER": "ALIVE", "WORKER_UUID": WORKER_UUID})
                continue

        s.close()
    except Exception:
        # connection failed
        pass
    return (None, None)


def run_worker():
    current = f"{HOST}:{PORT}"
    original = None
    while True:
        host, port = current.split(':')
        redirect_to, return_to = connect_and_run(host, int(port), original_master=original)
        if redirect_to:
            # became borrowed: remember original and set current to new master
            if not original:
                original = current
            current = redirect_to
            continue
        if return_to:
            # released: set current to original master
            current = return_to
            original = None
            continue
        # wait a bit then retry
        time.sleep(2)


if __name__ == '__main__':
    print(f"Worker starting with UUID {WORKER_UUID}")
    run_worker()
