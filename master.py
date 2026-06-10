import socket
import threading
import json
import time
import os
import uuid
from queue import Queue
from threading import Lock


def load_dotenv(path='.env'):
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip())


load_dotenv()

HOST = os.getenv('MASTER_HOST', '127.0.0.1')
PORT = int(os.getenv('MASTER_PORT', '9000'))
NEIGHBORS = [item.strip() for item in os.getenv('NEIGHBORS', '').split(',') if item.strip()]
TASK_COUNT = int(os.getenv('TASK_COUNT', '0'))
CAPACITY = int(os.getenv('CAPACITY', '4'))
RELEASE_THRESHOLD = int(os.getenv('RELEASE_THRESHOLD', '2'))

lock = Lock()
task_queue = Queue()
workers = {}
last_help_attempt = 0.0


def send_json(conn, obj):
    payload = json.dumps(obj, ensure_ascii=False) + '\n'
    conn.sendall(payload.encode('utf-8'))


def register_worker(worker_uuid, conn, addr, borrowed_from=None, temporary=False):
    with lock:
        workers[worker_uuid] = {
            'conn': conn,
            'addr': addr,
            'busy': False,
            'borrowed_from': borrowed_from,
            'temporary': temporary,
            'releasing': False,
        }


def send_task_or_idle(worker_uuid):
    with lock:
        info = workers.get(worker_uuid)
        if not info:
            return
        if not task_queue.empty():
            task = task_queue.get()
            info['busy'] = True
            send_json(info['conn'], {'TASK': 'QUERY', 'USER': task})
        else:
            send_json(info['conn'], {'TASK': 'NO_TASK'})


def current_load():
    with lock:
        busy_workers = sum(1 for info in workers.values() if info['busy'])
    return task_queue.qsize() + busy_workers


def request_help_from_neighbors():
    if not NEIGHBORS:
        print('[Master] No NEIGHBORS configured in .env')
        return False

    request_id = str(uuid.uuid4())
    payload = {
        'master_id': f'{HOST}:{PORT}',
        'current_load': current_load(),
        'capacity': CAPACITY,
        'workers_needed': max(1, current_load() - CAPACITY),
    }
    request = {
        'type': 'request_help',
        'request_id': request_id,
        'payload': payload,
    }

    for peer in NEIGHBORS:
        try:
            host, port = peer.split(':', 1)
            with socket.create_connection((host, int(port)), timeout=5) as conn:
                conn.settimeout(5)
                send_json(conn, request)
                print(f"[Master] request_help sent to {peer} request_id={request_id}")
                response_line = conn.makefile('r', encoding='utf-8', newline='\n').readline()
                if not response_line:
                    continue
                response = json.loads(response_line.strip())
                if response.get('request_id') != request_id:
                    continue
                if response.get('type') == 'response_accepted':
                    print(f"[Master] request_help accepted by {peer}")
                    return True
                print(f"[Master] request_help rejected by {peer}: {response.get('payload', {}).get('reason')}" )
        except Exception as exc:
            print(f"[Master] Failed to contact neighbor {peer}: {exc}")
    return False


def notify_worker_returned(master_address, worker_id):
    try:
        host, port = master_address.split(':', 1)
        with socket.create_connection((host, int(port)), timeout=5) as conn:
            send_json(conn, {
                'type': 'notify_worker_returned',
                'request_id': str(uuid.uuid4()),
                'payload': {'worker_id': worker_id},
            })
    except Exception as exc:
        print(f"[Master] Failed to notify {master_address} that {worker_id} returned: {exc}")


def release_borrowed_workers_if_needed():
    if current_load() > RELEASE_THRESHOLD:
        return
    with lock:
        borrowed = [
            (wid, info['borrowed_from'], info['conn'])
            for wid, info in workers.items()
            if info.get('borrowed_from') and not info.get('releasing')
        ]
        for wid, _, _ in borrowed:
            workers[wid]['releasing'] = True

    for worker_id, original_master, conn in borrowed:
        try:
            if conn.fileno() < 0:
                raise OSError('worker socket already closed')
            send_json(conn, {
                'type': 'command_release',
                'request_id': str(uuid.uuid4()),
                'payload': {'original_master_address': original_master},
            })
            notify_worker_returned(original_master, worker_id)
            print(f"[Master] Released borrowed worker {worker_id} back to {original_master}")
        except Exception as exc:
            print(f"[Master] Failed to release {worker_id}: {exc}")
        finally:
            with lock:
                workers.pop(worker_id, None)


def handle_request_help(conn, message):
    request_id = message.get('request_id')
    payload = message.get('payload', {})
    requester_master = payload.get('master_id', f'{HOST}:{PORT}')
    needed = int(payload.get('workers_needed', 1))
    with lock:
        candidates = [
            (wid, info)
            for wid, info in workers.items()
            if not info.get('borrowed_from') and not info.get('releasing')
        ]

    if len(candidates) < needed:
        send_json(conn, {
            'type': 'response_rejected',
            'request_id': request_id,
            'payload': {'reason': 'no_workers_available'},
        })
        return

    selected = candidates[:needed]
    response = {
        'type': 'response_accepted',
        'request_id': request_id,
        'payload': {
            'workers_offered': len(selected),
            'worker_details': [
                {'id': wid, 'address': f"{info['addr'][0]}:{info['addr'][1]}"}
                for wid, info in selected
            ],
        },
    }
    send_json(conn, response)

    for worker_id, info in selected:
        try:
            send_json(info['conn'], {
                'type': 'command_redirect',
                'request_id': str(uuid.uuid4()),
                'payload': {'new_master_address': requester_master},
            })
            with lock:
                workers[worker_id]['borrowed_from'] = requester_master
                workers[worker_id]['temporary'] = True
            print(f"[Master] Redirected {worker_id} to {requester_master}")
        except Exception as exc:
            print(f"[Master] Failed to redirect {worker_id}: {exc}")


def handle_incoming_message(conn, addr, msg):
    message_type = msg.get('type')

    if message_type == 'request_help':
        handle_request_help(conn, msg)
        return

    if message_type == 'notify_worker_returned':
        worker_id = msg.get('payload', {}).get('worker_id')
        if worker_id:
            with lock:
                info = workers.get(worker_id)
                if info:
                    info['borrowed_from'] = None
                    info['temporary'] = False
        return

    if message_type == 'register_temporary_worker':
        payload = msg.get('payload', {})
        worker_id = payload.get('worker_id')
        original_master = payload.get('original_master_address')
        if worker_id:
            register_worker(worker_id, conn, addr, borrowed_from=original_master, temporary=True)
            send_task_or_idle(worker_id)
        return

    if msg.get('TASK') == 'HEARTBEAT':
        send_json(conn, {'SERVER_UUID': f'Master-{HOST}:{PORT}', 'TASK': 'HEARTBEAT', 'RESPONSE': 'ALIVE'})
        return

    if msg.get('WORKER') == 'ALIVE' and 'WORKER_UUID' in msg:
        worker_id = msg['WORKER_UUID']
        register_worker(worker_id, conn, addr, borrowed_from=msg.get('SERVER_UUID'), temporary=bool(msg.get('SERVER_UUID')))
        send_task_or_idle(worker_id)
        return

    if 'STATUS' in msg and 'TASK' in msg and 'WORKER_UUID' in msg:
        worker_id = msg['WORKER_UUID']
        print(f"[Master] STATUS from {worker_id}: {msg['STATUS']}")
        with lock:
            if worker_id in workers:
                workers[worker_id]['busy'] = False
        send_json(conn, {'STATUS': 'ACK', 'WORKER_UUID': worker_id})
        release_borrowed_workers_if_needed()
        return

    print(f"[Master] Ignored unknown message from {addr}: {msg}")


def connection_loop(conn, addr):
    try:
        file_handle = conn.makefile('r', encoding='utf-8', newline='\n')
        while True:
            line = file_handle.readline()
            if not line:
                break
            try:
                msg = json.loads(line.strip())
            except Exception:
                continue
            handle_incoming_message(conn, addr, msg)
    finally:
        conn.close()


def producer():
    produced = 0
    while True:
        if TASK_COUNT > 0 and produced >= TASK_COUNT:
            time.sleep(1)
            continue
        time.sleep(4)
        task = f"Michel_{produced}"
        task_queue.put(task)
        produced += 1
        print(f"[Master] Enqueuing task {task}")
        if current_load() > CAPACITY:
            now = time.time()
            global last_help_attempt
            if now - last_help_attempt >= 5:
                last_help_attempt = now
                request_help_from_neighbors()


def monitor_release():
    while True:
        time.sleep(2)
        release_borrowed_workers_if_needed()


def start_server():
    threading.Thread(target=producer, daemon=True).start()
    threading.Thread(target=monitor_release, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    print(f"Master listening on {HOST}:{PORT}")
    try:
        while True:
            conn, addr = server.accept()
            threading.Thread(target=connection_loop, args=(conn, addr), daemon=True).start()
    finally:
        server.close()


if __name__ == '__main__':
    start_server()
