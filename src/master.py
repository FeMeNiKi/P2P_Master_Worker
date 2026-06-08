"""Minimal Master server for demo purposes."""
import argparse
import logging
import queue
import time
import threading
import uuid
from .net import start_server, send_line
from .protocol import loads, dumps

logger = logging.getLogger('master')

# simple in-memory task queue for demo
TASK_QUEUE = queue.Queue()
WORKERS = {}
MASTER_ID = 'master'
PEERS = []  # list of (host,port)
MASTER_ADDR = None

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
                    # master-to-master messages use 'type'
                    if msg.get('type'):
                        mtype = msg.get('type')
                        if mtype == 'request_help':
                            # decide if we can offer workers
                            payload = msg.get('payload', {})
                            needed = payload.get('workers_needed', 1)
                            # simple policy: offer if we have more workers than needed
                            available = max(0, len(WORKERS) - 0)
                            if available >= needed:
                                # offer dummy worker details (ids)
                                offered = []
                                for i, wid in enumerate(list(WORKERS.keys())[:needed]):
                                    offered.append({"id": wid, "address": f"{WORKERS[wid]['addr'][0]}:{WORKERS[wid]['addr'][1]}"})
                                resp = {"type": "response_accepted", "request_id": msg.get('request_id'), "payload": {"workers_offered": needed, "worker_details": offered}}
                            else:
                                resp = {"type": "response_rejected", "request_id": msg.get('request_id'), "payload": {"reason": "no_workers_available"}}
                            conn.sendall(dumps(resp).encode('utf-8'))
                            continue
                        if mtype == 'instruct_redirect':
                            payload = msg.get('payload', {})
                            wid = payload.get('worker_id')
                            new_addr = payload.get('new_master_address')
                            # find worker and send command_redirect over its conn
                            if wid in WORKERS and WORKERS[wid].get('conn'):
                                try:
                                    worker_conn = WORKERS[wid]['conn']
                                    cmd = {"type": "command_redirect", "request_id": str(uuid.uuid4()), "payload": {"new_master_address": new_addr}}
                                    worker_conn.sendall(dumps(cmd).encode('utf-8'))
                                    logger.info('sent command_redirect to worker %s', wid)
                                except Exception:
                                    logger.exception('failed to send command_redirect to worker %s', wid)
                            else:
                                logger.info('worker %s not found to redirect', wid)
                            continue

                    # heartbeat from worker
                    if msg.get('task') == 'heartbeat':
                        resp = {"server_uuid": MASTER_ID, "task": "heartbeat", "response": "alive"}
                        conn.sendall(dumps(resp).encode('utf-8'))
                        continue

                    # worker presentation
                    if msg.get('worker') == 'alive':
                        worker_uuid = msg.get('worker_uuid')
                        WORKERS[worker_uuid] = {'addr': addr, 'last_seen': time.time(), 'status': 'idle', 'conn': conn, 'original_master': None}
                        # try to assign a task
                        try:
                            task = TASK_QUEUE.get_nowait()
                            conn.sendall(dumps(task).encode('utf-8'))
                            WORKERS[worker_uuid]['status'] = 'busy'
                        except queue.Empty:
                            conn.sendall(dumps({"task": "no_task"}).encode('utf-8'))
                        continue

                    # status report from worker
                    if 'status' in msg:
                        logger.info('status from worker: %s', msg)
                        wid = msg.get('worker_uuid')
                        if wid in WORKERS:
                            WORKERS[wid]['status'] = 'idle'
                            WORKERS[wid]['last_seen'] = time.time()
                        ack = {"status": "ack", "worker_uuid": msg.get('worker_uuid')}
                        conn.sendall(dumps(ack).encode('utf-8'))
                        continue

                    # load report
                    if msg.get('type') == 'load_report':
                        payload = msg.get('payload', {})
                        wid = payload.get('worker_uuid')
                        load = payload.get('load')
                        if wid and wid in WORKERS:
                            WORKERS[wid]['load'] = load
                            WORKERS[wid]['last_seen'] = time.time()
                        continue

                    # register temporary worker on this master
                    if msg.get('type') == 'register_temporary_worker':
                        payload = msg.get('payload', {})
                        wid = payload.get('worker_id')
                        orig = payload.get('original_master_address')
                        # treat as a presented worker that is borrowed
                        WORKERS[wid] = {'addr': addr, 'last_seen': time.time(), 'status': 'idle', 'conn': conn, 'original_master': orig}
                        logger.info('registered temporary worker %s from %s', wid, orig)
                        # schedule release after short demo interval
                        def release_after_delay(wid_local, orig_addr, delay=5):
                            time.sleep(delay)
                            # send command_release to this worker
                            if wid_local in WORKERS and WORKERS[wid_local].get('conn'):
                                try:
                                    cmd = {"type": "command_release", "request_id": str(uuid.uuid4()), "payload": {"original_master_address": orig_addr}}
                                    WORKERS[wid_local]['conn'].sendall(dumps(cmd).encode('utf-8'))
                                    logger.info('sent command_release to %s', wid_local)
                                except Exception:
                                    logger.exception('failed to send command_release')
                            # notify original master
                            try:
                                oh, op = orig_addr.split(':')
                                notify = {"type": "notify_worker_returned", "request_id": str(uuid.uuid4()), "payload": {"worker_id": wid_local}}
                                send_line(oh, int(op), dumps(notify), timeout=2)
                                logger.info('notified original master %s that %s returned', orig_addr, wid_local)
                            except Exception:
                                logger.exception('failed to notify original master')
                            # cleanup
                            if wid_local in WORKERS:
                                del WORKERS[wid_local]

                        t = threading.Thread(target=release_after_delay, args=(wid, orig), daemon=True)
                        t.start()
                        continue

                    # command_release handled when sent to worker; master receives notify only from peers
                    if msg.get('type') == 'notify_worker_returned':
                        payload = msg.get('payload', {})
                        wid = payload.get('worker_id')
                        logger.info('peer notified that worker returned: %s', wid)
                        # remove from borrowed list if present
                        if wid in WORKERS and WORKERS[wid].get('original_master'):
                            del WORKERS[wid]
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
    p.add_argument('--peers', default='')
    args = p.parse_args()
    MASTER_ID = args.master_id
    global MASTER_ADDR
    MASTER_ADDR = f"{args.host}:{args.port}"
    logging.basicConfig(level=logging.INFO)
    logger.info('starting master %s on %s:%d', MASTER_ID, args.host, args.port)
    # parse peers list host:port,comma
    if args.peers:
        for part in args.peers.split(','):
            h, pport = part.split(':')
            PEERS.append((h, int(pport)))
    logger.info('peers: %s', PEERS)
    # preload some demo tasks
    for i in range(3):
        TASK_QUEUE.put({"task": "query", "user": f"demo{i+1}"})

    start_server(args.host, args.port, handle_conn)
    # start balancer thread
    t = threading.Thread(target=balancer_loop, daemon=True)
    t.start()
    # keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('shutting down')


def request_help(peer_host, peer_port, workers_needed=1, timeout=3):
    """Send request_help to a peer and wait for response."""
    req_id = str(uuid.uuid4())
    msg = {"type": "request_help", "request_id": req_id, "payload": {"master_id": MASTER_ID, "current_load": sum(1 for w in WORKERS if WORKERS[w].get('status')=='busy'), "capacity": len(WORKERS), "workers_needed": workers_needed}}
    try:
        resp_raw = send_line(peer_host, peer_port, dumps(msg), timeout=timeout)
        if not resp_raw:
            return None
        resp = loads(resp_raw.strip())
        return resp
    except Exception:
        logger.exception('request_help to %s:%s failed', peer_host, peer_port)
        return None


def balancer_loop():
    """Simple loop that checks queue and requests help if overloaded."""
    while True:
        try:
            qsize = TASK_QUEUE.qsize()
            idle_workers = sum(1 for w in WORKERS.values() if w.get('status') == 'idle')
            if qsize > idle_workers and PEERS:
                needed = qsize - idle_workers
                # ask peers in order
                for (h, p) in PEERS:
                    resp = request_help(h, p, workers_needed=needed)
                    if resp and resp.get('type') == 'response_accepted':
                        logger.info('peer %s:%s accepted help: %s', h, p, resp.get('payload'))
                        # instruct peer to redirect specific workers to this master
                        payload = resp.get('payload', {})
                        for w in payload.get('worker_details', []):
                            wid = w.get('id')
                            instruct = {"type": "instruct_redirect", "request_id": str(uuid.uuid4()), "payload": {"worker_id": wid, "new_master_address": MASTER_ADDR}}
                            try:
                                send_line(h, p, dumps(instruct), timeout=2)
                            except Exception:
                                logger.exception('failed to send instruct_redirect to peer')
                        break
            time.sleep(2)
        except Exception:
            logger.exception('balancer loop error')
            time.sleep(2)


if __name__ == '__main__':
    main()
