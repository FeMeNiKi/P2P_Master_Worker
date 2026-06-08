"""Minimal Worker client for demo purposes."""
import argparse
import logging
import time
from .protocol import make_presentation, loads, dumps
from .net import send_line

logger = logging.getLogger('worker')


def run_worker(master_host, master_port, worker_uuid):
    # present to master
    line = make_presentation(worker_uuid)
    resp_raw = send_line(master_host, master_port, line)
    if not resp_raw:
        logger.info('no response to presentation')
        return
    try:
        resp = loads(resp_raw.strip())
    except Exception:
        logger.exception('failed parse response')
        return

    # if assigned a task, process it and report status
    if resp.get('task') and resp.get('task') != 'no_task':
        logger.info('received task: %s', resp)
        # simulate processing
        time.sleep(1)
        status = {"status": "ok", "task": resp.get('task'), "worker_uuid": worker_uuid}
        ack_raw = send_line(master_host, master_port, dumps(status))
        if ack_raw:
            try:
                ack = loads(ack_raw.strip())
                logger.info('received ack: %s', ack)
            except Exception:
                logger.exception('failed parse ack')
    else:
        logger.info('no task assigned')

    # simple heartbeat loop (separate connections per heartbeat)
    try:
        while True:
            hb = {"server_uuid": "unknown", "task": "heartbeat"}
            resp_raw = send_line(master_host, master_port, dumps(hb), timeout=5)
            if resp_raw:
                try:
                    resp = loads(resp_raw.strip())
                    logger.info('heartbeat response: %s', resp)
                except Exception:
                    logger.exception('bad heartbeat response')

            # also send load report every heartbeat
            load_msg = {"type": "load_report", "payload": {"worker_uuid": worker_uuid, "load": 0}}
            try:
                send_line(master_host, master_port, dumps(load_msg), timeout=2)
            except Exception:
                logger.exception('failed to send load report')

            time.sleep(10)
    except KeyboardInterrupt:
        logger.info('worker stopping')


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
