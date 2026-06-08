"""Simple newline-delimited JSON protocol helpers.

All keys and control values are lowercase (per spec).
"""
import json

def dumps(obj):
    return json.dumps(obj, separators=(',', ':')) + "\n"

def loads(line):
    return json.loads(line)

def make_presentation(worker_uuid, server_uuid=None):
    payload = {"worker": "alive", "worker_uuid": worker_uuid}
    if server_uuid:
        payload["server_uuid"] = server_uuid
    return dumps(payload)
