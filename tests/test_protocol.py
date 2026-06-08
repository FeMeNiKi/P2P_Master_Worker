from src import protocol

def test_roundtrip():
    msg = {"worker": "alive", "worker_uuid": "w-1"}
    line = protocol.dumps(msg)
    parsed = protocol.loads(line.strip())
    assert parsed['worker'] == 'alive'
    assert parsed['worker_uuid'] == 'w-1'
