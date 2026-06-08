"""Lightweight in-memory state for master and peers."""
import threading

class NodeState:
    def __init__(self):
        self.lock = threading.Lock()
        self.peers = {}
        self.tasks_in_progress = 0
        self.queue_size = 0

    def update_peer(self, peer_id, info):
        with self.lock:
            self.peers[peer_id] = info

    def snapshot(self):
        with self.lock:
            return {
                "peers": dict(self.peers),
                "tasks_in_progress": self.tasks_in_progress,
                "queue_size": self.queue_size,
            }
