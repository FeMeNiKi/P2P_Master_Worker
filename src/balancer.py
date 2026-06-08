"""Simple balancer heuristics (placeholder)."""

def choose_target(peers):
    """Choose peer with smallest load (peers: dict id -> info)."""
    best = None
    best_load = None
    for pid, info in peers.items():
        load = info.get('current_load', 0)
        if best is None or load < best_load:
            best = pid
            best_load = load
    return best
