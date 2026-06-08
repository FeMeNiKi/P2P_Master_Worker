"""Simple HTTP metrics endpoint using http.server."""
import http.server
import json
import threading

class MetricsHandler(http.server.BaseHTTPRequestHandler):
    metrics = {"tasks_in_progress": 0, "queue_size": 0}

    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(self.metrics).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

def start_metrics_server(host='127.0.0.1', port=8000):
    server = http.server.ThreadingHTTPServer((host, port), MetricsHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server
