from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import mimetypes

mimetypes.add_type("application/wasm", ".wasm")

class Handler(SimpleHTTPRequestHandler):
    pass

if __name__ == "__main__":
    host = "0.0.0.0"
    port = 8000
    print(f"Serving on http://{host}:{port}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()
