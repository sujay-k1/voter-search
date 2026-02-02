from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import mimetypes

mimetypes.add_type("application/wasm", ".wasm")

class Handler(SimpleHTTPRequestHandler):
    pass

if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
