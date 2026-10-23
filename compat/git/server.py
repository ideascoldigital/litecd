"""Smart HTTP git server for the corpus: git http-backend behind Python's
http.server, push enabled, no authentication. Also serves /charts/ (a Helm
repository: index.yaml and packaged charts) as static files.

  /git/<name>.git/...   repositories under /srv/git (created with git init --bare)
  /charts/...           files under /srv/charts
"""

import os
import shutil
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

GIT_ROOT = "/srv/git"
CHARTS = "/srv/charts"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.route()

    def do_POST(self):
        self.route()

    def route(self):
        url = urlsplit(self.path)
        if url.path.startswith("/git/"):
            self.git(url.path[len("/git"):], url.query)
        elif url.path.startswith("/charts/") and self.command == "GET":
            self.static(url.path[len("/charts/"):])
        elif url.path == "/healthz":
            self.reply(200, b"ok\n")
        else:
            self.reply(404, b"not found\n")

    def body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            out = bytearray()
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip(), 16)
                if size == 0:
                    self.rfile.readline()
                    return bytes(out)
                out += self.rfile.read(size)
                self.rfile.readline()
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def git(self, path, query):
        env = {
            "PATH": os.environ["PATH"],
            "GIT_PROJECT_ROOT": GIT_ROOT,
            "GIT_HTTP_EXPORT_ALL": "1",
            "REQUEST_METHOD": self.command,
            "PATH_INFO": path,
            "QUERY_STRING": query,
            "CONTENT_TYPE": self.headers.get("Content-Type", ""),
            "REMOTE_ADDR": self.client_address[0],
            # Lets receive-pack run without http.receivepack in every repo.
            "REMOTE_USER": "compat",
        }
        if self.headers.get("Content-Encoding"):
            env["HTTP_CONTENT_ENCODING"] = self.headers["Content-Encoding"]
        if self.headers.get("Git-Protocol"):
            env["GIT_PROTOCOL"] = self.headers["Git-Protocol"]
        data = self.body() if self.command == "POST" else b""
        env["CONTENT_LENGTH"] = str(len(data))
        p = subprocess.run(["git", "http-backend"], input=data, env=env, capture_output=True)
        head, _, out = p.stdout.partition(b"\r\n\r\n")
        if not _:
            head, _, out = p.stdout.partition(b"\n\n")
        status = 200
        headers = []
        for line in head.decode("latin-1").splitlines():
            k, _, v = line.partition(":")
            if k.lower() == "status":
                status = int(v.strip().split()[0])
            elif k:
                headers.append((k, v.strip()))
        if p.returncode != 0 and not p.stdout:
            status, out = 500, p.stderr
        self.send_response(status)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def static(self, rel):
        path = os.path.normpath(os.path.join(CHARTS, rel))
        if not path.startswith(CHARTS + "/") or not os.path.isfile(path):
            self.reply(404, b"not found\n")
            return
        self.send_response(200)
        self.send_header("Content-Length", str(os.path.getsize(path)))
        self.end_headers()
        with open(path, "rb") as f:
            shutil.copyfileobj(f, self.wfile)

    def reply(self, status, data):
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    os.makedirs(GIT_ROOT, exist_ok=True)
    os.makedirs(CHARTS, exist_ok=True)
    ThreadingHTTPServer(("", 8080), Handler).serve_forever()
