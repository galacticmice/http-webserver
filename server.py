from socket import socket, AF_INET, SOCK_STREAM, timeout, SOL_SOCKET, SO_REUSEADDR
import threading
import os
from typing import List
from urllib.parse import unquote
from http import HTTPStatus
from email.utils import formatdate
import mimetypes
import signal
import sys
import select

PORT = 8080
ROOT_DIR = os.path.abspath("files")

threads: List[threading.Thread] = []
lock = threading.Lock()
stop_event = threading.Event()
_active = 0
active_lock = threading.Lock()


def inc_active():
    global _active
    with active_lock:
        _active += 1
        return _active


def dec_active():
    global _active
    with active_lock:
        _active = max(0, _active - 1)
        return _active


def keepalive_timeout():
    with active_lock:
        n = _active

    if n < 5:
        return 10
    elif n < 20:
        return 5
    else:
        return 2


def handle_signals(signum, frame):  # override signals to join threads while in use
    stop_event.set()


def parse_args():
    global PORT
    global ROOT_DIR
    args = sys.argv[1:]
    if "-port" in args:
        PORT = int(args[args.index("-port") + 1])
    if "-document_root" in args:
        ROOT_DIR = os.path.abspath(args[args.index("-document_root") + 1])
    if not os.path.isdir(ROOT_DIR):
        print(f"Error: Root directory '{ROOT_DIR}' does not exist.")
        sys.exit(1)


def parse_request(stop_event, connectionSocket):
    request = b""
    while True:
        if stop_event.is_set():
            return None
        try:
            chunk = connectionSocket.recv(4096)
            if not chunk:
                sys.stderr.write("connection disrupted\n")
                sys.stderr.flush()
                raise ValueError("connection disrupted")
            request += chunk
            if b"\r\n\r\n" in request:
                break
        except timeout:
            continue

    lines = request.decode("utf-8", errors="ignore").split("\r\n")
    if len(lines) < 1:
        sys.stderr.write("empty request\n")
        sys.stderr.flush()
        raise ValueError("empty request")

    # first line of request: method | URI | http-version
    req_line = lines[0].split()
    if len(req_line) != 3:
        sys.stderr.write("invalid request line\n")
        sys.stderr.flush()
        raise ValueError("invalid request line")
    method, uri, ver = req_line

    if method not in ("GET", "HEAD"):
        sys.stderr.write("method not supported\n")
        sys.stderr.flush()
        raise ValueError(f"{method} is not supported")

    # get Host and Connection headers for HTTP/1.1
    host = None
    connection = None
    for header in lines[1:]:
        if not header.strip():
            break

        key, sep, value = header.partition(":")
        if not sep:
            continue
        key = key.strip().lower()
        value = value.strip()
        if key == "host":
            host = value
        elif key == "connection":
            connection = value

    # check for Host if HTTP/1.1
    if ver == "HTTP/1.1" and not host:
        sys.stderr.write("missing host header 1.1\n")
        sys.stderr.flush()
        raise ValueError("HTTP/1.1 with missing Host header")

    return {
        "method": method,
        "uri": unquote(uri),
        "version": ver,
        "host": host,
        "connection": connection,
    }


def get_file(uri):
    if uri == "/":
        uri = "/index.html"

    file = os.path.abspath(os.path.join(ROOT_DIR, uri.lstrip("/")))

    # prevent out of bounds requests
    if os.path.commonpath([ROOT_DIR, file]) != ROOT_DIR:
        raise PermissionError("Forbidden: out of bounds traversal")

    # check valid file
    if not os.path.exists(file) or not os.path.isfile(file):
        raise FileNotFoundError(f"{uri} not found in directory")

    # check permissions
    if not os.access(file, os.R_OK):
        raise PermissionError("Permission denied")

    return file


def respond(
    status,
    date,
    version,
    body,
    content_type,
    connection,
):
    response = f"{version} {status.value} {status.phrase}\r\n".encode()
    response += date.encode()
    response += f"Content-Type: {content_type}\r\n".encode()
    if body:
        response += f"Content-Length: {len(body)}\r\n".encode()

    if version == "HTTP/1.1" and connection:
        response += f"Connection: {connection}\r\n\r\n".encode()
    elif status == 400:
        response += b"Connection: close\r\n\r\n"
    else:
        response += b"\r\n"

    if status != HTTPStatus.NO_CONTENT:
        response += body

    return response


def handler(stop_event, connectionSocket, addr):
    inc_active()
    connectionSocket.settimeout(0.5)
    try:
        while True:
            if stop_event.is_set():
                break

            version = "HTTP/1.0"
            connection = None
            body = b""
            content_type = "text/html"
            status = HTTPStatus.OK

            try:
                request = parse_request(stop_event, connectionSocket)
                if request is None:
                    break
                version = request["version"]
                connection = request["connection"]
                file = get_file(request["uri"])
                guessed, _ = mimetypes.guess_type(file)

                content_type = guessed or "application/octet-stream"

                with open(file, "rb") as f:
                    body = f.read() if request["method"] == "GET" else b""

            except ValueError as e:
                status = HTTPStatus.BAD_REQUEST
                body = b"<h1>400 Bad Request</h1>"
                content_type = "text/html"
                print(f"400 due to {e}", flush=True)
            except PermissionError as e:
                status = HTTPStatus.FORBIDDEN
                body = b"<h1>403 Forbidden</h1>"
                content_type = "text/html"
                print(f"forbidden due to {e}", flush=True)
            except FileNotFoundError as e:
                status = HTTPStatus.NOT_FOUND
                body = b"<h1>404 Not Found</h1>"
                content_type = "text/html"
                print(f"not found due to {e}", flush=True)

            date = f"Date: {formatdate(usegmt=True)}\r\n"

            response = respond(status, date, version, body, content_type, connection)
            connectionSocket.sendall(response)

            if version == "HTTP/1.0" or connection == "close" or status >= 400:
                break

            tout = keepalive_timeout()

            if stop_event.is_set():
                tout = 0.1
            ready, _, _ = select.select([connectionSocket], [], [], tout)
            if not ready:
                break
    finally:
        dec_active()
        connectionSocket.close()


if __name__ == "__main__":
    parse_args()
    # override ctrl-z/c signals
    signal.signal(signal.SIGINT, handle_signals)
    signal.signal(signal.SIGTERM, handle_signals)

    serverSocket = socket(AF_INET, SOCK_STREAM)
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serverSocket.bind(("", PORT))
    serverSocket.listen(10)
    serverSocket.settimeout(1.0)
    print(f"Server listening on localhost:{PORT}, root: {ROOT_DIR}\n")

    # timer to periodically check for server close command
    while not stop_event.is_set():
        try:
            connectionSocket, addr = serverSocket.accept()
        except timeout:
            continue
        print(f"{addr} Accepted connection. \n")
        t = threading.Thread(
            target=handler, args=(stop_event, connectionSocket, addr), daemon=True
        )
        t.start()
        with lock:
            threads.append(t)

    # join all stopped threads and close socket
    serverSocket.close()
    with lock:
        thread_list = list(threads)

    for t in thread_list:
        t.join(timeout=3)
