#!/usr/bin/env python3
"""Linux process acceptance: fake Sonos discovery, SIGTERM, and failed listeners.

Uses temporary state and private loopback listeners. CI suppresses mDNS advertising;
no physical Sonos device or sender is used. Socket and subprocess waits are bounded.
"""
import argparse
import contextlib
import html
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def get(port, path):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


class FakeSonos(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, body):
        encoded = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/xml")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        self.reply("<root><device><roomName>Test &amp; Room</roomName>"
                   "<modelName>Fake Sonos</modelName><UDN>uuid:RINCON_TEST</UDN></device></root>")

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        action = self.headers.get("SOAPAction", "").strip('"').split("#")[-1]
        if action == "GetZoneGroupState":
            ip = self.server.server_address[0]
            topology = (f'<ZoneGroups><ZoneGroup Coordinator="RINCON_TEST" ID="test">'
                        f'<ZoneGroupMember UUID="RINCON_TEST" ZoneName="Test &amp; Room" '
                        f'Location="http://{ip}:1400/xml/device_description.xml"/>'
                        '</ZoneGroup></ZoneGroups>')
            payload = f"<ZoneGroupState>{html.escape(topology)}</ZoneGroupState>"
        elif action == "GetVolume":
            payload = "<CurrentVolume>30</CurrentVolume>"
        elif action == "GetMute":
            payload = "<CurrentMute>0</CurrentMute>"
        elif action == "GetTransportInfo":
            payload = "<CurrentTransportState>STOPPED</CurrentTransportState>"
        else:
            payload = ""
        self.reply(f'<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">'
                   f'<s:Body><{action}Response>{payload}</{action}Response></s:Body></s:Envelope>')


def check(binary, ip, failure):
    with tempfile.TemporaryDirectory(prefix="airsonos2-process-") as directory, contextlib.ExitStack() as resources:
        ports = set()
        while len(ports) < 3:
            ports.add(free_port())
        http_port, rtsp_port, diagnostics_port = ports
        root = Path(directory)
        config = root / "config.toml"
        config.write_text(f'''[server]
bind = "127.0.0.1"
http_port = {http_port}
state_dir = "{root / 'state'}"
[airplay]
base_rtsp_port = {rtsp_port}
[sonos]
auto_discover = false
static_ips = ["{ip}"]
[stream]
codec = "wav"
[diagnostics]
metrics_addr = "127.0.0.1:{diagnostics_port}"
''')
        if failure:
            occupied = resources.enter_context(socket.socket())
            occupied.bind(("127.0.0.1", http_port if failure == "http" else diagnostics_port))
            occupied.listen()
        with (root / "process.log").open("w+") as log:
            process = subprocess.Popen([str(binary), "serve", "--config", str(config)],
                                       env={**os.environ, "CI": "true", "LC_ALL": "C"}, stdout=log, stderr=log)
            try:
                if failure:
                    assert process.wait(timeout=15) != 0, "listener failure must return an error"
                    log.flush()
                    log.seek(0)
                    output = log.read()
                    assert "Address already in use" in output, f"expected bind failure, got: {output}"
                    assert "shutdown exceeded" not in output.lower(), "cleanup exceeded its deadline"
                else:
                    deadline = time.monotonic() + 15
                    while True:
                        if process.poll() is not None:
                            raise AssertionError("daemon exited before readiness")
                        try:
                            if get(http_port, "/healthz")[0] == 200 and get(diagnostics_port, "/healthz")[0] == 200:
                                break
                        except (OSError, http.client.HTTPException):
                            pass
                        assert time.monotonic() < deadline, "daemon readiness timeout"
                        time.sleep(0.02)
                    assert get(http_port, "/metrics")[0] == 404
                    assert get(diagnostics_port, "/metrics")[0] == 200
                    idle = resources.enter_context(socket.create_connection(("127.0.0.1", rtsp_port), timeout=1))
                    started = time.monotonic()
                    process.send_signal(signal.SIGTERM)
                    assert process.wait(timeout=12) == 0
                    assert idle.recv(1) == b"", "idle RTSP connection must close"
                    log.flush()
                    log.seek(0)
                    assert "shutdown exceeded" not in log.read().lower(), "cleanup exceeded its deadline"
                    print(f"SIGTERM to process exit: {(time.monotonic() - started) * 1000:.1f} ms")
                for port in [rtsp_port] + ([] if failure == "http" else [http_port]) + ([] if failure == "diagnostics" else [diagnostics_port]):
                    with socket.socket() as probe:
                        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        probe.bind(("127.0.0.1", port))
                print(f"PASS: {failure or 'SIGTERM'} cleanup released listeners")
            except BaseException:
                log.seek(0)
                print(log.read())
                raise
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=3, help="repeat startup/shutdown scenarios (default: 3)")
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("this process fixture requires Linux private loopback addresses")
    if not 1 <= args.cycles <= 100:
        parser.error("--cycles must be between 1 and 100")
    if not args.binary.is_file():
        parser.error(f"binary does not exist: {args.binary}")
    # A private Linux loopback address avoids reserving the user's normal Sonos port.
    ip = f"127.77.{os.getpid() // 250 % 250}.{os.getpid() % 250 + 1}"
    server = ThreadingHTTPServer((ip, 1400), FakeSonos)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        for cycle in range(1, args.cycles + 1):
            print(f"Process lifecycle cycle {cycle}/{args.cycles}")
            for failure in [None, "http", "diagnostics"]:
                check(args.binary.resolve(), ip, failure)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
