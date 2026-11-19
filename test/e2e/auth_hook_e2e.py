#!/usr/bin/env python3
"""O-3 端到端验证：HTTP 鉴权回调。

覆盖 done.md 里 O-3 的验收点：

  * RTSP DESCRIBE / ANNOUNCE 被 hook 拒绝时返回 403，放行时正常；
  * RTMP publish / play 被 hook 拒绝时连接失败，放行时能推能拉；
  * hook 传参包含 token、来源 IP、流名；
  * 判定结果按 TTL 缓存：连续两次相同请求只打一次 hook；
  * hook 超时按默认策略处理（本脚本验证默认拒绝）。

脚本会自己编译并启动 flow、起一个 Python hook 服务，不依赖已运行的实例。
只用标准库 + ffmpeg/ffprobe。

用法（在仓库根目录）：

    python test/e2e/auth_hook_e2e.py

退出码 0 表示通过，1 表示失败。
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

SDP = (
    "v=0\r\n"
    "o=- 0 0 IN IP4 127.0.0.1\r\n"
    "s=hook\r\n"
    "c=IN IP4 127.0.0.1\r\n"
    "t=0 0\r\n"
    "m=video 0 RTP/AVP 96\r\n"
    "a=rtpmap:96 H264/90000\r\n"
    "a=fmtp:96 packetization-mode=1\r\n"
    "a=control:streamid=0\r\n"
).encode()


class HookState:
    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0
        self.last = {}

    def record(self, req):
        with self.lock:
            self.count += 1
            self.last = req

    def snapshot(self):
        with self.lock:
            return self.count, dict(self.last)


STATE = HookState()


class HookHandler(BaseHTTPRequestHandler):
    # 不打印每个请求，避免刷屏；需要时用 /count、/last 查。
    def log_message(self, fmt, *args):
        pass

    def _json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        count, last = STATE.snapshot()
        if self.path == "/count":
            self._json(200, {"count": count})
        elif self.path == "/last":
            self._json(200, last)
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(n)
        try:
            req = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            req = {}
        STATE.record(req)

        if self.path == "/slow":
            time.sleep(1.0)
            self._json(200, {"allow": True})
            return
        # /policy：token=good 才放行，验证 hook 能看到 token/IP/流名。
        if req.get("token") == "good":
            self._json(200, {"allow": True})
        else:
            self._json(403, {"allow": False})


def start_hook():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), HookHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"server did not open port {port}")


def build_server(tmp):
    exe = os.path.join(tmp, "flow-authhook-e2e.exe")
    subprocess.run(
        ["go", "build", "-o", exe, "./cmd/server"],
        cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
    )
    return exe


def start_flow(exe, rtsp_port, rtmp_port, hook_url, timeout, default_policy="deny", log=None):
    cmd = [
        exe,
        "-rtsp-addr", f"127.0.0.1:{rtsp_port}",
        "-rtmp-addr", f"127.0.0.1:{rtmp_port}",
        "-httpflv-addr", "", "-hls-addr", "", "-dash-addr", "",
        "-srt-addr", "", "-webrtc-addr", "", "-snapshot-addr", "",
        "-admin-addr", "", "-metrics", "",
        "-auth-hook", hook_url,
        "-auth-hook-timeout", timeout,
        "-auth-hook-cache-ttl", "5s",
        "-auth-hook-default", default_policy,
    ]
    out = log or subprocess.DEVNULL
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT)
    try:
        wait_port(rtsp_port)
        wait_port(rtmp_port)
    except Exception:
        proc.kill()
        raise
    return proc


def stop_flow(proc):
    if proc.poll() is None:
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def rtsp_status(port, method, uri, body=b"", extra_headers=None):
    headers = dict(extra_headers or {})
    head = f"{method} {uri} RTSP/1.0\r\nCSeq: 1\r\n"
    for k, v in headers.items():
        head += f"{k}: {v}\r\n"
    if body:
        head += f"Content-Length: {len(body)}\r\n"
    head += "\r\n"
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        s.sendall(head.encode() + body)
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
    return buf.split(b"\r\n", 1)[0].decode("utf-8", "replace")


def http_get_json(port, path):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
        return json.load(resp)


def run_ffmpeg(args, timeout=15):
    try:
        return subprocess.run(
            args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return None


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def phase_policy(exe, rtsp, rtmp, hook_port):
    proc = start_flow(exe, rtsp, rtmp, f"http://127.0.0.1:{hook_port}/policy", "500ms")
    try:
        base = f"rtsp://127.0.0.1:{rtsp}/live/cam"

        status = rtsp_status(rtsp, "DESCRIBE", base, extra_headers={"Accept": "application/sdp"})
        check(status == "RTSP/1.0 403 Forbidden", f"DESCRIBE without token = {status}")

        status = rtsp_status(rtsp, "DESCRIBE", base + "?token=good",
                             extra_headers={"Accept": "application/sdp"})
        check(status == "RTSP/1.0 200 OK", f"DESCRIBE with token = {status}")

        last = http_get_json(hook_port, "/last")
        check(last.get("action") == "play", f"hook action = {last.get('action')}")
        check(last.get("stream") == "live/cam", f"hook stream = {last.get('stream')}")
        check(last.get("ip") == "127.0.0.1", f"hook ip = {last.get('ip')}")
        check(last.get("token") == "good", f"hook token = {last.get('token')}")

        status = rtsp_status(rtsp, "ANNOUNCE", base,
                             body=SDP, extra_headers={"Content-Type": "application/sdp"})
        check(status == "RTSP/1.0 403 Forbidden", f"ANNOUNCE without token = {status}")

        status = rtsp_status(rtsp, "ANNOUNCE", base + "?token=good",
                             body=SDP, extra_headers={"Content-Type": "application/sdp"})
        check(status == "RTSP/1.0 200 OK", f"ANNOUNCE with token = {status}")

        # 缓存：两次完全相同的 DESCRIBE 只应该产生一次 hook 调用。
        before = http_get_json(hook_port, "/count")["count"]
        for _ in range(2):
            status = rtsp_status(rtsp, "DESCRIBE", base + "?token=good",
                                 extra_headers={"Accept": "application/sdp"})
            check(status == "RTSP/1.0 200 OK", f"cached DESCRIBE = {status}")
        after = http_get_json(hook_port, "/count")["count"]
        check(after == before, f"cache not used: hook calls {before} -> {after}")

        # RTMP：错误 token 推流必须失败。
        bad = run_ffmpeg([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-re",
            "-i", "testdata/with_audio.mp4", "-c", "copy", "-f", "flv",
            f"rtmp://127.0.0.1:{rtmp}/live/push?token=bad",
        ], timeout=10)
        check(bad is not None and bad.returncode != 0, "RTMP publish with bad token succeeded")

        # 正确 token 推流：后台跑，确认没有被 hook 拒绝后，再拉流验证。
        publisher = subprocess.Popen([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-re",
            "-i", "testdata/with_audio.mp4", "-c", "copy", "-f", "flv",
            f"rtmp://127.0.0.1:{rtmp}/live/push?token=good",
        ], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            time.sleep(2.0)
            check(publisher.poll() is None, "allowed RTMP publish exited early")

            probe = run_ffmpeg([
                "ffprobe", "-v", "error", "-rw_timeout", "5000000",
                "-show_entries", "stream=codec_name", "-of", "csv=p=0",
                f"rtmp://127.0.0.1:{rtmp}/live/test?token=good",
            ], timeout=10)
            check(probe is not None and probe.returncode == 0, "RTMP play with good token failed")
            codecs = probe.stdout.decode().lower()
            check("h264" in codecs, f"RTMP play codecs = {codecs!r}")
        finally:
            publisher.kill()
            try:
                publisher.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

        probe_bad = run_ffmpeg([
            "ffprobe", "-v", "error", "-rw_timeout", "5000000",
            "-show_entries", "stream=codec_name", "-of", "csv=p=0",
            f"rtmp://127.0.0.1:{rtmp}/live/test?token=bad",
        ], timeout=10)
        check(probe_bad is None or probe_bad.returncode != 0,
              "RTMP play with bad token succeeded")
    finally:
        stop_flow(proc)


def phase_timeout(exe, hook_port):
    rtsp, rtmp = free_port(), free_port()
    proc = start_flow(exe, rtsp, rtmp, f"http://127.0.0.1:{hook_port}/slow", "200ms")
    try:
        start = time.time()
        status = rtsp_status(rtsp, "DESCRIBE", f"rtsp://127.0.0.1:{rtsp}/live/cam?token=good",
                             extra_headers={"Accept": "application/sdp"})
        elapsed = time.time() - start
        check(status == "RTSP/1.0 403 Forbidden", f"timeout DESCRIBE = {status}")
        check(elapsed < 0.9, f"timeout fallback took {elapsed:.2f}s, want < 0.9s")
    finally:
        stop_flow(proc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-server", action="store_true", help="print build dir")
    args = parser.parse_args()

    tmp = tempfile.mkdtemp(prefix="flow-authhook-e2e-")
    print(f"build dir: {tmp}")
    exe = build_server(tmp)
    print(f"server: {exe}")

    hook_srv, hook_port = start_hook()
    print(f"hook: http://127.0.0.1:{hook_port}/policy")
    rtsp, rtmp = free_port(), free_port()

    try:
        print("[1/2] policy allow/deny + cache + RTMP")
        phase_policy(exe, rtsp, rtmp, hook_port)
        print("[2/2] hook timeout -> default deny")
        phase_timeout(exe, hook_port)
    finally:
        hook_srv.shutdown()
        hook_srv.server_close()
        if not args.keep_server:
            try:
                os.remove(exe)
                os.rmdir(tmp)
            except OSError:
                pass

    print("PASS: auth hook e2e")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
