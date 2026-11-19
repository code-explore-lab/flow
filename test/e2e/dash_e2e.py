#!/usr/bin/env python3
"""P-3 MPEG-DASH 端到端探针。

默认会自己 build 并启动一个只开 DASH 的 flow 实例，用 ffmpeg 把
testdata/with_audio.mp4 循环推到随机 RTSP 端口，然后：

  * 校验 dynamic MPD：profiles / availabilityStartTime / publishTime /
    UTCTiming / SegmentTemplate / SegmentTimeline / 音视频 Representation；
  * 连续采样 MPD，确认窗口随推流前进（segment 数、$Number$、时间线增长）；
  * 下载 init + 窗口内所有段，检查 ftyp/styp/moof 结构，ffprobe/ffmpeg 解码；
  * 用 headless Chrome（CDP）打开 dash.js 参考播放器，确认真正出画
    （currentTime 推进 > 2s，fragments > 0）；
  * ffmpeg 直接以 MPD 为输入解码 3s。

在没有 ffmpeg 时用 -local 走内置 testdata 文件源（有限时长，仅做结构校验）。
dash.js 从 CDN 加载；离线时把 dash.all.min.js 放到 test/e2e/ 同名目录即可。

用法：
    python test/e2e/dash_e2e.py
    python test/e2e/dash_e2e.py --base http://127.0.0.1:8083/dash/live/demo
    python test/e2e/dash_e2e.py --no-browser --seconds 15

退出码 0 表示通过。
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
E2E_DIR = Path(__file__).resolve().parent
NS = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}
CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]


# ---------------------------------------------------------------- HTTP / MPD

def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def http_get(url: str, timeout: float = 10.0, headers: dict | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": "flow-dash-e2e", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read()


class DASHProbe:
    def __init__(self, base: str):
        # base 指向 /dash/<name>，manifest 为 base/manifest.mpd
        self.base = base.rstrip("/")

    def abs(self, uri: str) -> str:
        if uri.startswith("http://") or uri.startswith("https://"):
            return uri
        return self.base + "/" + uri.lstrip("/")

    def manifest(self, timeout: float = 10.0) -> tuple[ET.Element, bytes]:
        status, body = http_get(self.abs("manifest.mpd"), timeout)
        if status != 200:
            raise RuntimeError(f"manifest -> {status}")
        try:
            return ET.fromstring(body), body
        except ET.ParseError as e:
            raise RuntimeError(f"manifest is not XML: {e}\n{body[:500]!r}") from e


def adaptation(root: ET.Element, kind: str) -> ET.Element:
    for aset in root.findall("mpd:Period/mpd:AdaptationSet", NS):
        if aset.get("contentType") == kind:
            return aset
    raise AssertionError(f"no {kind} AdaptationSet")


def representation(aset: ET.Element) -> ET.Element:
    reps = aset.findall("mpd:Representation", NS)
    if len(reps) != 1:
        raise AssertionError(f"{aset.get('contentType')}: {len(reps)} Representations")
    return reps[0]


def segment_template(aset: ET.Element) -> ET.Element:
    st = representation(aset).find("mpd:SegmentTemplate", NS)
    if st is None:
        st = aset.find("mpd:SegmentTemplate", NS)
    if st is None:
        raise AssertionError(f"{aset.get('contentType')}: no SegmentTemplate")
    return st


def timeline(st: ET.Element) -> list[ET.Element]:
    tl = st.find("mpd:SegmentTimeline", NS)
    if tl is None:
        raise AssertionError("SegmentTemplate has no SegmentTimeline")
    entries = tl.findall("mpd:S", NS)
    if not entries:
        raise AssertionError("empty SegmentTimeline")
    return entries


def segment_numbers(st: ET.Element) -> list[int]:
    start = int(st.get("startNumber") or 0)
    out = []
    n = start
    for s in timeline(st):
        repeat = int(s.get("r") or 0)
        for _ in range(repeat + 1):
            out.append(n)
            n += 1
    return out


def check_manifest_structure(root: ET.Element, segment: float) -> dict:
    if root.tag != "{urn:mpeg:dash:schema:mpd:2011}MPD":
        raise AssertionError(f"root tag = {root.tag}")
    if root.get("type") != "dynamic":
        raise AssertionError(f"type = {root.get('type')}")
    if root.get("profiles") != "urn:mpeg:dash:profile:isoff-live:2011":
        raise AssertionError(f"profiles = {root.get('profiles')}")
    ast = root.get("availabilityStartTime") or ""
    if not ast.endswith("Z"):
        raise AssertionError(f"availabilityStartTime = {ast!r}")
    # availabilityStartTime 应该在过去（除非刚启动、时钟抖动）。
    start = parse_iso(ast)
    now = time.time()
    if start - now > 120:
        raise AssertionError(f"availabilityStartTime {ast} is far in the future")
    pt = parse_iso(root.get("publishTime") or "")
    if abs(now - pt) > 300:
        raise AssertionError(f"publishTime {root.get('publishTime')} is stale")

    timing = root.find("mpd:UTCTiming", NS)
    if timing is None:
        raise AssertionError("no UTCTiming")
    if timing.get("schemeIdUri") != "urn:mpeg:dash:utc:http-iso:2014":
        raise AssertionError(f"UTCTiming scheme = {timing.get('schemeIdUri')}")

    periods = root.findall("mpd:Period", NS)
    if len(periods) != 1 or periods[0].get("start") != "PT0S":
        raise AssertionError(f"Periods = {[p.get('start') for p in periods]}")

    out = {"video": {}, "audio": {}}
    for kind in ("video", "audio"):
        aset = adaptation(root, kind)
        rep = representation(aset)
        st = segment_template(aset)
        ts = int(st.get("timescale") or 0)
        if ts <= 0:
            raise AssertionError(f"{kind}: timescale = {ts}")
        entries = timeline(st)
        durs = [int(s.get("d"), 0) for s in entries]
        if min(durs) <= 0:
            raise AssertionError(f"{kind}: bad segment durations {durs}")
        avg = sum(durs) / len(durs) / ts
        if abs(avg - segment) > segment:
            raise AssertionError(f"{kind}: avg segment {avg:.2f}s vs target {segment}s")
        init = st.get("initialization") or ""
        if not init or "init" not in init:
            raise AssertionError(f"{kind}: initialization = {init!r}")
        media = st.get("media") or ""
        if "$Number$" not in media:
            raise AssertionError(f"{kind}: media = {media!r}")
        if not rep.get("codecs"):
            raise AssertionError(f"{kind}: no codecs")
        if kind == "video" and not (int(rep.get("width") or 0) > 0):
            raise AssertionError("video: no width")
        if kind == "audio" and int(rep.get("audioSamplingRate") or 0) <= 0:
            raise AssertionError("audio: no audioSamplingRate")
        out[kind] = {
            "rep_id": rep.get("id"),
            "codecs": rep.get("codecs"),
            "timescale": ts,
            "init": init,
            "media": media,
            "start_number": int(st.get("startNumber") or 0),
            "segments": len(entries),
            "timeline": [(int(s.get("t")) if s.get("t") is not None else None, int(s.get("d")))
                         for s in entries],
        }
    return out


def parse_iso(v: str) -> float:
    v = v.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    import datetime as dt
    return dt.datetime.fromisoformat(v).timestamp()


def wait_manifest(probe: DASHProbe, seconds: float) -> ET.Element:
    deadline = time.time() + seconds
    last = ""
    while time.time() < deadline:
        try:
            root, _ = probe.manifest(timeout=5)
            entries = timeline(segment_template(adaptation(root, "video")))
            last = f"video segments={len(entries)}"
            if len(entries) >= 2:
                return root
        except (urllib.error.URLError, TimeoutError, OSError, AssertionError) as e:
            last = str(e)
        time.sleep(0.4)
    raise AssertionError(f"manifest never became ready ({last})")


def check_advancing(probe: DASHProbe, seconds: float) -> tuple[int, int]:
    first, _ = probe.manifest()
    before = segment_numbers(segment_template(adaptation(first, "video")))
    deadline = time.time() + seconds
    while time.time() < deadline:
        time.sleep(0.5)
        root, _ = probe.manifest()
        nums = segment_numbers(segment_template(adaptation(root, "video")))
        if nums[-1] > before[-1]:
            return before[-1], nums[-1]
    raise AssertionError(f"MPD window did not advance past segment {before[-1]}")


def download_track(probe: DASHProbe, kind: str, outdir: str) -> str:
    root, _ = probe.manifest()
    aset = adaptation(root, kind)
    st = segment_template(aset)
    numbers = segment_numbers(st)
    init = st.get("initialization")
    blob = bytearray(http_get(probe.abs(init))[1])
    for n in numbers:
        uri = st.get("media").replace("$Number$", str(n))
        blob += http_get(probe.abs(uri))[1]
    path = os.path.join(outdir, kind + ".mp4")
    Path(path).write_bytes(blob)
    return path


def check_mp4(path: str) -> None:
    data = Path(path).read_bytes()
    if len(data) < 16 or data[4:8] != b"ftyp":
        raise AssertionError(f"{path}: missing init ftyp")
    if b"moof" not in data or b"mdat" not in data:
        raise AssertionError(f"{path}: missing moof/mdat")


def ffprobe_streams(path: str) -> list[dict]:
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-show_entries",
         "stream=codec_name,width,height,sample_rate,channels", "-of", "json", path],
        text=True,
    )
    return json.loads(out).get("streams", [])


def check_decode(video_path: str, audio_path: str) -> None:
    vstreams = ffprobe_streams(video_path)
    astreams = ffprobe_streams(audio_path)
    v = next((s for s in vstreams if s.get("codec_name") == "h264"), None)
    a = next((s for s in astreams if s.get("codec_name") == "aac"), None)
    if v is None:
        raise AssertionError(f"video codec not h264: {vstreams}")
    if a is None:
        raise AssertionError(f"audio codec not aac: {astreams}")
    if (v.get("width"), v.get("height")) != (640, 480):
        raise AssertionError(f"video size = {v.get('width')}x{v.get('height')}")
    for p in (video_path, audio_path):
        r = subprocess.run(["ffmpeg", "-hide_banner", "-v", "error", "-i", p, "-f", "null", "-"],
                           capture_output=True, text=True, timeout=90)
        if r.returncode != 0:
            raise AssertionError(f"ffmpeg decode {p} failed:\n{r.stderr[-1500:]}")


def check_ffmpeg_mpd(probe: DASHProbe) -> None:
    r = subprocess.run(
        ["ffmpeg", "-hide_banner", "-v", "error", "-i", probe.abs("manifest.mpd"),
         "-t", "3", "-f", "null", "-"],
        capture_output=True, text=True, timeout=90,
    )
    if r.returncode != 0:
        raise AssertionError(f"ffmpeg MPD decode failed:\n{r.stderr[-1500:]}")


# ---------------------------------------------------------------- CDP (stdlib)

class WebSocket:
    """最小可用的客户端 WebSocket，只服务于 Chrome DevTools Protocol。"""

    def __init__(self, url: str, timeout: float = 20.0):
        u = urllib.parse.urlparse(url)
        self.sock = socket.create_connection((u.hostname, u.port or 80), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {u.path} HTTP/1.1\r\n"
            f"Host: {u.hostname}:{u.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(req.encode())
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("websocket handshake closed")
            head += chunk
        if b" 101 " not in head.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"websocket handshake failed: {head[:200]!r}")

    def _read(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise RuntimeError("websocket closed")
            buf += chunk
        return buf

    def send_text(self, text: str) -> None:
        payload = text.encode()
        mask = os.urandom(4)
        header = bytearray([0x81])
        if len(payload) < 126:
            header.append(0x80 | len(payload))
        elif len(payload) < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", len(payload))
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", len(payload))
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + masked)

    def recv_message(self) -> str:
        buffer = b""
        while True:
            h = self._read(2)
            opcode = h[0] & 0x0F
            masked = h[1] & 0x80
            length = h[1] & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._read(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._read(8))[0]
            mask = self._read(4) if masked else None
            data = self._read(length)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if opcode == 0x9:  # ping
                self.sock.sendall(b"\x8a\x80" + os.urandom(4))
                continue
            if opcode == 0x8:
                raise RuntimeError("websocket closed by peer")
            if opcode in (0x0, 0x1):
                buffer += data
                if h[0] & 0x80:
                    return buffer.decode("utf-8", "replace")

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


class CDP:
    def __init__(self, ws_url: str):
        self.ws = WebSocket(ws_url)
        self._id = 0

    def call(self, method: str, params: dict | None = None, timeout: float = 30.0) -> dict:
        self._id += 1
        msg_id = self._id
        self.ws.send_text(json.dumps({"id": msg_id, "method": method, "params": params or {}}))
        self.ws.sock.settimeout(timeout)
        while True:
            msg = json.loads(self.ws.recv_message())
            if msg.get("id") == msg_id:
                if "error" in msg:
                    raise RuntimeError(f"CDP {method}: {msg['error']}")
                return msg.get("result", {})

    def evaluate(self, expr: str, timeout: float = 30.0) -> object:
        r = self.call("Runtime.evaluate",
                      {"expression": expr, "returnByValue": True, "awaitPromise": True},
                      timeout)
        if "exceptionDetails" in r:
            raise RuntimeError(f"evaluate failed: {r['exceptionDetails']}")
        return r.get("result", {}).get("value")

    def close(self) -> None:
        self.ws.close()


def chrome_candidates(explicit: str = "") -> list[str]:
    if explicit:
        return [explicit]
    seen, out = set(), []
    names = ("google-chrome", "chrome", "chromium", "chromium-browser", "msedge")
    for path in [shutil.which(n) for n in names] + CHROME_CANDIDATES:
        if path and os.path.exists(path) and path not in seen:
            seen.add(path)
            out.append(path)
    return out


_BROWSER_PROBE: dict[str, tuple[str, bool]] = {}


def probe_browser(chrome: str) -> tuple[str, bool]:
    """启动一次 headless 浏览器，确认它支持 H.264 + AAC 的 MSE。

    某些 Chrome 构建（区域/企业版）不含专有编解码器，dash.js 会把整个
    AdaptationSet 过滤掉，表现为 "No streams to play"。
    """
    if chrome in _BROWSER_PROBE:
        return _BROWSER_PROBE[chrome]
    profile = tempfile.mkdtemp(prefix="flow_dash_probe_")
    proc = None
    cdp = None
    try:
        proc, port = launch_chrome(chrome, profile)
        cdp = CDP(page_ws_url(port))
        cdp.call("Runtime.enable")
        ua = cdp.evaluate("navigator.userAgent") or ""
        ok = bool(cdp.evaluate(
            "typeof MediaSource === 'function' && "
            "MediaSource.isTypeSupported('video/mp4; codecs=\"avc1.64001e\"') && "
            "MediaSource.isTypeSupported('audio/mp4; codecs=\"mp4a.40.2\"')"))
        _BROWSER_PROBE[chrome] = (ua, ok)
        return ua, ok
    finally:
        if cdp is not None:
            cdp.close()
        stop_proc(proc)
        shutil.rmtree(profile, ignore_errors=True)


def pick_browser(explicit: str) -> str:
    candidates = chrome_candidates(explicit)
    if not candidates:
        raise RuntimeError("Chrome/Edge not found; pass --chrome PATH or --no-browser")
    problems = []
    for path in candidates:
        try:
            ua, ok = probe_browser(path)
        except Exception as e:
            problems.append(f"{path}: {e}")
            continue
        cap = "H.264/AAC MSE OK" if ok else "no H.264/AAC MSE (codeless build)"
        print(f"browser: {ua.split(') ')[-1]} - {cap}")
        if ok:
            return path
        problems.append(f"{path}: {cap}")
    raise RuntimeError("no browser can play H.264/AAC MSE: " + "; ".join(problems))


class _PlayerHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(E2E_DIR), **kwargs)

    def log_message(self, *args):
        pass


def free_static_server() -> ThreadingHTTPServer:
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _PlayerHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def launch_chrome(chrome: str, profile: str,
                  extra_args: tuple[str, ...] = ()) -> tuple[subprocess.Popen, int]:
    port = free_port()
    proc = subprocess.Popen(
        [chrome, "--headless=new", "--disable-gpu", "--no-first-run",
         "--no-default-browser-check", "--mute-audio", "--disable-extensions",
         "--autoplay-policy=no-user-gesture-required", "--remote-allow-origins=*",
         *extra_args,
         f"--remote-debugging-port={port}", f"--user-data-dir={profile}", "about:blank"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 25
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("chrome exited during startup")
        try:
            _, body = http_get(f"http://127.0.0.1:{port}/json/version", timeout=1.5)
            if body:
                return proc, port
        except (urllib.error.URLError, TimeoutError, OSError):
            pass
        time.sleep(0.2)
    raise RuntimeError("chrome remote debugging port never opened")


def page_ws_url(port: int) -> str:
    deadline = time.time() + 10
    while time.time() < deadline:
        _, body = http_get(f"http://127.0.0.1:{port}/json/list", timeout=2)
        targets = json.loads(body)
        for t in targets:
            if t.get("type") == "page" and t.get("webSocketDebuggerUrl"):
                return t["webSocketDebuggerUrl"]
        time.sleep(0.2)
    raise RuntimeError("no page target in chrome")


def browser_playback(chrome: str, mpd_url: str, seconds: float) -> dict:
    static = free_static_server()
    profile = tempfile.mkdtemp(prefix="flow_dash_chrome_")
    proc = None
    cdp = None
    try:
        proc, port = launch_chrome(chrome, profile)
        cdp = CDP(page_ws_url(port))
        cdp.call("Page.enable")
        cdp.call("Runtime.enable")
        player_url = (f"http://127.0.0.1:{static.server_address[1]}/dash_player.html"
                      f"?src={urllib.parse.quote(mpd_url, safe='')}")
        cdp.call("Page.navigate", {"url": player_url})

        deadline = time.time() + seconds
        state = None
        last_err = ""
        while time.time() < deadline:
            time.sleep(0.5)
            try:
                raw = cdp.evaluate(
                    "JSON.stringify(window.__flow ? window.__flow.state() : "
                    "{status:'boot',error:'',played:0,fragments:0})")
            except (RuntimeError, ConnectionError, OSError) as e:
                last_err = str(e)
                continue
            if not raw:
                continue
            state = json.loads(raw)
            status = state.get("status")
            if status == "error":
                raise AssertionError(f"dash.js error: {state.get('error')}")
            if status == "playing" and state.get("played", 0) > 2 and state.get("fragments", 0) > 0:
                break
            if state.get("error"):
                last_err = state["error"]
        else:
            raise AssertionError(f"dash.js did not reach playing: {state} ({last_err})")

        if not (state.get("width", 0) > 0):
            raise AssertionError(f"no decoded video frames: {state}")
        return {
            "status": state["status"],
            "played_seconds": state["played"],
            "fragments": state["fragments"],
            "latency_seconds": state.get("latencySeconds"),
            "buffer_seconds": state.get("bufferSeconds"),
            "throughput_kbps": state.get("throughputKbps"),
            "video": f"{state['width']}x{state['height']}",
        }
    finally:
        if cdp is not None:
            cdp.close()
        if proc is not None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        static.shutdown()
        shutil.rmtree(profile, ignore_errors=True)


# ---------------------------------------------------------------- server / source

def build_server(tmpdir: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    bin_path = os.path.join(tmpdir, "flow_dash_e2e" + suffix)
    subprocess.run(["go", "build", "-o", bin_path, "./cmd/server"], cwd=ROOT, check=True)
    return bin_path


def tail(path: str, n: int = 40) -> str:
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-n:])
    except OSError:
        return ""


def start_server(args: argparse.Namespace, tmpdir: str):
    dash_port = free_port()
    rtsp_port = free_port()
    rtmp_port = free_port()
    bin_path = args.server_bin or build_server(tmpdir)
    log_path = os.path.join(tmpdir, "server.log")
    log = open(log_path, "w", encoding="utf-8")
    cmd = [
        bin_path,
        "-dash-addr", f"127.0.0.1:{dash_port}",
        "-dash-cors", "*",
        "-dash-update", "1s",
        "-hls-addr", "",
        "-hls-segment", args.segment,
        "-hls-part", "200ms",
        "-hls-window", str(args.window),
        "-hls-idle", "30s",
        "-rtsp-addr", f"127.0.0.1:{rtsp_port}",
        "-rtmp-addr", f"127.0.0.1:{rtmp_port}",
        "-httpflv-addr", "",
        "-srt-addr", "",
        "-metrics", "",
        "-pprof", "",
    ]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    name = "live/test" if args.local else "live/demo"
    probe = DASHProbe(f"http://127.0.0.1:{dash_port}/dash/{name}")
    try:
        deadline = time.time() + 25
        while time.time() < deadline:
            if proc.poll() is not None:
                log.flush()
                raise RuntimeError(f"server exited early:\n{tail(log_path)}")
            try:
                status, _ = http_get(probe.abs("utc"), timeout=1.5)
                if status == 200:
                    return proc, probe, rtsp_port, log, log_path
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(0.25)
        log.flush()
        raise RuntimeError(f"server did not become ready:\n{tail(log_path)}")
    except Exception:
        stop_proc(proc)
        log.close()
        raise


def start_push(rtsp_port: int, tmpdir: str) -> subprocess.Popen:
    log_path = os.path.join(tmpdir, "push.log")
    log = open(log_path, "w", encoding="utf-8")
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-re", "-stream_loop", "-1", "-i", "testdata/with_audio.mp4",
        "-c", "copy", "-f", "rtsp", "-rtsp_transport", "tcp",
        f"rtsp://127.0.0.1:{rtsp_port}/live/demo",
    ]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    proc._flow_log = log  # 便于退出时关闭
    proc._flow_log_path = log_path
    # 3s 内挂掉通常意味着端口/参数错误，提前失败更省时间。
    time.sleep(1.5)
    if proc.poll() is not None:
        log.flush()
        raise RuntimeError(f"ffmpeg push exited:\n{tail(log_path)}")
    return proc


def stop_proc(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    log = getattr(proc, "_flow_log", None)
    if log is not None:
        try:
            log.close()
        except Exception:
            pass


# ---------------------------------------------------------------- main

def run(args: argparse.Namespace) -> dict:
    tmpdir = tempfile.mkdtemp(prefix="flow_dash_e2e_")
    server = push = None
    started = time.time()
    try:
        if args.base:
            probe = DASHProbe(args.base)
        else:
            if not shutil.which("go"):
                raise RuntimeError("go not found in PATH")
            server, probe, rtsp_port, _, log_path = start_server(args, tmpdir)
            args._log_path = log_path
            if not args.local:
                if not shutil.which("ffmpeg"):
                    raise RuntimeError("ffmpeg not found; use --local to test with built-in testdata")
                push = start_push(rtsp_port, tmpdir)

        root = wait_manifest(probe, args.seconds)
        info = check_manifest_structure(root, parse_segment_seconds(args.segment))
        print(f"manifest: OK (video={info['video']['segments']} audio={info['audio']['segments']} segments)")

        _, body = probe.manifest()
        if b"manifest.mpd" in body:
            raise AssertionError("MPD unexpectedly references itself")
        utc_status, utc_body = http_get(probe.abs("utc"), timeout=5)
        if utc_status != 200 or not utc_body.strip().endswith(b"Z"):
            raise AssertionError(f"UTCTiming endpoint = {utc_status} {utc_body[:40]!r}")
        # UTCTiming 的 value 必须能拼出 /utc 相对路径。
        timing = root.find("mpd:UTCTiming", NS)
        if timing.get("value") != "utc":
            raise AssertionError(f"UTCTiming value = {timing.get('value')!r}")

        if not args.local:
            first, last = check_advancing(probe, args.advance)
            print(f"window advances: segment {first} -> {last}")

        video_path = download_track(probe, "video", tmpdir)
        audio_path = download_track(probe, "audio", tmpdir)
        check_mp4(video_path)
        check_mp4(audio_path)
        if shutil.which("ffprobe") and shutil.which("ffmpeg"):
            check_decode(video_path, audio_path)
            print("decode: h264 640x480 + aac OK (ffprobe/ffmpeg)")
        else:
            print("decode: skipped (ffprobe/ffmpeg not found)")

        result = {
            "base": probe.base,
            "segment_target": args.segment,
            "video": info["video"],
            "audio": info["audio"],
            "elapsed_s": time.time() - started,
        }

        if args.no_browser:
            print("dash.js: skipped (--no-browser)")
            result["browser"] = None
        else:
            chrome = pick_browser(args.chrome)
            playback = browser_playback(chrome, probe.abs("manifest.mpd"), args.browser_timeout)
            result["browser"] = playback
            print(f"dash.js: playing, played={playback['played_seconds']:.1f}s "
                  f"fragments={playback['fragments']} "
                  f"latency={fmt(playback['latency_seconds'])}s "
                  f"video={playback['video']}")

        if push is not None and shutil.which("ffmpeg"):
            check_ffmpeg_mpd(probe)
            print("ffmpeg MPD decode: 3s OK")

        print("PASS")
        return result
    except Exception:
        log_path = getattr(args, "_log_path", "")
        if log_path:
            print("server log:", file=sys.stderr)
            print(tail(log_path), file=sys.stderr)
        raise
    finally:
        stop_proc(push)
        stop_proc(server)
        shutil.rmtree(tmpdir, ignore_errors=True)


def fmt(v) -> str:
    return "—" if v is None else f"{v:.2f}"


def parse_segment_seconds(v: str) -> float:
    v = v.strip().lower()
    if v.endswith("ms"):
        return float(v[:-2]) / 1000
    if v.endswith("s"):
        return float(v[:-1])
    return float(v)


def main() -> int:
    ap = argparse.ArgumentParser(description="P-3 MPEG-DASH e2e probe (incl. dash.js)")
    ap.add_argument("--base", default="", help="existing DASH base, e.g. http://127.0.0.1:8083/dash/live/demo")
    ap.add_argument("--server-bin", default="", help="flow binary; default builds ./cmd/server")
    ap.add_argument("--segment", default="1s", help="segment target for self-started server")
    ap.add_argument("--window", type=int, default=8, help="segment window for self-started server")
    ap.add_argument("--seconds", type=float, default=25, help="how long to wait for a ready manifest")
    ap.add_argument("--advance", type=float, default=10, help="how long to wait for the window to advance")
    ap.add_argument("--local", action="store_true", help="use built-in testdata source instead of ffmpeg RTSP push")
    ap.add_argument("--no-browser", action="store_true", help="skip the dash.js headless Chrome check")
    ap.add_argument("--chrome", default="", help="Chrome/Edge binary path")
    ap.add_argument("--browser-timeout", type=float, default=60, help="how long dash.js may take to play")
    ap.add_argument("--json", default="", help="write result JSON to this path")
    args = ap.parse_args()

    try:
        result = run(args)
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 1
    if args.json:
        Path(args.json).write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"json: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
