#!/usr/bin/env python3
"""P-4 WebRTC（WHIP/WHEP）端到端探针。

默认自己 build 并启动一个只开 WebRTC 的 flow 实例，然后用 headless
Edge/Chrome 打开 test/e2e/webrtc_e2e.html：页面用 canvas 生成 H.264
推流（WHIP）并从同一路流回拉（WHEP），画面底部的黑白条码编码了绘制
时刻，拉流端逐帧解码后与本地时钟相减，得到真实的端到端延迟。

校验项：
  * WHIP POST 返回 201 + Location + ETag；WHEP 同理；
  * 浏览器协商出的编码是 H.264，WHIP/WHEP 双双 connected；
  * 解码帧数持续增长，端到端延迟 p50/p95 < --max-latency（默认 500ms）；
  * DELETE 两个会话均返回 204，Prometheus 会话计数回落。

用法：
    python test/e2e/webrtc_e2e.py
    python test/e2e/webrtc_e2e.py --base http://127.0.0.1:8084 --name live/demo
    python test/e2e/webrtc_e2e.py --no-audio --max-latency 300 --json out.json

退出码 0 表示通过。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

E2E_DIR = Path(__file__).resolve().parent
ROOT = E2E_DIR.parents[1]
sys.path.insert(0, str(E2E_DIR))

from dash_e2e import (  # noqa: E402  复用最小 CDP 客户端与静态服务器
    CDP,
    free_port,
    free_static_server,
    http_get,
    launch_chrome,
    page_ws_url,
    stop_proc,
    tail,
)

BROWSER_CANDIDATES = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
]


def browser_h264(chrome: str) -> bool:
    """确认浏览器既能编码也能解码 H.264（Chromium 无专有编解码器时不行）。"""
    profile = tempfile.mkdtemp(prefix="flow_webrtc_probe_")
    proc = cdp = None
    try:
        proc, port = launch_chrome(chrome, profile)
        cdp = CDP(page_ws_url(port))
        cdp.call("Runtime.enable")
        return bool(cdp.evaluate(
            "(() => { try { const has = c => /h264/i.test(c.mimeType);"
            " return RTCRtpSender.getCapabilities('video').codecs.some(has)"
            " && RTCRtpReceiver.getCapabilities('video').codecs.some(has); }"
            " catch { return false; } })()"))
    finally:
        if cdp is not None:
            cdp.close()
        stop_proc(proc)
        shutil.rmtree(profile, ignore_errors=True)


def pick_browser(explicit: str) -> str:
    if explicit:
        return explicit
    seen, candidates = set(), []
    for path in BROWSER_CANDIDATES + [
            shutil.which(n) for n in ("msedge", "microsoft-edge", "google-chrome", "chrome", "chromium")]:
        if path and os.path.exists(path) and path not in seen:
            seen.add(path)
            candidates.append(path)
    if not candidates:
        raise RuntimeError("Chrome/Edge not found; pass --chrome PATH")
    problems = []
    for path in candidates:
        try:
            if browser_h264(path):
                return path
            problems.append(f"{path}: no H.264 in WebRTC")
        except Exception as e:  # 浏览器起不来就换下一个
            problems.append(f"{path}: {e}")
    raise RuntimeError("no browser supports H.264 over WebRTC: " + "; ".join(problems))


def build_server(tmpdir: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    bin_path = os.path.join(tmpdir, "flow_webrtc_e2e" + suffix)
    subprocess.run(["go", "build", "-o", bin_path, "./cmd/server"], cwd=ROOT, check=True)
    return bin_path


def wait_http(url: str, timeout: float) -> int:
    """等 HTTP 端口起来；任何 HTTP 响应（含 404）都算就绪。"""
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "flow-webrtc-e2e"})
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last = str(e)
            time.sleep(0.2)
    raise RuntimeError(f"{url} never became ready: {last}")


def start_server(args: argparse.Namespace, tmpdir: str):
    webrtc_port = free_port()
    rtsp_port = free_port()
    rtmp_port = free_port()
    metrics_port = free_port()
    bin_path = args.server_bin or build_server(tmpdir)
    log_path = os.path.join(tmpdir, "server.log")
    log = open(log_path, "w", encoding="utf-8")
    cmd = [
        bin_path,
        "-webrtc-addr", f"127.0.0.1:{webrtc_port}",
        "-webrtc-cors", "*",
        "-webrtc-loopback",
        "-rtsp-addr", f"127.0.0.1:{rtsp_port}",
        "-rtmp-addr", f"127.0.0.1:{rtmp_port}",
        "-httpflv-addr", "",
        "-hls-addr", "",
        "-dash-addr", "",
        "-srt-addr", "",
        "-multicast-addr", "",
        "-metrics", f"127.0.0.1:{metrics_port}",
        "-pprof", "",
    ]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{webrtc_port}"
    try:
        # 未知流返回 404 说明 HTTP 路由已经可用。
        wait_http(base + "/whep/live/probe", timeout=20)
        if proc.poll() is not None:
            log.flush()
            raise RuntimeError(f"server exited early:\n{tail(log_path)}")
        return proc, base, metrics_port, log_path
    except Exception:
        stop_proc(proc)
        log.close()
        raise


def read_metrics(port: int) -> str:
    _, body = http_get(f"http://127.0.0.1:{port}/metrics", timeout=3)
    return body.decode("utf-8", "replace")


def metric_value(text: str, name: str, kind: str | None = None) -> float | None:
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith(name):
            continue
        if kind is not None:
            if not line.startswith(name + '{kind="' + kind + '"}'):
                continue
        try:
            return float(line.rsplit(" ", 1)[1])
        except (IndexError, ValueError):
            continue
    return None


def run_browser(chrome: str, page_url: str, args: argparse.Namespace) -> dict:
    profile = tempfile.mkdtemp(prefix="flow_webrtc_chrome_")
    proc = None
    cdp = None
    try:
        proc, port = launch_chrome(
            chrome, profile,
            extra_args=(
                # headless 页面也要保持 30fps 的 rAF，否则 canvas 时间码会停。
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--disable-backgrounding-occluded-windows",
                # 关掉 mDNS 候选混淆，本机 ICE 直连更稳。
                "--disable-features=WebRtcHideLocalIpsWithMdns",
            ),
        )
        cdp = CDP(page_ws_url(port))
        cdp.call("Page.enable")
        cdp.call("Runtime.enable")
        url = page_url + (
            f"&name={urllib.parse.quote(args.name, safe='')}"
            f"&audio={0 if args.no_audio else 1}"
        )
        cdp.call("Page.navigate", {"url": url})

        deadline = time.time() + args.timeout
        state = None
        last_err = ""
        while time.time() < deadline:
            time.sleep(0.5)
            try:
                raw = cdp.evaluate(
                    "JSON.stringify(window.__flow ? window.__flow.state() : {status:'boot'})")
            except (RuntimeError, ConnectionError, OSError) as e:
                last_err = str(e)
                continue
            if not raw:
                continue
            state = json.loads(raw)
            if state.get("status") == "error":
                raise AssertionError(f"page error: {state.get('error')}")
            play = state.get("play") or {}
            latency = play.get("latency") or {}
            if (play.get("connected")
                    and play.get("framesDecoded", 0) >= args.min_frames
                    and latency.get("count", 0) >= args.min_samples):
                break
        else:
            raise AssertionError(f"did not reach playing state: {state} ({last_err})")

        # 再观察一小段时间，让延迟样本稳定，然后重新取一次状态。
        time.sleep(args.settle)
        raw = cdp.evaluate("JSON.stringify(window.__flow.state())")
        state = json.loads(raw)

        play = state.get("play") or {}
        latency = play.get("latency") or {}
        push = state.get("push") or {}
        if "h264" not in (push.get("codec") or "").lower():
            raise AssertionError(f"WHIP did not negotiate H.264: {push.get('codec')!r}")
        if "h264" not in (play.get("codec") or "").lower():
            raise AssertionError(f"WHEP did not negotiate H.264: {play.get('codec')!r}")
        if not (play.get("width") or 0):
            raise AssertionError(f"no decoded frame size: {play}")
        p95 = latency.get("p95")
        if p95 is None:
            raise AssertionError(f"no latency samples: {latency}")
        if args.max_latency and p95 > args.max_latency:
            raise AssertionError(
                f"end-to-end latency p95={p95}ms exceeds {args.max_latency}ms "
                f"(p50={latency.get('p50')}, max={latency.get('max')})")

        deleted = cdp.evaluate("window.__flow.stop()")
        return {
            "push": {
                "state": push.get("state"),
                "codec": push.get("codec"),
                "rtt_ms": push.get("rttMs"),
            },
            "play": {
                "state": play.get("state"),
                "codec": play.get("codec"),
                "frames_decoded": play.get("framesDecoded"),
                "packets_lost": play.get("packetsLost"),
                "jitter_ms": play.get("jitterMs"),
                "jitter_buffer_ms": play.get("jitterBufferMs"),
                "rtt_ms": play.get("rttMs"),
                "video": f"{play.get('width')}x{play.get('height')}",
            },
            "latency_ms": {
                "samples": latency.get("count"),
                "p50": latency.get("p50"),
                "p95": latency.get("p95"),
                "max": latency.get("max"),
                "last": latency.get("last"),
            },
            "audio": state.get("audio"),
            "http": {
                "whip": state.get("http", {}).get("whip"),
                "whep": state.get("http", {}).get("whep"),
                "delete": deleted,
            },
        }
    finally:
        if cdp is not None:
            cdp.close()
        stop_proc(proc)
        shutil.rmtree(profile, ignore_errors=True)


def run(args: argparse.Namespace) -> dict:
    tmpdir = tempfile.mkdtemp(prefix="flow_webrtc_e2e_")
    server = None
    server_log = ""
    try:
        if args.base:
            base = args.base.rstrip("/")
            metrics_port = args.metrics_port
        else:
            server, base, metrics_port, server_log = start_server(args, tmpdir)

        chrome = pick_browser(args.chrome)
        print(f"browser: {chrome}")
        static = None
        try:
            # 页面与 flow 不同源，-webrtc-cors 已配置为 *。
            static = free_static_server()
            page_base = f"http://127.0.0.1:{static.server_address[1]}/webrtc_e2e.html"
            page_url = page_base + "?base=" + urllib.parse.quote(base, safe="")
            result = run_browser(chrome, page_url, args)
        finally:
            if static is not None:
                static.shutdown()

        http = result["http"]
        whip, whep = http.get("whip") or {}, http.get("whep") or {}
        if whip.get("status") != 201 or not whip.get("location") or not whip.get("etag"):
            raise AssertionError(f"WHIP response headers incomplete: {whip}")
        if whep.get("status") != 201 or not whep.get("location") or not whep.get("etag"):
            raise AssertionError(f"WHEP response headers incomplete: {whep}")
        deleted = http.get("delete") or {}
        if deleted.get("whip") != 204 or deleted.get("whep") != 204:
            raise AssertionError(f"DELETE did not return 204: {deleted}")

        result["server"] = {"base": base, "log": server_log}
        if metrics_port:
            deadline = time.time() + 5
            sessions = total_whip = total_whep = None
            while time.time() < deadline:
                text = read_metrics(metrics_port)
                sessions = metric_value(text, "flow_webrtc_sessions", "whip")
                total_whip = metric_value(text, "flow_webrtc_sessions_total", "whip")
                total_whep = metric_value(text, "flow_webrtc_sessions_total", "whep")
                if total_whip and total_whep and sessions == 0:
                    break
                time.sleep(0.2)
            result["metrics"] = {
                "sessions_whip": sessions,
                "sessions_total_whip": total_whip,
                "sessions_total_whep": total_whep,
            }
            if not total_whip or not total_whep:
                raise AssertionError(f"WebRTC metrics missing: {result['metrics']}")
            if sessions != 0:
                raise AssertionError(f"WHIP session gauge did not return to 0: {sessions}")
        return result
    finally:
        stop_proc(server)
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="P-4 WebRTC (WHIP/WHEP) e2e probe")
    ap.add_argument("--base", default="", help="existing flow base, e.g. http://127.0.0.1:8084")
    ap.add_argument("--metrics-port", type=int, default=0,
                    help="Prometheus port of an existing server (only with --base)")
    ap.add_argument("--server-bin", default="", help="flow binary; default builds ./cmd/server")
    ap.add_argument("--name", default="live/e2e", help="stream name used for WHIP and WHEP")
    ap.add_argument("--chrome", default="", help="Chrome/Edge binary path")
    ap.add_argument("--timeout", type=float, default=40, help="how long to wait for playback")
    ap.add_argument("--settle", type=float, default=3, help="media settle time before reading latency")
    ap.add_argument("--min-frames", type=int, default=60, help="decoded frames required")
    ap.add_argument("--min-samples", type=int, default=20, help="latency samples required")
    ap.add_argument("--max-latency", type=float, default=500,
                    help="p95 end-to-end latency budget in ms (0 disables)")
    ap.add_argument("--no-audio", action="store_true", help="push/pull video only")
    ap.add_argument("--json", default="", help="write result JSON to this path")
    args = ap.parse_args()

    try:
        result = run(args)
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 1

    lat = result["latency_ms"]
    print(f"WHIP  201 location={result['http']['whip']['location']}")
    print(f"WHEP  201 location={result['http']['whep']['location']}")
    print(f"codec {result['push']['codec']} / {result['play']['codec']}")
    print(f"video {result['play']['video']}, decoded {result['play']['frames_decoded']} frames")
    print(f"latency p50={lat['p50']}ms p95={lat['p95']}ms max={lat['max']}ms "
          f"({lat['samples']} samples)")
    print("PASS")
    if args.json:
        Path(args.json).write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"json: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
