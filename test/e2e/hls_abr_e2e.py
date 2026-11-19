#!/usr/bin/env python3
"""F-5 HLS 多码率 / ABR 端到端探针。

默认自己 build 并启动一个 flow 实例（RTSP + HLS + 两档转码），用 ffmpeg
把 testdata/with_audio.mp4 循环推到 RTSP，然后：

  * 校验 /hls/<source>/master.m3u8 聚合了两档 variant，按 BANDWIDTH 升序；
  * 每个 variant 有自己的 EXT-X-MEDIA 音频组，URI 可访问；
  * 两档 media playlist 都有段，ffprobe/ffmpeg 能解出对应分辨率；
  * headless Edge/Chrome 打开 hls.js 播放器，先播最低档，再主动
    currentLevel 切到最高档，确认 currentTime 继续前进、无 fatal error。

用法：
    python test/e2e/hls_abr_e2e.py
    python test/e2e/hls_abr_e2e.py --no-browser
    python test/e2e/hls_abr_e2e.py --base http://127.0.0.1:8082/hls/live/test

退出码 0 表示通过。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
E2E_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(E2E_DIR))

from dash_e2e import (  # noqa: E402  (复用 dash e2e 的 CDP / 浏览器探测)
    CDP,
    free_static_server,
    launch_chrome,
    page_ws_url,
    pick_browser,
    stop_proc,
)


# ---------------------------------------------------------------- HTTP / m3u8

class ABRProbe:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def abs(self, uri: str) -> str:
        if uri.startswith("http://") or uri.startswith("https://"):
            return uri
        if uri.startswith("/"):
            root = urllib.parse.urlsplit(self.base)
            return f"{root.scheme}://{root.netloc}{uri}"
        return self.base + "/" + uri

    def get(self, uri: str, timeout: float = 10.0) -> tuple[int, bytes]:
        req = urllib.request.Request(self.abs(uri), headers={"User-Agent": "flow-abr-e2e"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()

    def text(self, uri: str, timeout: float = 10.0) -> str:
        status, body = self.get(uri, timeout)
        if status != 200:
            raise AssertionError(f"GET {uri} -> {status}")
        return body.decode("utf-8", "replace")


def parse_attrs(line: str) -> dict:
    out = {}
    for m in re.finditer(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)', line):
        out[m.group(1)] = m.group(2).strip('"')
    return out


def parse_master(text: str) -> tuple[list[dict], list[dict]]:
    media, variants, pending = [], [], None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-MEDIA:"):
            media.append(parse_attrs(line[len("#EXT-X-MEDIA:"):]))
        elif line.startswith("#EXT-X-STREAM-INF:"):
            pending = parse_attrs(line[len("#EXT-X-STREAM-INF:"):])
        elif not line.startswith("#") and pending is not None:
            pending["URI"] = line
            variants.append(pending)
            pending = None
    return variants, media


def segment_count(playlist: str) -> int:
    return playlist.count("#EXTINF:")


# ---------------------------------------------------------------- server

def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def build_server(tmpdir: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    bin_path = os.path.join(tmpdir, "flow_abr_e2e" + suffix)
    subprocess.run(["go", "build", "-o", bin_path, "./cmd/server"], cwd=ROOT, check=True)
    return bin_path


def tail(path: str, n: int = 80) -> str:
    try:
        return "\n".join(Path(path).read_text(encoding="utf-8", errors="replace").splitlines()[-n:])
    except OSError:
        return ""


def start_server(args: argparse.Namespace, tmpdir: str):
    hls_port, rtsp_port = free_port(), free_port()
    bin_path = args.server_bin or build_server(tmpdir)
    log_path = os.path.join(tmpdir, "server.log")
    log = open(log_path, "w", encoding="utf-8")
    cmd = [
        bin_path,
        "-hls-addr", f"127.0.0.1:{hls_port}",
        "-hls-segment", "1s",
        "-hls-part", "500ms",
        "-hls-window", "8",
        "-hls-cors", "*",
        "-rtsp-addr", f"127.0.0.1:{rtsp_port}",
        "-rtmp-addr", "",
        "-httpflv-addr", "",
        "-srt-addr", "",
        "-dash-addr", "",
        "-webrtc-addr", "",
        "-snapshot-addr", "",
        "-metrics", "",
        "-pprof", "",
        "-transcode",
        ("live/test=w=640,h=480,bitrate=1200k,out=live/test_480p;"
         "live/test=w=320,h=240,bitrate=400k,out=live/test_240p"),
        "-transcode-gop", "2s",
    ]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)

    push_log = open(os.path.join(tmpdir, "push.log"), "w", encoding="utf-8")
    push = subprocess.Popen([
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-re", "-stream_loop", "-1", "-i", str(ROOT / "testdata" / "with_audio.mp4"),
        "-c", "copy", "-f", "rtsp", "-rtsp_transport", "tcp",
        f"rtsp://127.0.0.1:{rtsp_port}/live/test",
    ], cwd=ROOT, stdout=push_log, stderr=subprocess.STDOUT)

    probe = ABRProbe(f"http://127.0.0.1:{hls_port}/hls/live/test")
    return proc, push, probe, log, log_path


def stop_pair(proc, push=None) -> None:
    stop_proc(push)
    stop_proc(proc)


# ---------------------------------------------------------------- checks

def wait_abr_master(probe: ABRProbe, timeout: float) -> tuple[list[dict], list[dict], str]:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            text = probe.text("master.m3u8", timeout=3)
            variants, media = parse_master(text)
            last = text
            if len(variants) >= 2:
                return variants, media, text
        except (urllib.error.URLError, TimeoutError, OSError, AssertionError) as e:
            last = str(e)
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for ABR master:\n{last}")


def check_master(variants: list[dict], media: list[dict], text: str) -> None:
    if len(variants) != 2:
        raise AssertionError(f"variants = {len(variants)}, want 2:\n{text}")
    bw = [int(v["BANDWIDTH"]) for v in variants]
    if bw != sorted(bw):
        raise AssertionError(f"BANDWIDTH not ascending: {bw}")
    expected = [
        ("/hls/live/test_240p/video.m3u8", "320x240"),
        ("/hls/live/test_480p/video.m3u8", "640x480"),
    ]
    for v, (uri, res) in zip(variants, expected):
        if v.get("URI") != uri:
            raise AssertionError(f"variant URI = {v.get('URI')}, want {uri}")
        if v.get("RESOLUTION") != res:
            raise AssertionError(f"variant RESOLUTION = {v.get('RESOLUTION')}, want {res}")
        if "avc1" not in v.get("CODECS", ""):
            raise AssertionError(f"variant CODECS = {v.get('CODECS')!r}")
        if not v.get("AUDIO"):
            raise AssertionError(f"variant missing AUDIO group: {v}")
    groups = {m.get("GROUP-ID") for m in media if m.get("TYPE") == "AUDIO"}
    if groups != {"aud0", "aud1"}:
        raise AssertionError(f"audio groups = {groups}, want aud0/aud1")
    for m in media:
        if not m.get("URI", "").endswith("audio.m3u8"):
            raise AssertionError(f"audio rendition URI = {m.get('URI')!r}")


def wait_playlist(probe: ABRProbe, uri: str, timeout: float, label: str) -> str:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            text = probe.text(uri, timeout=3)
            last = text
            if segment_count(text) > 0:
                return text
        except (urllib.error.URLError, TimeoutError, OSError, AssertionError) as e:
            last = str(e)
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {label} segments:\n{last}")


def ffprobe_video_url(url: str) -> dict:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
         "stream=codec_name,width,height", "-of", "json", url],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        raise AssertionError(f"ffprobe {url} failed:\n{r.stderr[-2000:]}")
    streams = json.loads(r.stdout or "{}").get("streams", [])
    if not streams:
        raise AssertionError(f"ffprobe {url}: no video stream")
    return streams[0]


def check_variant_decode(probe: ABRProbe, variant: dict, want: tuple[str, str]) -> dict:
    uri, res = want
    url = probe.abs(uri)
    info = ffprobe_video_url(url)
    if info.get("codec_name") != "h264":
        raise AssertionError(f"{uri}: codec = {info.get('codec_name')}")
    if f"{info.get('width')}x{info.get('height')}" != res:
        raise AssertionError(f"{uri}: size = {info.get('width')}x{info.get('height')}, want {res}")
    r = subprocess.run(
        ["ffmpeg", "-hide_banner", "-v", "error", "-i", url, "-t", "4", "-f", "null", "-"],
        capture_output=True, text=True, timeout=40,
    )
    if r.returncode != 0:
        raise AssertionError(f"ffmpeg decode {uri} failed:\n{r.stderr[-2000:]}")
    return info


# ---------------------------------------------------------------- browser

def browser_switch_abr(chrome: str, master_url: str, timeout: float) -> dict:
    static = free_static_server()
    profile = tempfile.mkdtemp(prefix="flow_abr_chrome_")
    proc = cdp = None
    try:
        proc, port = launch_chrome(chrome, profile)
        cdp = CDP(page_ws_url(port))
        cdp.call("Page.enable")
        cdp.call("Runtime.enable")
        player_url = (f"http://127.0.0.1:{static.server_address[1]}/hls_player.html"
                      f"?src={urllib.parse.quote(master_url, safe='')}")
        cdp.call("Page.navigate", {"url": player_url})

        deadline = time.time() + timeout
        state = None
        while time.time() < deadline:
            time.sleep(0.5)
            try:
                raw = cdp.evaluate(
                    "JSON.stringify(window.__flow ? window.__flow.state() : null)")
            except (RuntimeError, ConnectionError, OSError):
                continue
            state = json.loads(raw) if raw else None
            if state and len(state.get("levels") or []) >= 2 and state.get("currentTime", 0) > 1:
                break
        else:
            raise AssertionError(f"player never reached 2 levels / playback: {state}")

        levels = state["levels"]
        if len(levels) != 2:
            raise AssertionError(f"hls.js levels = {len(levels)}, want 2")
        # 固定从最低档开始，记录切换前的时间点。
        cdp.evaluate("window.__flow.setLevel(0)")
        time.sleep(2.5)
        before = json.loads(cdp.evaluate("JSON.stringify(window.__flow.state())"))
        if before.get("currentLevel") != 0:
            raise AssertionError(f"failed to select level 0: {before}")

        t_before = float(before.get("currentTime") or 0)
        # 切到最高档，确认播放继续而不是停在原地。
        cdp.evaluate(f"window.__flow.setLevel({len(levels) - 1})")
        time.sleep(4)
        after = json.loads(cdp.evaluate("JSON.stringify(window.__flow.state())"))
        if after.get("currentLevel") != len(levels) - 1:
            raise AssertionError(f"level switch did not take effect: {after}")
        t_after = float(after.get("currentTime") or 0)
        if t_after - t_before < 1.5:
            raise AssertionError(
                f"playback stalled across switch: {t_before:.2f}s -> {t_after:.2f}s ({after})")
        if after.get("status") == "error":
            raise AssertionError(f"player fatal error after switch: {after.get('error')}")

        return {
            "levels": [{k: l.get(k) for k in ("bitrate", "width", "height")} for l in levels],
            "before_level": before.get("currentLevel"),
            "after_level": after.get("currentLevel"),
            "time_before": t_before,
            "time_after": t_after,
        }
    finally:
        if cdp is not None:
            cdp.close()
        stop_proc(proc)
        shutil.rmtree(profile, ignore_errors=True)


# ---------------------------------------------------------------- main

def run(args: argparse.Namespace) -> dict:
    tmpdir = tempfile.mkdtemp(prefix="flow_abr_e2e_")
    proc = push = log = None
    log_path = ""
    started = time.time()
    try:
        if args.base:
            probe = ABRProbe(args.base)
        else:
            if not shutil.which("go"):
                raise RuntimeError("go not found in PATH")
            if not shutil.which("ffmpeg"):
                raise RuntimeError("ffmpeg not found in PATH")
            proc, push, probe, log, log_path = start_server(args, tmpdir)

        variants, media, text = wait_abr_master(probe, args.timeout)
        check_master(variants, media, text)
        print(f"master: OK, {len(variants)} variants, "
              f"BANDWIDTH={[int(v['BANDWIDTH']) for v in variants]}")

        checks = [
            (variants[0], ("320x240", "low")),
            (variants[1], ("640x480", "high")),
        ]
        for variant, (res, label) in checks:
            vpl = wait_playlist(probe, variant["URI"], args.timeout, f"{label} video")
            apl_uri = variant["URI"].rsplit("/", 1)[0] + "/audio.m3u8"
            apl = wait_playlist(probe, apl_uri, args.timeout, f"{label} audio")
            print(f"{label}: {segment_count(vpl)} video segments, {segment_count(apl)} audio segments")
        info_low = check_variant_decode(probe, variants[0], (variants[0]["URI"], "320x240"))
        info_high = check_variant_decode(probe, variants[1], (variants[1]["URI"], "640x480"))
        print(f"decode: low={info_low['width']}x{info_low['height']}, "
              f"high={info_high['width']}x{info_high['height']}")

        result = {
            "base": probe.base,
            "variants": [{"uri": v["URI"], "bandwidth": int(v["BANDWIDTH"]),
                          "resolution": v.get("RESOLUTION")} for v in variants],
            "elapsed_s": 0.0,
        }
        if not args.no_browser:
            chrome = pick_browser(args.chrome)
            master_url = probe.abs("master.m3u8")
            switch = browser_switch_abr(chrome, master_url, args.browser_timeout)
            print(f"browser: levels={[l['bitrate'] for l in switch['levels']]}, "
                  f"level {switch['before_level']}->{switch['after_level']}, "
                  f"currentTime {switch['time_before']:.2f}s->{switch['time_after']:.2f}s")
            result["browser"] = switch
        result["elapsed_s"] = time.time() - started
        print("PASS")
        return result
    except Exception:
        if log_path:
            print("server log:", file=sys.stderr)
            print(tail(log_path), file=sys.stderr)
        raise
    finally:
        if push is not None or proc is not None:
            stop_pair(proc, push)
        if log is not None:
            log.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default="", help="existing /hls/<source> base; skip build/start")
    ap.add_argument("--server-bin", default="", help="use a prebuilt flow binary")
    ap.add_argument("--timeout", type=float, default=45, help="seconds to wait for streams")
    ap.add_argument("--browser-timeout", type=float, default=40, help="seconds for browser playback")
    ap.add_argument("--chrome", default="", help="Chrome/Edge binary path")
    ap.add_argument("--no-browser", action="store_true", help="skip hls.js switching test")
    args = ap.parse_args()
    try:
        result = run(args)
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
