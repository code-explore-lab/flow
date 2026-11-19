#!/usr/bin/env python3
"""F-5 DASH 多码率 / ABR 端到端探针。

自己 build 并启动一个 flow 实例（RTSP + DASH + 两档转码），用 ffmpeg 把
testdata/with_audio.mp4 循环推到 RTSP，然后：

  * 校验 /dash/<source>/manifest.mpd 的 video AdaptationSet 有两个
    Representation，各自指向 /dash/<stream>/video/... 的绝对路径；
  * 音频 AdaptationSet 只有一个 Representation（取第一档）；
  * 各档下载 init + 前几段后 ffprobe 能解出对应分辨率；
  * headless Edge/Chrome 打开 dash.js 播放器，先固定最低档，再切到最高档，
    确认 currentTime 继续前进、无致命错误。

用法：
    python test/e2e/dash_abr_e2e.py
    python test/e2e/dash_abr_e2e.py --no-browser

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
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
E2E_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(E2E_DIR))

from dash_e2e import (  # noqa: E402  (复用 dash e2e 的 CDP / 浏览器探测)
    CDP,
    NS,
    free_port,
    free_static_server,
    http_get,
    launch_chrome,
    page_ws_url,
    pick_browser,
    stop_proc,
)


# ---------------------------------------------------------------- server

def build_server(tmpdir: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    bin_path = os.path.join(tmpdir, "flow_dash_abr_e2e" + suffix)
    subprocess.run(["go", "build", "-o", bin_path, "./cmd/server"], cwd=ROOT, check=True)
    return bin_path


def tail(path: str, n: int = 80) -> str:
    try:
        return "\n".join(Path(path).read_text(encoding="utf-8", errors="replace").splitlines()[-n:])
    except OSError:
        return ""


def start_server(args: argparse.Namespace, tmpdir: str):
    dash_port, rtsp_port = free_port(), free_port()
    bin_path = args.server_bin or build_server(tmpdir)
    log_path = os.path.join(tmpdir, "server.log")
    log = open(log_path, "w", encoding="utf-8")
    cmd = [
        bin_path,
        "-dash-addr", f"127.0.0.1:{dash_port}",
        "-dash-cors", "*",
        "-dash-update", "1s",
        "-hls-addr", "",
        "-hls-segment", "1s",
        "-hls-window", "8",
        "-rtsp-addr", f"127.0.0.1:{rtsp_port}",
        "-rtmp-addr", "",
        "-httpflv-addr", "",
        "-srt-addr", "",
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

    root = f"http://127.0.0.1:{dash_port}"
    deadline = time.time() + 25
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited early:\n" + tail(log_path))
        try:
            status, _ = http_get(root + "/dash/live/test/utc", timeout=1.5)
            if status == 200:
                return proc, push, root, log, log_path
        except (urllib.error.URLError, TimeoutError, OSError):
            pass
        time.sleep(0.25)
    raise RuntimeError("server did not become ready:\n" + tail(log_path))


def stop_pair(proc, push=None) -> None:
    stop_proc(push)
    stop_proc(proc)


# ---------------------------------------------------------------- MPD

def abs_url(root: str, uri: str) -> str:
    if uri.startswith("http://") or uri.startswith("https://"):
        return uri
    if uri.startswith("/"):
        return root + uri
    return root + "/dash/live/test/" + uri


def wait_abr_mpd(root: str, timeout: float) -> tuple[ET.Element, bytes]:
    url = root + "/dash/live/test/manifest.mpd"
    last = ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = http_get(url, timeout=3)
            last = f"status={status}"
            if status == 200:
                tree = ET.fromstring(body)
                video = tree.find("mpd:Period/mpd:AdaptationSet[@contentType='video']", NS)
                reps = video.findall("mpd:Representation", NS) if video is not None else []
                if len(reps) >= 2:
                    return tree, body
                last = f"{len(reps)} video Representation(s)"
        except (urllib.error.URLError, TimeoutError, OSError, ET.ParseError) as e:
            last = str(e)
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for ABR MPD ({last})")


def check_mpd(root: str, tree: ET.Element) -> list[dict]:
    video = tree.find("mpd:Period/mpd:AdaptationSet[@contentType='video']", NS)
    if video is None:
        raise AssertionError("no video AdaptationSet")
    reps = video.findall("mpd:Representation", NS)
    if len(reps) != 2:
        raise AssertionError(f"video Representations = {len(reps)}, want 2")
    seen = []
    for r in reps:
        st = r.find("mpd:SegmentTemplate", NS)
        seen.append((r.get("id"), r.get("width"), r.get("height"),
                     st.get("media") if st is not None else None))

    want = [
        ("/dash/live/test_240p/", "320x240"),
        ("/dash/live/test_480p/", "640x480"),
    ]
    out = []
    for i, (rep, (base, size)) in enumerate(zip(reps, want)):
        if rep.get("id") != f"video{i}":
            raise AssertionError(f"rep[{i}] id = {rep.get('id')!r}")
        if f"{rep.get('width')}x{rep.get('height')}" != size:
            raise AssertionError(
                f"rep[{i}] size = {rep.get('width')}x{rep.get('height')}, want {size}; "
                f"reps={seen}; codecs={[r.get('codecs') for r in reps]}")
        if int(rep.get("bandwidth", "0")) <= 0:
            raise AssertionError(f"rep[{i}] bandwidth = {rep.get('bandwidth')!r}")
        st = rep.find("mpd:SegmentTemplate", NS)
        if st is None:
            raise AssertionError(f"rep[{i}] has no SegmentTemplate")
        media = st.get("media", "")
        if media != base + "video/seg-$Number$.m4s":
            raise AssertionError(f"rep[{i}] media = {media!r}")
        init = st.get("initialization", "")
        if init != base + "video/init.mp4":
            raise AssertionError(f"rep[{i}] initialization = {init!r}")
        segs = st.findall("mpd:SegmentTimeline/mpd:S", NS)
        if not segs:
            raise AssertionError(f"rep[{i}] has an empty SegmentTimeline")
        out.append({
            "base": base,
            "media": media,
            "init": init,
            "start_number": int(st.get("startNumber", "1")),
            "segments": len(segs),
            "bandwidth": int(rep.get("bandwidth", "0")),
        })

    audio = tree.find("mpd:Period/mpd:AdaptationSet[@contentType='audio']", NS)
    if audio is None:
        raise AssertionError("no audio AdaptationSet")
    areps = audio.findall("mpd:Representation", NS)
    if len(areps) != 1:
        raise AssertionError(f"audio Representations = {len(areps)}, want 1")
    ast = areps[0].find("mpd:SegmentTemplate", NS)
    if ast is None or not ast.get("media", "").startswith("/dash/live/test_240p/audio/"):
        raise AssertionError(f"audio media = {ast.get('media') if ast is not None else None!r}")

    ast_avail = tree.get("availabilityStartTime", "")
    if not ast_avail.endswith("Z"):
        raise AssertionError(f"availabilityStartTime = {ast_avail!r}")
    return out


def download_rep(root: str, info: dict, outdir: str, count: int = 1) -> str:
    init_url = abs_url(root, info["init"])
    try:
        data = bytearray(http_get(init_url, timeout=10)[1])
    except urllib.error.HTTPError as e:
        raise AssertionError(f"GET {init_url} -> {e.code}") from e
    for n in range(info["start_number"], info["start_number"] + count):
        url = abs_url(root, info["media"].replace("$Number$", str(n)))
        try:
            data += http_get(url, timeout=10)[1]
        except urllib.error.HTTPError as e:
            raise AssertionError(f"GET {url} -> {e.code}") from e
    if len(data) < 1024:
        raise AssertionError(f"{info['base']}: downloaded only {len(data)} bytes")
    path = os.path.join(outdir, info["base"].strip("/").replace("/", "_") + ".mp4")
    Path(path).write_bytes(data)
    return path


def probe_video(path: str) -> dict:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
         "stream=codec_name,width,height", "-of", "json", path],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0:
        raise AssertionError(f"ffprobe {path} failed:\n{r.stderr[-2000:]}")
    streams = json.loads(r.stdout or "{}").get("streams", [])
    if not streams:
        raise AssertionError(f"ffprobe {path}: no video stream")
    return streams[0]


# ---------------------------------------------------------------- browser

def browser_switch(chrome: str, mpd_url: str, timeout: float) -> dict:
    static = free_static_server()
    profile = tempfile.mkdtemp(prefix="flow_dash_abr_chrome_")
    proc = cdp = None
    try:
        proc, port = launch_chrome(chrome, profile)
        cdp = CDP(page_ws_url(port))
        cdp.call("Page.enable")
        cdp.call("Runtime.enable")
        player_url = (f"http://127.0.0.1:{static.server_address[1]}/dash_player.html"
                      f"?src={urllib.parse.quote(mpd_url, safe='')}")
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
            if state and state.get("status") == "error":
                raise AssertionError(f"dash.js error: {state.get('error')}")
            if state and len(state.get("levels") or []) >= 2 and state.get("played", 0) > 2:
                break
        else:
            raise AssertionError(f"player never reached 2 levels / playback: {state}")

        levels = state["levels"]
        if len(levels) != 2:
            raise AssertionError(f"dash.js levels = {len(levels)}, want 2")

        cdp.evaluate("window.__flow.setLevel(0)")
        time.sleep(2.5)
        before = json.loads(cdp.evaluate("JSON.stringify(window.__flow.state())"))
        if before.get("currentLevel") != 0:
            raise AssertionError(f"failed to select level 0: {before}")
        t_before = float(before.get("currentTime") or 0)

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
    tmpdir = tempfile.mkdtemp(prefix="flow_dash_abr_e2e_")
    proc = push = log = None
    log_path = ""
    started = time.time()
    try:
        if not shutil.which("go"):
            raise RuntimeError("go not found in PATH")
        if not shutil.which("ffmpeg"):
            raise RuntimeError("ffmpeg not found in PATH")
        proc, push, root, log, log_path = start_server(args, tmpdir)

        tree, body = wait_abr_mpd(root, args.timeout)
        reps = check_mpd(root, tree)
        print(f"mpd: OK, {len(reps)} video Representations, "
              f"BANDWIDTH={[r['bandwidth'] for r in reps]}")

        outdir = os.path.join(tmpdir, "media")
        os.makedirs(outdir, exist_ok=True)
        sizes = []
        for info, want in zip(reps, ("320x240", "640x480")):
            path = download_rep(root, info, outdir, count=1)
            v = probe_video(path)
            if v.get("codec_name") != "h264":
                raise AssertionError(f"{info['base']}: codec = {v.get('codec_name')}")
            if f"{v.get('width')}x{v.get('height')}" != want:
                raise AssertionError(
                    f"{info['base']}: size = {v.get('width')}x{v.get('height')}, want {want}")
            sizes.append(f"{v.get('width')}x{v.get('height')}")
            print(f"{info['base']}: {info['segments']} segments, decode {sizes[-1]}")

        result = {
            "mpd_bytes": len(body),
            "representations": reps,
            "decoded": sizes,
            "elapsed_s": 0.0,
        }
        if not args.no_browser:
            chrome = pick_browser(args.chrome)
            switch = browser_switch(chrome, root + "/dash/live/test/manifest.mpd",
                                    args.browser_timeout)
            print(f"browser: levels={[l['bitrate'] for l in switch['levels']]}, "
                  f"level {switch['before_level']}->{switch['after_level']}, "
                  f"currentTime {switch['time_before']:.2f}s->{switch['time_after']:.2f}s")
            result["browser"] = switch
        result["elapsed_s"] = time.time() - started
        print("PASS")
        return result
    except Exception:
        if "root" in locals():
            try:
                diag = os.path.join(tmpdir, "diag")
                os.makedirs(diag, exist_ok=True)
                for base in ("live/test_240p", "live/test_480p"):
                    st = ET.fromstring(http_get(f"{root}/dash/live/test/manifest.mpd")[1])
                    video = st.find("mpd:Period/mpd:AdaptationSet[@contentType='video']", NS)
                    rep = None
                    for r in video.findall("mpd:Representation", NS):
                        seg = r.find("mpd:SegmentTemplate", NS)
                        if seg is not None and base + "/" in seg.get("media", ""):
                            rep = r
                    if rep is None:
                        continue
                    tpl = rep.find("mpd:SegmentTemplate", NS)
                    info = {
                        "base": base, "init": tpl.get("initialization"),
                        "media": tpl.get("media"),
                        "start_number": int(tpl.get("startNumber", "1")),
                    }
                    path = download_rep(root, info, diag, count=1)
                    print(f"diagnostic {base}: {probe_video(path)}", file=sys.stderr)
            except Exception as e2:
                print(f"diagnostic failed: {e2}", file=sys.stderr)
        if log_path:
            print("server log:", file=sys.stderr)
            print(tail(log_path, 400), file=sys.stderr)
        raise
    finally:
        if push is not None or proc is not None:
            stop_pair(proc, push)
        if log is not None:
            log.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--server-bin", default="", help="use a prebuilt flow binary")
    ap.add_argument("--timeout", type=float, default=60, help="seconds to wait for the ABR MPD")
    ap.add_argument("--browser-timeout", type=float, default=45, help="seconds for dash.js playback")
    ap.add_argument("--chrome", default="", help="Chrome/Edge binary path")
    ap.add_argument("--no-browser", action="store_true", help="skip the dash.js switching test")
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
