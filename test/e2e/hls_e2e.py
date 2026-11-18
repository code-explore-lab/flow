#!/usr/bin/env python3
"""P-2 HLS / LL-HLS 端到端探针。

默认会自己 build 并启动一个只开 HLS 的 flow 实例（其它协议绑到临时端口或关闭），
用 testdata 的本地回退源跑完整流程；也可以用 --base 指向已经运行的服务。

覆盖：
  * master / video / audio / init / segment / part 路由；
  * 窗口 >= 3 段、序列号单调、part <= PART-TARGET、85% 规则；
  * LL 的 EXT-X-PART / EXT-X-PRELOAD-HINT / _HLS_msn 阻塞重载；
  * edge_age、blocking reload、preload hint 延迟；
  * A/V 首段 PDT 偏差 <= 300ms；
  * 下载 init + 窗口内所有段，ffprobe / ffmpeg 解码。

用法：
    python test/e2e/hls_e2e.py --ll
    python test/e2e/hls_e2e.py --base http://127.0.0.1:8082/hls/live/test --ll
    python test/e2e/hls_e2e.py --part 0 --segment 1s   # 经典 HLS

退出码 0 表示通过。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASE = "http://127.0.0.1:8082/hls/live/test"


# ---------------------------------------------------------------- HTTP / m3u8

class HLSProbe:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def abs(self, uri: str) -> str:
        if uri.startswith("http://") or uri.startswith("https://"):
            return uri
        return self.base + "/" + uri.lstrip("/")

    def fetch(self, uri: str, timeout: float = 10.0) -> tuple[int, bytes, dict]:
        req = urllib.request.Request(self.abs(uri), headers={"User-Agent": "flow-hls-e2e"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)

    def text(self, uri: str, timeout: float = 10.0) -> str:
        status, body, _ = self.fetch(uri, timeout)
        if status != 200:
            raise RuntimeError(f"GET {uri} -> {status}")
        return body.decode("utf-8", "replace")

    def playlist(self, track: str) -> dict:
        return parse_playlist(self.text(track + ".m3u8"))


def parse_attrs(line: str) -> dict:
    out = {}
    for m in re.finditer(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)', line):
        out[m.group(1)] = m.group(2).strip('"')
    return out


def parse_pdt(v: str) -> dt.datetime:
    v = v.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    return dt.datetime.fromisoformat(v).astimezone(dt.timezone.utc)


def parse_playlist(text: str) -> dict:
    pl = {
        "version": 0,
        "target": 0,
        "part_target": 0.0,
        "media_seq": 0,
        "disc_seq": 0,
        "segments": [],
        "parts": [],
        "preload": "",
        "endlist": False,
        "server": {},
        "map": "",
    }
    current_map = ""
    current_pdt = None
    pending_dur = 0.0
    pending_disc = False
    part_start = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            if line.startswith("#EXT-X-VERSION:"):
                pl["version"] = int(line.split(":", 1)[1])
            elif line.startswith("#EXT-X-TARGETDURATION:"):
                pl["target"] = int(line.split(":", 1)[1])
            elif line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                pl["media_seq"] = int(line.split(":", 1)[1])
            elif line.startswith("#EXT-X-DISCONTINUITY-SEQUENCE:"):
                pl["disc_seq"] = int(line.split(":", 1)[1])
            elif line.startswith("#EXT-X-PART-INF:"):
                pl["part_target"] = float(parse_attrs(line)["PART-TARGET"])
            elif line.startswith("#EXT-X-SERVER-CONTROL:"):
                pl["server"] = parse_attrs(line)
            elif line.startswith("#EXT-X-MAP:"):
                current_map = parse_attrs(line)["URI"]
                pl["map"] = current_map
            elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
                current_pdt = parse_pdt(line.split(":", 1)[1])
            elif line == "#EXT-X-DISCONTINUITY":
                pending_disc = True
            elif line.startswith("#EXTINF:"):
                pending_dur = float(line.split(":", 1)[1].split(",", 1)[0])
            elif line.startswith("#EXT-X-PART:"):
                attrs = parse_attrs(line)
                dur = float(attrs["DURATION"])
                if part_start is None:
                    part_start = current_pdt
                pl["parts"].append({
                    "duration": dur,
                    "uri": attrs["URI"],
                    "independent": attrs.get("INDEPENDENT") == "YES",
                    "pdt": part_start,
                    "disc": pending_disc,
                })
                if part_start is not None:
                    part_start += dt.timedelta(seconds=dur)
                pending_disc = False
            elif line.startswith("#EXT-X-PRELOAD-HINT:"):
                pl["preload"] = parse_attrs(line).get("URI", "")
            elif line == "#EXT-X-ENDLIST":
                pl["endlist"] = True
            continue

        if pending_dur > 0:
            pl["segments"].append({
                "uri": line,
                "duration": pending_dur,
                "map": current_map,
                "pdt": current_pdt,
                "disc": pending_disc,
            })
            pending_dur = 0.0
            pending_disc = False
            current_pdt = None
            part_start = None
    return pl


def media_end(pl: dict) -> dt.datetime | None:
    if pl["parts"]:
        p = pl["parts"][-1]
        if p["pdt"] is not None:
            return p["pdt"] + dt.timedelta(seconds=p["duration"])
    if pl["segments"]:
        s = pl["segments"][-1]
        if s["pdt"] is not None:
            return s["pdt"] + dt.timedelta(seconds=s["duration"])
    return None


def first_start(pl: dict) -> dt.datetime | None:
    if pl["segments"] and pl["segments"][0]["pdt"] is not None:
        return pl["segments"][0]["pdt"]
    if pl["parts"] and pl["parts"][0]["pdt"] is not None:
        return pl["parts"][0]["pdt"]
    return None


def check_playlist(pl: dict, label: str, require_ll: bool) -> None:
    if pl["target"] < 1:
        raise AssertionError(f"{label}: bad target duration {pl['target']}")
    msns = [pl["media_seq"] + i for i in range(len(pl["segments"]))]
    if len(msns) >= 2 and msns != list(range(msns[0], msns[-1] + 1)):
        raise AssertionError(f"{label}: media sequence not monotonic: {msns}")
    for s in pl["segments"]:
        if s["duration"] <= 0 or round(s["duration"]) > pl["target"]:
            raise AssertionError(f"{label}: bad segment duration {s['duration']}")
        if not s["map"]:
            raise AssertionError(f"{label}: segment without EXT-X-MAP")
        if s["pdt"] is None:
            raise AssertionError(f"{label}: segment without PDT")
    if require_ll:
        if pl["part_target"] <= 0:
            raise AssertionError(f"{label}: classic playlist but LL expected")
        if not pl["preload"] and not pl["endlist"]:
            raise AssertionError(f"{label}: LL playlist without preload hint")
        for i, p in enumerate(pl["parts"]):
            if p["duration"] > pl["part_target"] + 1e-6:
                raise AssertionError(f"{label}: part {p['uri']} duration {p['duration']} > {pl['part_target']}")
            last = i == len(pl["parts"]) - 1
            if not p["independent"] and not last and p["duration"] + 1e-6 < pl["part_target"] * 0.85:
                raise AssertionError(f"{label}: part {p['uri']} below 85% rule")
        if pl["server"].get("CAN-BLOCK-RELOAD") != "YES":
            raise AssertionError(f"{label}: missing CAN-BLOCK-RELOAD")
    else:
        if pl["part_target"] != 0 or pl["parts"] or pl["preload"]:
            raise AssertionError(f"{label}: classic playlist has LL tags")
    if pl["endlist"] and pl["preload"]:
        raise AssertionError(f"{label}: ENDLIST with preload hint")


def check_master(text: str, require_audio: bool = True) -> None:
    if "#EXTM3U" not in text:
        raise AssertionError("master: missing #EXTM3U")
    media = [line for line in text.splitlines() if line.startswith("#EXT-X-MEDIA:")]
    variants = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF:") and i + 1 < len(lines):
            variants.append((parse_attrs(line), lines[i + 1].strip()))
    if not variants:
        raise AssertionError("master: no variants")
    if require_audio:
        aud = [parse_attrs(line) for line in media]
        if not any(a.get("TYPE") == "AUDIO" and a.get("GROUP-ID") == "aud" for a in aud):
            raise AssertionError("master: missing audio rendition group aud")
        if not any(v[0].get("AUDIO") == "aud" and v[1].startswith("video.m3u8") for v in variants):
            raise AssertionError("master: no video variant referencing aud")
        if not any(a.get("URI", "").startswith("audio.m3u8") for a in aud):
            raise AssertionError("master: audio rendition URI missing")
        codecs = next(v[0].get("CODECS", "") for v in variants if v[1].startswith("video.m3u8"))
        if "avc1" not in codecs or "mp4a" not in codecs:
            raise AssertionError(f"master: bad CODECS {codecs!r}")
        res = next(v[0].get("RESOLUTION", "") for v in variants if v[1].startswith("video.m3u8"))
        if res != "640x480":
            raise AssertionError(f"master: RESOLUTION {res!r}")


# ---------------------------------------------------------------- server

def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def build_server(tmpdir: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    bin_path = os.path.join(tmpdir, "flow_hls_e2e" + suffix)
    subprocess.run(["go", "build", "-o", bin_path, "./cmd/server"], cwd=ROOT, check=True)
    return bin_path


def start_server(args: argparse.Namespace, tmpdir: str):
    hls_port = free_port()
    rtsp_port = free_port()
    rtmp_port = free_port()
    bin_path = args.server_bin or build_server(tmpdir)
    log_path = os.path.join(tmpdir, "server.log")
    log = open(log_path, "w", encoding="utf-8")
    cmd = [
        bin_path,
        "-hls-addr", f"127.0.0.1:{hls_port}",
        "-hls-segment", args.segment,
        "-hls-part", args.part,
        "-hls-window", str(args.window),
        "-hls-idle", "30s",
        "-hls-max-bytes", "67108864",
        "-rtsp-addr", f"127.0.0.1:{rtsp_port}",
        "-rtmp-addr", f"127.0.0.1:{rtmp_port}",
        "-httpflv-addr", "",
        "-srt-addr", "",
        "-metrics", "",
        "-pprof", "",
    ]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{hls_port}/hls/live/test"
    probe = HLSProbe(base)
    deadline = time.time() + 25
    try:
        while time.time() < deadline:
            if proc.poll() is not None:
                log.flush()
                raise RuntimeError(f"server exited early:\n{tail(log_path)}")
            try:
                status, body, _ = probe.fetch("master.m3u8", timeout=1.5)
                if status == 200 and b"#EXTM3U" in body:
                    return proc, probe, log, log_path
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(0.25)
        log.flush()
        raise RuntimeError(f"server did not become ready:\n{tail(log_path)}")
    except Exception:
        stop_server(proc, log)
        raise


def tail(path: str, n: int = 60) -> str:
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-n:])
    except OSError:
        return ""


def stop_server(proc: subprocess.Popen, log) -> None:
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    try:
        log.close()
    except Exception:
        pass


# ---------------------------------------------------------------- checks

def wait_window(probe: HLSProbe, require_ll: bool, timeout: float) -> tuple[dict, dict]:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            v = probe.playlist("video")
        except Exception as e:
            raise RuntimeError(f"video playlist {probe.abs('video.m3u8')}: {e}") from e
        try:
            a = probe.playlist("audio")
        except Exception as e:
            raise RuntimeError(f"audio playlist {probe.abs('audio.m3u8')}: {e}") from e
        last = f"video={len(v['segments'])}/{len(v['parts'])} audio={len(a['segments'])}/{len(a['parts'])}"
        parts_ready = (not require_ll) or (len(v["parts"]) > 0 and len(a["parts"]) > 0)
        if len(v["segments"]) >= 3 and len(a["segments"]) >= 3 and parts_ready:
            check_playlist(v, "video", require_ll)
            check_playlist(a, "audio", require_ll)
            return v, a
        if v["endlist"] and len(v["segments"]) < 3:
            raise AssertionError(f"stream ended before 3 segments ({last})")
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for 3 segments ({last})")


def sample_edge_age(probe: HLSProbe, n: int) -> list[float]:
    ages = []
    for _ in range(n):
        pl = probe.playlist("video")
        end = media_end(pl)
        if end is not None:
            ages.append((dt.datetime.now(dt.timezone.utc) - end).total_seconds())
        time.sleep(0.15)
    if not ages:
        raise AssertionError("edge_age: no PDT/duration")
    return ages


def sample_blocking_reload(probe: HLSProbe, require_ll: bool, n: int) -> list[float]:
    out = []
    for _ in range(n):
        old = probe.playlist("video")
        open_msn = old["media_seq"] + len(old["segments"])
        if require_ll:
            part = len(old["parts"])
            url = f"video.m3u8?_HLS_msn={open_msn}&_HLS_part={part}"
        else:
            url = f"video.m3u8?_HLS_msn={open_msn}"
        t0 = time.monotonic()
        status, body, _ = probe.fetch(url, timeout=10)
        elapsed = time.monotonic() - t0
        if status != 200:
            raise AssertionError(f"blocking reload -> {status}")
        new = parse_playlist(body.decode("utf-8", "replace"))
        old_last = old["media_seq"] + len(old["segments"]) - 1
        new_last = new["media_seq"] + len(new["segments"]) - 1
        progressed = (
            new_last > old_last
            or len(new["parts"]) > len(old["parts"])
            or new["endlist"]
        )
        if not progressed:
            raise AssertionError("blocking reload returned without new media")
        out.append(elapsed)
        time.sleep(0.1)
    return out


def sample_preload(probe: HLSProbe, n: int) -> list[float]:
    out = []
    for _ in range(n):
        pl = probe.playlist("video")
        if not pl["preload"]:
            break
        t0 = time.monotonic()
        status, body, _ = probe.fetch(pl["preload"], timeout=10)
        elapsed = time.monotonic() - t0
        if status != 200:
            raise AssertionError(f"preload hint -> {status}")
        if b"moof" not in body:
            raise AssertionError("preload hint response missing moof")
        out.append(elapsed)
        time.sleep(0.1)
    return out


def download_track(probe: HLSProbe, track: str, outdir: str) -> str:
    pl = probe.playlist(track)
    if not pl["segments"]:
        raise AssertionError(f"{track}: no complete segments to download")
    init = next((s["map"] for s in pl["segments"] if s["map"]), pl["map"])
    if not init:
        raise AssertionError(f"{track}: missing init URI")
    blob = bytearray(probe.fetch(init)[1])
    for seg in pl["segments"]:
        blob += probe.fetch(seg["uri"])[1]
    path = os.path.join(outdir, track + ".mp4")
    Path(path).write_bytes(blob)
    return path


def ffprobe_streams(path: str) -> list[dict]:
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-show_entries",
         "stream=codec_name,width,height,sample_rate,channels", "-of", "json", path],
        text=True,
    )
    return json.loads(out).get("streams", [])


def ffmpeg_decode(path: str) -> None:
    r = subprocess.run(
        ["ffmpeg", "-hide_banner", "-v", "error", "-i", path, "-f", "null", "-"],
        capture_output=True, text=True, timeout=90,
    )
    if r.returncode != 0:
        raise AssertionError(f"ffmpeg decode {path} failed:\n{r.stderr[-2000:]}")


def check_decode(video_path: str, audio_path: str) -> None:
    vstreams = ffprobe_streams(video_path)
    astreams = ffprobe_streams(audio_path)
    vcodec = next((s.get("codec_name") for s in vstreams if s.get("codec_name")), "")
    acodec = next((s.get("codec_name") for s in astreams if s.get("codec_name")), "")
    if vcodec != "h264":
        raise AssertionError(f"video codec = {vcodec}")
    if acodec != "aac":
        raise AssertionError(f"audio codec = {acodec}")
    v = next(s for s in vstreams if s.get("codec_name") == "h264")
    a = next(s for s in astreams if s.get("codec_name") == "aac")
    if v.get("width") != 640 or v.get("height") != 480:
        raise AssertionError(f"video size = {v.get('width')}x{v.get('height')}")
    ffmpeg_decode(video_path)
    ffmpeg_decode(audio_path)


# ---------------------------------------------------------------- main

def run(args: argparse.Namespace) -> dict:
    tmpdir = tempfile.mkdtemp(prefix="flow_hls_e2e_")
    proc = None
    log = None
    log_path = ""
    started = time.time()
    try:
        if args.base:
            probe = HLSProbe(args.base)
        else:
            if not shutil.which("go"):
                raise RuntimeError("go not found in PATH")
            proc, probe, log, log_path = start_server(args, tmpdir)

        require_ll = args.ll or args.part not in ("0", "0s")
        if args.base:
            require_ll = args.ll

        master = probe.text("master.m3u8")
        check_master(master, require_audio=True)
        print(f"master: OK ({len(master.splitlines())} lines)")
        if args.verbose:
            try:
                status, stats, _ = probe.fetch("stats.json", timeout=3)
                print(f"stats: {status} {stats.decode('utf-8', 'replace')[:600]}")
            except Exception as e:
                print(f"stats: {e}")

        v, a = wait_window(probe, require_ll, args.seconds)
        if require_ll:
            if len(v["parts"]) == 0:
                raise AssertionError("LL playlist has no parts")
            if not v["preload"]:
                raise AssertionError("LL playlist has no preload hint")
        print(f"window: video={len(v['segments'])} segments/{len(v['parts'])} parts, "
              f"audio={len(a['segments'])}/{len(a['parts'])}")

        vstart, astart = first_start(v), first_start(a)
        if vstart is None or astart is None:
            raise AssertionError("missing first PDT")
        av_delta = abs((vstart - astart).total_seconds())
        if av_delta > 0.3:
            raise AssertionError(f"A/V first PDT delta = {av_delta:.3f}s > 0.3s")
        print(f"A/V first PDT delta: {av_delta * 1000:.1f} ms")

        ages = sample_edge_age(probe, args.samples)
        reloads = sample_blocking_reload(probe, require_ll, args.samples)
        hints = sample_preload(probe, args.samples) if require_ll else []
        print(f"edge_age p50: {statistics.median(ages) * 1000:.0f} ms (n={len(ages)})")
        print(f"blocking reload p50: {statistics.median(reloads) * 1000:.0f} ms (n={len(reloads)})")
        if hints:
            print(f"preload hint p50: {statistics.median(hints) * 1000:.0f} ms (n={len(hints)})")

        video_path = download_track(probe, "video", tmpdir)
        audio_path = download_track(probe, "audio", tmpdir)
        check_decode(video_path, audio_path)
        print("decode: h264 640x480 + aac OK (ffprobe/ffmpeg)")

        result = {
            "base": probe.base,
            "segment_target": args.segment,
            "part_target": args.part,
            "ll": require_ll,
            "segments_video": len(v["segments"]),
            "segments_audio": len(a["segments"]),
            "parts_video": len(v["parts"]),
            "av_start_delta_ms": av_delta * 1000,
            "edge_age_ms_p50": statistics.median(ages) * 1000,
            "blocking_reload_ms_p50": statistics.median(reloads) * 1000,
            "preload_hint_ms_p50": statistics.median(hints) * 1000 if hints else None,
            "elapsed_s": time.time() - started,
        }
        print("\nlatency table:")
        print(f"  scenario={'LL' if require_ll else 'classic'} segment={args.segment} part={args.part}")
        hint_text = f"{result['preload_hint_ms_p50']:.0f}ms" if result["preload_hint_ms_p50"] is not None else "n/a"
        print(f"  edge_age p50={result['edge_age_ms_p50']:.0f}ms "
              f"reload p50={result['blocking_reload_ms_p50']:.0f}ms "
              f"hint p50={hint_text}")
        print("PASS")
        return result
    except Exception:
        if log_path:
            print("server log:", file=sys.stderr)
            print(tail(log_path), file=sys.stderr)
        raise
    finally:
        if proc is not None:
            stop_server(proc, log)
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="P-2 HLS/LL-HLS e2e probe")
    ap.add_argument("--base", default="", help="existing HLS base, e.g. http://127.0.0.1:8082/hls/live/test")
    ap.add_argument("--server-bin", default="", help="flow binary; default builds ./cmd/server")
    ap.add_argument("--segment", default="1s", help="segment target for self-started server")
    ap.add_argument("--part", default="200ms", help="part target for self-started server; 0 = classic")
    ap.add_argument("--window", type=int, default=6, help="HLS window for self-started server")
    ap.add_argument("--seconds", type=float, default=15, help="how long to wait for 3 segments")
    ap.add_argument("--samples", type=int, default=5, help="latency samples")
    ap.add_argument("--ll", action="store_true", help="require LL-HLS tags")
    ap.add_argument("--verbose", action="store_true", help="print stats.json and extra diagnostics")
    ap.add_argument("--json", default="", help="write latency JSON to this path")
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

