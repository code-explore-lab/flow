#!/usr/bin/env python3
"""T-10：RTMP 互通矩阵端到端自动化。

把 rtmp.md 里的手工验收固化成一条命令：

  1. 本地文件回放：ffmpeg 拉 rtmp://.../live/test，断言解码帧数与音视频轨；
  2. RTMP 推 -> RTSP 拉；
  3. RTSP 推 -> RTMP 拉；
  4. RTMP 推 -> RTMP 拉；
  5. 断开回收：推/拉都断开后，服务器日志出现 unpublished，且
     /metrics 里的连接/推流/拉流/会话 gauge 全部回零。

脚本自己编译并启动 cmd/server（可用 --server-bin 指定现成二进制），
自动挑选空闲端口，结束后清理进程。依赖 PATH 里的 ffmpeg，Python 3.7+。

用法：

    python test/e2e/rtmp_matrix.py
    python test/e2e/rtmp_matrix.py --case rtmp_to_rtsp   # 只跑一项
    python test/e2e/rtmp_matrix.py --keep                # 保留临时目录和日志

退出码：0 通过，1 有用例失败，2 环境/启动错误。
"""

import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TESTDATA = ROOT / "testdata"
MEDIA = TESTDATA / "with_audio.mp4"
FPS = 25  # with_audio.mp4 / test.h264 都是 25fps

# 单个推流进程的存活时长：够起播 + 拉 2 秒，又不至于拖长矩阵。
PUSH_SECONDS = 8
PULL_SECONDS = 2.0

STREAM_RE = re.compile(
    r"Stream #\d+:\d+(?:\[[^\]]*\])?(?:\([^)]*\))?: (Video|Audio): ([A-Za-z0-9_]+)"
)
FRAME_RE = re.compile(r"^frame=(\d+)\s*$")
# T-6：DTS/CTS 处理正确时，ffmpeg 拉流解码不应报时间戳倒退。
BAD_TIMESTAMP_RE = re.compile(r"non monotonically increasing dts")


class SetupError(Exception):
    """环境不满足（缺 ffmpeg、编译失败、端口起不来）。"""


class CaseFailure(Exception):
    """单个用例断言失败。"""


class ProcLog:
    """后台读取子进程合并后的输出，保留行缓冲并支持等待日志。"""

    def __init__(self, name, proc, echo=False):
        self.name = name
        self.proc = proc
        self.echo = echo
        self.lines = []
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self):
        try:
            for line in self.proc.stdout:
                line = line.rstrip("\r\n")
                with self._lock:
                    self.lines.append(line)
                if self.echo:
                    print(f"    [{self.name}] {line}")
        except (OSError, ValueError):
            pass

    def wait_for(self, pattern, timeout):
        """等待某一行匹配 pattern（字符串包含或正则），返回该行或 None。"""
        if isinstance(pattern, str):
            match = lambda line: pattern in line
        else:
            match = lambda line: pattern.search(line) is not None
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                for line in self.lines:
                    if match(line):
                        return line
            if self.proc.poll() is not None:
                # 进程已经退出，再给读线程一点时间把缓冲刷完。
                time.sleep(0.05)
                with self._lock:
                    for line in self.lines:
                        if match(line):
                            return line
                return None
            time.sleep(0.02)
        return None

    def tail(self, n=15):
        with self._lock:
            return self.lines[-n:]


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def free_ports(n):
    """挑 n 个互不相同的空闲端口（连续 free_port 可能撞到同一个）。"""
    ports = set()
    while len(ports) < n:
        ports.add(free_port())
    return list(ports)


def wait_port(port, timeout, proc=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def stop_process(proc, timeout=5):
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except OSError:
        pass
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            pass


def start_process(args, name, cwd=None):
    proc = subprocess.Popen(
        args,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    return proc, ProcLog(name, proc)


class Context:
    def __init__(self, ffmpeg, server):
        self.ffmpeg = ffmpeg
        self.server = server

    # ---- 服务器侧 -------------------------------------------------

    def rtmp_url(self, name):
        return f"rtmp://127.0.0.1:{self.server.rtmp_port}/live/{name}"

    def rtsp_url(self, name):
        return f"rtsp://127.0.0.1:{self.server.rtsp_port}/{name}"

    def wait_publish(self, proto, name, timeout=8):
        pattern = (
            f'rtmp: publish "{name}"'
            if proto == "rtmp"
            else f'published stream "{name}"'
        )
        line = self.server.log.wait_for(pattern, timeout)
        if line is None:
            raise CaseFailure(
                f"服务器没有在 {timeout}s 内记录 {proto} 发布日志 {pattern!r}"
            )
        return line

    def scrape_metrics(self):
        url = f"http://127.0.0.1:{self.server.metrics_port}/metrics"
        try:
            body = urllib.request.urlopen(url, timeout=2).read().decode("utf-8", "replace")
        except OSError as err:
            raise CaseFailure(f"抓取 /metrics 失败: {err}")
        values = {}
        for line in body.splitlines():
            if not line or line.startswith("#"):
                continue
            key, _, value = line.rpartition(" ")
            try:
                values[key.strip()] = float(value)
            except ValueError:
                continue
        return values

    def wait_idle(self, timeout=8, need_unpublished=None):
        """等所有活跃 gauge 回零（断开回收的判据）。"""
        keys = [
            'flow_connections{proto="rtmp"}',
            'flow_connections{proto="rtsp"}',
            'flow_rtmp_publishes',
            'flow_rtmp_plays',
            'flow_sessions{proto="rtsp"}',
        ]
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            last = self.scrape_metrics()
            dirty = {k: last.get(k) for k in keys if last.get(k, 0) != 0}
            if not dirty:
                return last
            time.sleep(0.1)
        raise CaseFailure(f"{timeout}s 后仍有活跃指标未回零: {last}")

    # ---- ffmpeg ---------------------------------------------------

    def pull(self, url, seconds=PULL_SECONDS):
        """拉流解码到 null，返回 {rc, frames, tracks, log}。"""
        args = [
            self.ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "info",
            "-progress", "pipe:2", "-nostats",
        ]
        if url.startswith("rtsp://"):
            args += ["-rtsp_transport", "tcp"]
        args += [
            "-i", url, "-t", str(seconds),
            "-map", "0:v?", "-map", "0:a?",
            "-f", "null", "-",
        ]
        proc, log = start_process(args, "pull")
        try:
            rc = proc.wait(timeout=seconds + 15)
        except subprocess.TimeoutExpired:
            stop_process(proc)
            raise CaseFailure(
                f"拉流 {url} 超过 {seconds + 15}s 未退出\n{self._tail(log)}"
            )
        frames = 0
        tracks = set()
        codecs = {}
        for line in list(log.lines):
            m = FRAME_RE.match(line.strip())
            if m:
                frames = max(frames, int(m.group(1)))
                continue
            m = STREAM_RE.search(line)
            if m:
                tracks.add(m.group(1))
                # 输入流行在输出流行之前：只记第一次见到的编码，
                # 避免把 -f null 的输出编码 wrapped_avframe 当成源编码。
                codecs.setdefault(m.group(1), m.group(2))
        return {"rc": rc, "frames": frames, "tracks": tracks,
                "codecs": codecs, "log": log}

    def start_push(self, proto, name, seconds=PUSH_SECONDS):
        if proto == "rtmp":
            target = ["-f", "flv", self.rtmp_url(name)]
        else:
            target = ["-rtsp_transport", "tcp", "-f", "rtsp", self.rtsp_url(name)]
        args = [
            self.ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "error",
            "-re", "-stream_loop", "-1", "-i", str(MEDIA),
            "-c", "copy", "-t", str(seconds),
        ] + target
        proc, log = start_process(args, f"push-{proto}")
        return proc, log

    @staticmethod
    def _tail(log, n=15):
        lines = log.tail(n)
        return "\n".join("    " + line for line in lines)


def check_pull(result, label, min_frames, need_audio=True):
    if result["rc"] != 0:
        raise CaseFailure(
            f"{label}: ffmpeg 退出码 {result['rc']}\n{Context._tail(result['log'])}"
        )
    if "Video" not in result["tracks"]:
        raise CaseFailure(f"{label}: 没有视频轨 (tracks={sorted(result['tracks'])})")
    if need_audio and "Audio" not in result["tracks"]:
        raise CaseFailure(f"{label}: 没有音频轨 (tracks={sorted(result['tracks'])})")
    if result["frames"] < min_frames:
        raise CaseFailure(
            f"{label}: 只解出 {result['frames']} 帧，期望 >= {min_frames}\n"
            f"{Context._tail(result['log'])}"
        )
    bad = [line for line in result["log"].lines if BAD_TIMESTAMP_RE.search(line)]
    if bad:
        raise CaseFailure(
            f"{label}: ffmpeg 报时间戳倒退（DTS/CTS 处理有误）\n"
            + "\n".join("    " + line for line in bad[:5])
        )


def describe(result):
    tracks = "+".join(sorted(result["tracks"])) or "none"
    codecs = ",".join(f"{k}:{v}" for k, v in sorted(result["codecs"].items()))
    return f"frames={result['frames']} tracks={tracks} codecs={codecs}"


# ---- 用例 ---------------------------------------------------------

def case_local_playback(ctx):
    result = ctx.pull(ctx.rtmp_url("test"))
    check_pull(result, "本地回放", min_frames=int(FPS * PULL_SECONDS * 0.5))
    return describe(result)


def run_cross(ctx, push_proto, pull_proto, name):
    push, plog = ctx.start_push(push_proto, name)
    try:
        ctx.wait_publish(push_proto, name)
        if push.poll() is not None:
            raise CaseFailure(
                f"推流进程提前退出，rc={push.returncode}\n{ctx._tail(plog)}"
            )
        result = ctx.pull(
            ctx.rtmp_url(name) if pull_proto == "rtmp" else ctx.rtsp_url(name)
        )
        check_pull(
            result, f"{push_proto.upper() if push_proto == 'rtmp' else 'RTSP'} 推 -> "
                    f"{pull_proto.upper() if pull_proto == 'rtmp' else 'RTSP'} 拉",
            min_frames=int(FPS * PULL_SECONDS * 0.4),
        )
        return describe(result)
    finally:
        stop_process(push)
        ctx.wait_idle()


def case_rtmp_to_rtsp(ctx):
    return run_cross(ctx, "rtmp", "rtsp", "t10-rtmp2rtsp")


def case_rtsp_to_rtmp(ctx):
    return run_cross(ctx, "rtsp", "rtmp", "t10-rtsp2rtmp")


def case_rtmp_to_rtmp(ctx):
    return run_cross(ctx, "rtmp", "rtmp", "t10-rtmp2rtmp")


def case_disconnect_reclaim(ctx):
    name = "t10-reclaim"
    push, plog = ctx.start_push("rtmp", name)
    try:
        ctx.wait_publish("rtmp", name)
        result = ctx.pull(ctx.rtmp_url(name), seconds=1.0)
        check_pull(result, "断开回收-拉流", min_frames=int(FPS * 1.0 * 0.3))
    finally:
        # 直接终止推流端：模拟客户端掉线，而不是走 FCUnpublish。
        stop_process(push)

    line = ctx.server.log.wait_for(f'rtmp: unpublished "{name}"', 8)
    if line is None:
        raise CaseFailure(
            f"推流断开后 8s 内没有 unpublished 日志\n"
            f"{ctx._tail(plog)}\n服务器日志:\n{ctx._tail(ctx.server.log)}"
        )
    ctx.wait_idle()
    return f"unpublished + gauges=0 ({describe(result)})"


CASES = [
    ("local_playback", "本地文件回放", case_local_playback),
    ("rtmp_to_rtsp", "RTMP 推 -> RTSP 拉", case_rtmp_to_rtsp),
    ("rtsp_to_rtmp", "RTSP 推 -> RTMP 拉", case_rtsp_to_rtmp),
    ("rtmp_to_rtmp", "RTMP 推 -> RTMP 拉", case_rtmp_to_rtmp),
    ("disconnect_reclaim", "断开回收", case_disconnect_reclaim),
]


class Server:
    def __init__(self, binary, workdir, echo=False):
        self.binary = binary
        self.workdir = workdir
        self.echo = echo
        self.rtmp_port, self.rtsp_port, self.metrics_port = free_ports(3)
        self.proc = None
        self.log = None

    def start(self):
        args = [
            str(self.binary),
            "-rtmp-addr", f"127.0.0.1:{self.rtmp_port}",
            "-rtsp-addr", f"127.0.0.1:{self.rtsp_port}",
            "-metrics", f"127.0.0.1:{self.metrics_port}",
        ]
        self.proc, self.log = start_process(args, "server", cwd=self.workdir)
        if not wait_port(self.rtmp_port, 10, self.proc):
            raise SetupError(
                "服务器 RTMP 端口没有就绪\n" + Context._tail(self.log)
            )
        if not wait_port(self.rtsp_port, 10, self.proc):
            raise SetupError(
                "服务器 RTSP 端口没有就绪\n" + Context._tail(self.log)
            )
        if not wait_port(self.metrics_port, 10, self.proc):
            raise SetupError(
                "服务器 metrics 端口没有就绪\n" + Context._tail(self.log)
            )

    def stop(self):
        stop_process(self.proc)


def build_server(workdir):
    exe = "flow-server.exe" if os.name == "nt" else "flow-server"
    out = Path(workdir) / exe
    print(f"==> 编译服务器: go build -o {out} ./cmd/server")
    result = subprocess.run(
        ["go", "build", "-o", str(out), "./cmd/server"],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        raise SetupError("编译服务器失败:\n" + result.stdout)
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description="T-10 RTMP e2e matrix")
    parser.add_argument("--ffmpeg", default="ffmpeg", help="ffmpeg 可执行文件")
    parser.add_argument("--server-bin", default="", help="跳过编译，使用现成服务器二进制")
    parser.add_argument("--case", action="append", choices=[c[0] for c in CASES],
                        help="只跑指定用例，可重复")
    parser.add_argument("--keep", action="store_true", help="保留临时目录与日志")
    parser.add_argument("--verbose", action="store_true", help="实时打印服务器与 ffmpeg 日志")
    args = parser.parse_args(argv)

    # Windows 控制台默认可能不是 UTF-8，统一输出编码避免中文乱码。
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    ffmpeg = shutil.which(args.ffmpeg)
    if not ffmpeg:
        print(f"错误: PATH 里找不到 ffmpeg ({args.ffmpeg})", file=sys.stderr)
        return 2
    if not MEDIA.is_file():
        print(f"错误: 缺少测试媒体 {MEDIA}", file=sys.stderr)
        return 2

    workdir = tempfile.mkdtemp(prefix="flow-e2e-")
    server = None
    failures = []
    try:
        binary = Path(args.server_bin) if args.server_bin else build_server(workdir)
        if not Path(binary).is_file():
            raise SetupError(f"服务器二进制不存在: {binary}")

        server = Server(binary, ROOT, echo=args.verbose)
        server.start()
        print(
            f"==> 服务器就绪 rtmp=127.0.0.1:{server.rtmp_port} "
            f"rtsp=127.0.0.1:{server.rtsp_port} "
            f"metrics=127.0.0.1:{server.metrics_port}"
        )
        ctx = Context(ffmpeg, server)

        selected = [c for c in CASES if not args.case or c[0] in args.case]
        for i, (key, title, fn) in enumerate(selected, 1):
            prefix = f"[{i}/{len(selected)}] {title}"
            print(f"{prefix} ...", end="", flush=True)
            started = time.time()
            try:
                detail = fn(ctx)
                print(f" PASS ({time.time() - started:.1f}s, {detail})")
            except CaseFailure as err:
                failures.append((key, str(err)))
                print(f" FAIL ({time.time() - started:.1f}s)")
                print(f"    {err}")
                print("    服务器日志尾部:")
                for line in server.log.tail(12):
                    print(f"      {line}")

        print("\n===== T-10 矩阵结果 =====")
        failed_keys = {key for key, _ in failures}
        for key, title, _ in selected:
            print(f"  {'FAIL' if key in failed_keys else 'PASS'}  {title}")
        if failures:
            print(f"\n{len(failures)} 个用例失败")
            return 1
        print("\nPASS: RTMP e2e 全矩阵通过")
        return 0
    except SetupError as err:
        print(f"错误: {err}", file=sys.stderr)
        return 2
    finally:
        if server is not None:
            server.stop()
        if args.keep:
            print(f"临时目录保留: {workdir}")
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
