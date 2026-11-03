#!/usr/bin/env python3
"""P0 端到端验证：PLAY -> PAUSE -> PLAY 的推流生命周期正确性。

覆盖 done.md 里 P0-1 ~ P0-4 的验收点，可反复跑：

  * PLAY 之后能收到 RTP；
  * PAUSE 之后 1 秒内不再有 RTP（推流 goroutine 真的停了）；
  * 再次 PLAY 恢复出流，且不是「又起一组」——SSRC 不变、seq 单调连续、
    速率不翻倍；
  * TEARDOWN 正常。

用法（先启动服务端，默认 8554）：

    go run "Stage5/5-1-6.go"          # 或先编译成 exe 再跑
    python test/e2e/rtsp_pause_play.py --port 8554

退出码 0 表示通过，1 表示失败。依赖 Python 3.7+，只用标准库。
"""

import argparse
import socket
import sys
import time

RTP_HDR_MIN = 12  # RTP 固定头至少 12 字节


def header_value(head, name):
    for line in head.split("\r\n")[1:]:
        key, _, value = line.partition(":")
        if key.strip().lower() == name.lower():
            return value.strip()
    return ""


class RTSPClient:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.sock.settimeout(0.1)
        self.buf = bytearray()
        # 每个元素：(channel, seq, ssrc, timestamp)
        self.rtp = []
        self.cseq = 0
        self.session = ""
        self._last_head = ""

    def _more(self):
        data = self.sock.recv(65536)
        if not data:
            raise EOFError("server closed connection")
        self.buf += data

    def _ensure(self, n):
        while len(self.buf) < n:
            self._more()

    def read_one(self):
        """读一条消息：interleaved RTP 帧返回 'rtp'，RTSP 响应返回状态行。"""
        while True:
            self._ensure(1)
            if self.buf[0] == 0x24:  # '$' interleaved 帧
                self._ensure(4)
                channel = self.buf[1]
                length = (self.buf[2] << 8) | self.buf[3]
                self._ensure(4 + length)
                payload = bytes(self.buf[4 : 4 + length])
                del self.buf[: 4 + length]
                if length >= RTP_HDR_MIN and (payload[0] & 0xC0) == 0x80:
                    seq = (payload[2] << 8) | payload[3]
                    ssrc = payload[8:12].hex()
                    ts = int.from_bytes(payload[4:8], "big")
                    self.rtp.append((channel, seq, ssrc, ts))
                return "rtp"

            while b"\r\n\r\n" not in self.buf:
                self._more()
            idx = self.buf.index(b"\r\n\r\n")
            head = self.buf[:idx].decode("utf-8", "replace")
            del self.buf[: idx + 4]
            content_length = 0
            for line in head.split("\r\n")[1:]:
                key, _, value = line.partition(":")
                if key.strip().lower() == "content-length":
                    content_length = int(value.strip())
            self._ensure(content_length)
            del self.buf[:content_length]
            self._last_head = head
            return head.split("\r\n")[0]

    def request(self, method, url, headers=None):
        self.cseq += 1
        lines = [f"{method} {url} RTSP/1.0", f"CSeq: {self.cseq}"]
        for key, value in (headers or {}).items():
            lines.append(f"{key}: {value}")
        self.sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
        status = None
        while True:
            msg = self.read_one()
            if msg != "rtp":
                status = msg
                break
        print(f">>> {method}: {status}")
        return status, self._last_head

    def collect(self, seconds):
        """读 seconds 秒，返回这期间新收到的 RTP 包（可能含 RTCP 被过滤）。"""
        end = time.time() + seconds
        mark = len(self.rtp)
        while time.time() < end:
            try:
                self.read_one()
            except socket.timeout:
                pass
        return self.rtp[mark:]

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def seq_continuity(packets):
    """seq 是否单调连续（允许 16 位回绕和小的正常间隔，不允许回退/归零重启）。"""
    last = None
    for _, seq, _, _ in packets:
        if last is None:
            last = seq
            continue
        delta = (seq - last) & 0xFFFF
        if delta == 0:
            return False, f"duplicate seq {seq}"
        if delta > 1000:
            return False, f"gap too large {last} -> {seq}"
        last = seq
    return True, ""


def run(host, port, path):
    url = f"rtsp://{host}:{port}{path}"
    client = RTSPClient(host, port)
    failures = []

    def ok(method, status):
        if "200" not in status:
            failures.append(f"{method}: {status}")

    status, _ = client.request("OPTIONS", url)
    ok("OPTIONS", status)
    status, _ = client.request("DESCRIBE", url, {"Accept": "application/sdp"})
    ok("DESCRIBE", status)
    status, head = client.request(
        "SETUP", url + "/trackID=0",
        {"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1"},
    )
    ok("SETUP video", status)
    client.session = header_value(head, "Session").split(";")[0]
    status, _ = client.request(
        "SETUP", url + "/trackID=1",
        {"Transport": "RTP/AVP/TCP;unicast;interleaved=2-3",
         "Session": client.session},
    )
    ok("SETUP audio", status)

    # 第一次 PLAY
    status, _ = client.request("PLAY", url, {"Session": client.session})
    ok("PLAY", status)
    first = client.collect(0.5)
    video_first = [p for p in first if p[0] == 0]
    if not video_first:
        failures.append("no video RTP after first PLAY")
        v_ssrc = v_last_seq = None
    else:
        v_ssrc = video_first[0][2]
        v_last_seq = video_first[-1][1]
        print(f"first PLAY: {len(video_first)} video pkts, ssrc={v_ssrc}, last_seq={v_last_seq}")

    # PAUSE 后应完全静默
    status, _ = client.request("PAUSE", url, {"Session": client.session})
    ok("PAUSE", status)
    pause_ack = time.time()
    mark = len(client.rtp)
    client.collect(1.2)
    after_pause = [p for p in client.rtp[mark:] if p[0] in (0, 2)]
    print(f"after PAUSE: {len(after_pause)} RTP packets in 1.2s")
    if after_pause:
        failures.append(f"{len(after_pause)} RTP packets after PAUSE ack")

    # 第二次 PLAY：恢复且不重复起流
    status, _ = client.request("PLAY", url, {"Session": client.session})
    ok("PLAY resume", status)
    second = client.collect(0.5)
    video_second = [p for p in second if p[0] == 0]
    if not video_second:
        failures.append("no video RTP after second PLAY")
    else:
        v2_ssrc = video_second[0][2]
        v2_first_seq = video_second[0][1]
        print(f"second PLAY: {len(video_second)} video pkts, ssrc={v2_ssrc}, first_seq={v2_first_seq}")

        if v_ssrc is not None and v2_ssrc != v_ssrc:
            failures.append(f"SSRC changed across PAUSE: {v_ssrc} -> {v2_ssrc}")

        # seq 从 PAUSE 前的 last_seq 连续接上（新起一组会从 0 开始，这里会断）
        if v_last_seq is not None:
            joined = [(0, v_last_seq, v2_ssrc, 0)] + video_second
            okk, why = seq_continuity(joined)
            if not okk:
                failures.append(f"seq discontinuity across PAUSE: {why}")

        # 速率不翻倍：恢复后 0.5s 的包数不该是第一段的 1.5 倍以上
        if video_first and len(video_second) > len(video_first) * 1.5:
            failures.append(
                f"packet rate doubled: {len(video_first)} -> {len(video_second)}"
            )

    # 全程同一条通道上只该有一个 SSRC（重复起流会冒出第二个 SSRC）
    for channel in (0, 2):
        ssrcs = {p[2] for p in client.rtp if p[0] == channel}
        if len(ssrcs) > 1:
            failures.append(f"channel {channel} saw multiple SSRCs: {ssrcs}")

    status, _ = client.request("TEARDOWN", url, {"Session": client.session})
    ok("TEARDOWN", status)
    client.close()

    if failures:
        print("\nFAIL:")
        for failure in failures:
            print("  -", failure)
        return 1
    print("\nPASS: PAUSE stops streams; PLAY resumes one continuous stream")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="RTSP PLAY/PAUSE/PLAY e2e check")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8554)
    parser.add_argument("--path", default="/live")
    args = parser.parse_args(argv)
    return run(args.host, args.port, args.path)


if __name__ == "__main__":
    sys.exit(main())
