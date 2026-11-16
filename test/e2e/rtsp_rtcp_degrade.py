#!/usr/bin/env python3
"""C-5/C-6 端到端验证：SR 策略与反馈驱动降级。

覆盖 done.md 里 C-5/C-6 的验收点，可反复跑：

  * DESCRIBE 的 SDP 含 a=rtcp-rsize；
  * 正常播放时能收到 RTCP SR，且 9 秒内 SR 数量符合 2.5~7.5s 的间隔；
  * 持续发送高丢包 RR 后，服务端只发参数集/IDR，视频包速率明显下降；
  * 高丢包期间 RTP 仍在流动（不断流）；
  * 持续发送低丢包 RR 后，发送速率恢复。

用法（先启动服务端 + 循环推流）：

    go run ./cmd/server
    ffmpeg -re -stream_loop -1 -i testdata/test.mp4 -c copy \
           -f rtsp -rtsp_transport tcp rtsp://127.0.0.1:8554/live/test
    python test/e2e/rtsp_rtcp_degrade.py --path /live/test

Git Bash 会把以 / 开头的参数当成路径（/live/test 变成 C:/Program
Files/Git/live/test），此时去掉 --path 用默认值，或设置
MSYS_NO_PATHCONV=1。若 8554 已被其他程序占用，用 --host 指向本机
另一个网卡地址即可。

退出码 0 表示通过，1 表示失败。依赖 Python 3.7+，只用标准库。
"""

import argparse
import socket
import struct
import sys
import time

RTP_HDR_MIN = 12
VIDEO_RTP_CH = 0
VIDEO_RTCP_CH = 1


def header_value(head, name):
    for line in head.split("\r\n")[1:]:
        key, _, value = line.partition(":")
        if key.strip().lower() == name.lower():
            return value.strip()
    return ""


def build_rr(media_ssrc, seq, fraction_lost):
    """构造一个 32 字节的 RR（RC=1，单个 report block）。"""
    reporter = 0x11223344
    header = struct.pack("!BBHI", 0x81, 201, 7, reporter)
    block = struct.pack(
        "!IB3sIIII",
        media_ssrc,
        int(fraction_lost * 256) & 0xFF,
        b"\x00\x00\x00",  # cumulative lost
        seq,              # extended highest seq
        0,                # jitter
        0,                # LSR
        0,                # DLSR
    )
    return header + block


class RTSPClient:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.sock.settimeout(0.05)
        self.buf = bytearray()
        self.rtp = []  # (channel, seq, ssrc, timestamp)
        self.rtcp = []  # (channel, pt, length)
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
        while True:
            self._ensure(1)
            if self.buf[0] == 0x24:  # '$' interleaved 帧
                self._ensure(4)
                channel = self.buf[1]
                length = (self.buf[2] << 8) | self.buf[3]
                self._ensure(4 + length)
                payload = bytes(self.buf[4:4 + length])
                del self.buf[:4 + length]
                if length >= 4 and (payload[0] & 0xC0) == 0x80:
                    pt = payload[1]
                    if 192 <= pt <= 223:
                        self.rtcp.append((channel, pt, length))
                    elif channel == VIDEO_RTP_CH and length >= RTP_HDR_MIN:
                        seq = (payload[2] << 8) | payload[3]
                        ssrc = int.from_bytes(payload[8:12], "big")
                        ts = int.from_bytes(payload[4:8], "big")
                        self.rtp.append((channel, seq, ssrc, ts))
                return "interleaved"

            while b"\r\n\r\n" not in self.buf:
                self._more()
            idx = self.buf.index(b"\r\n\r\n")
            head = self.buf[:idx].decode("utf-8", "replace")
            del self.buf[:idx + 4]
            content_length = 0
            for line in head.split("\r\n")[1:]:
                key, _, value = line.partition(":")
                if key.strip().lower() == "content-length":
                    content_length = int(value.strip())
            body = b""
            if content_length:
                self._ensure(content_length)
                body = bytes(self.buf[:content_length])
                del self.buf[:content_length]
            self._last_head = head
            self._last_body = body.decode("utf-8", "replace")
            return head.split("\r\n")[0]

    def request(self, method, url, headers=None):
        self.cseq += 1
        lines = [f"{method} {url} RTSP/1.0", f"CSeq: {self.cseq}"]
        for key, value in (headers or {}).items():
            lines.append(f"{key}: {value}")
        self.sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
        while True:
            msg = self.read_one()
            if msg != "interleaved":
                print(f">>> {method}: {msg}")
                return msg, self._last_head, getattr(self, "_last_body", "")

    def send_rr(self, fraction_lost):
        if not self.rtp:
            return
        _, seq, ssrc, _ = self.rtp[-1]
        pkt = build_rr(ssrc, seq, fraction_lost)
        frame = b"$" + bytes([VIDEO_RTCP_CH]) + len(pkt).to_bytes(2, "big") + pkt
        self.sock.sendall(frame)

    def pump(self, seconds, rr_loss=None, rr_interval=1.0):
        """读取 seconds 秒；rr_loss 非 None 时每秒发一条该丢包率的 RR。"""
        end = time.time() + seconds
        next_rr = time.time()
        mark_rtp, mark_rtcp = len(self.rtp), len(self.rtcp)
        while time.time() < end:
            if rr_loss is not None and time.time() >= next_rr:
                self.send_rr(rr_loss)
                next_rr += rr_interval
            try:
                self.read_one()
            except socket.timeout:
                pass
        return self.rtp[mark_rtp:], self.rtcp[mark_rtcp:]

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def run(host, port, path, loss):
    url = f"rtsp://{host}:{port}{path}"
    client = RTSPClient(host, port)
    failures = []

    def ok(name, status):
        if "200" not in status:
            failures.append(f"{name}: {status}")

    status, _, _ = client.request("OPTIONS", url)
    ok("OPTIONS", status)
    status, _, sdp = client.request("DESCRIBE", url, {"Accept": "application/sdp"})
    ok("DESCRIBE", status)
    if "a=rtcp-rsize" not in sdp:
        failures.append("SDP missing a=rtcp-rsize")

    status, head, _ = client.request(
        "SETUP", url + "/trackID=0",
        {"Transport": "RTP/AVP/TCP;unicast;interleaved=0-1"},
    )
    ok("SETUP video", status)
    client.session = header_value(head, "Session").split(";")[0]

    status, _, _ = client.request("PLAY", url, {"Session": client.session})
    ok("PLAY", status)

    # 基线：正常播放 9 秒。首个 SR 的间隔是 [0.5,1.5) × 5s，最坏 7.5s
    # 才到，窗口必须覆盖它，否则 C-5 断言会偶发失败。
    base_rtp, base_rtcp = client.pump(9.0)
    if not base_rtp:
        failures.append("no video RTP after PLAY")
        client.close()
        return 1
    base_rate = len(base_rtp) / 9.0
    print(f"baseline: {len(base_rtp)} video pkts in 9s ({base_rate:.1f}/s), "
          f"{len(base_rtcp)} RTCP pkts")

    # C-5：正常码率下 Td 被 5s 下限钳住，9 秒内应有 1~3 个 SR。
    # 少于 1 个说明根本没发；多于 4 个说明间隔没有被下限/预算约束。
    if not base_rtcp:
        failures.append("no RTCP SR received in 9s")
    else:
        pts = {pt for _, pt, _ in base_rtcp}
        if 200 not in pts:
            failures.append(f"no SR (PT=200) received, got PTs {pts}")
        if len(base_rtcp) > 4:
            failures.append(
                f"too many RTCP packets in 9s: {len(base_rtcp)} (SR interval below 2.5s)"
            )

    # C-6：持续高丢包，等待降级。
    client.pump(7.0, rr_loss=loss, rr_interval=1.0)
    degraded_rtp, _ = client.pump(3.0)
    degraded_rate = len(degraded_rtp) / 3.0
    print(f"degraded: {len(degraded_rtp)} video pkts in 3s ({degraded_rate:.1f}/s)")

    if not degraded_rtp:
        failures.append("RTP stopped during degradation (stream broke)")
    if degraded_rate >= base_rate * 0.5:
        failures.append(
            f"frame rate did not drop: {base_rate:.1f}/s -> {degraded_rate:.1f}/s"
        )

    # 持续低丢包，等待恢复。
    client.pump(7.0, rr_loss=0.0, rr_interval=1.0)
    recovered_rtp, _ = client.pump(2.0)
    recovered_rate = len(recovered_rtp) / 2.0
    print(f"recovered: {len(recovered_rtp)} video pkts in 2s ({recovered_rate:.1f}/s)")
    if recovered_rate < base_rate * 0.5:
        failures.append(
            f"frame rate did not recover: {base_rate:.1f}/s -> {recovered_rate:.1f}/s"
        )

    status, _, _ = client.request("TEARDOWN", url, {"Session": client.session})
    ok("TEARDOWN", status)
    client.close()

    if failures:
        print("\nFAIL:")
        for failure in failures:
            print("  -", failure)
        return 1
    print("\nPASS: SDP advertises rtcp-rsize; SR present; "
          "non-key frames dropped under loss and restored afterwards")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="RTSP C-5/C-6 e2e check")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8554)
    parser.add_argument("--path", default="/live/test")
    parser.add_argument("--loss", type=float, default=0.25,
                        help="fraction lost sent during the high-loss phase")
    args = parser.parse_args(argv)
    return run(args.host, args.port, args.path, args.loss)


if __name__ == "__main__":
    sys.exit(main())
