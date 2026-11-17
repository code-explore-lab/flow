package rtmp

// T-2 RTMPS 测试：证书加载、TLS 监听复用现有 RTMP 处理逻辑、明文客户端被拒绝。

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"flow/internal/flv"
)

// writeSelfSignedCert 生成一张自签证书（localhost + 127.0.0.1）写到 dir，
// 返回 cert/key 文件路径。
func writeSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// startRTMPSServer 起一个挂了自签证书、走 Serve 的 RTMPS 服务器。
// 返回地址；测试结束时取消 ctx 并等待 Serve 退出。
func startRTMPSServer(t *testing.T) string {
	t.Helper()

	certPath, keyPath := writeSelfSignedCert(t, t.TempDir())
	s := New(nil, nil, nil)
	if err := s.SetTLS(certPath, keyPath); err != nil {
		t.Fatalf("SetTLS: %v", err)
	}
	if s.tlsConfig == nil {
		t.Fatal("SetTLS succeeded but tlsConfig is nil")
	}

	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Serve(ctx, addr) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("Serve returned %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after cancel")
		}
	})
	return addr
}

// dialRTMPS 轮询建立 TLS 连接，等 Serve 真正开始监听（端口刚释放，有窗口）。
func dialRTMPS(t *testing.T, addr string) *tls.Conn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial rtmps %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestServeRTMPS 验证 RTMPS 监听复用现有 RTMP 处理逻辑：TLS 握手成功后
// 完成 RTMP 握手并收到 connect 的 _result。
func TestServeRTMPS(t *testing.T) {
	addr := startRTMPSServer(t)
	raw := dialRTMPS(t, addr)
	defer raw.Close()
	if err := raw.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	client := newConn(raw)
	clientHandshake(t, client)

	if err := client.writeMessage(&Message{
		TypeID:  msgCommandAMF0,
		Payload: buildCommand("connect", 1, flv.Obj(flv.Elem("app", flv.Str("live")))),
	}, commandCSID); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	res := readUntilCommand(t, client, "_result")
	if code := commandCode(res); code != "NetConnection.Connect.Success" {
		t.Fatalf("connect code = %q", code)
	}
}

// TestRTMPSRejectsPlaintext 验证明文客户端连 TLS 端口不会拿到 RTMP 响应：
// 显式 TLS 握手失败后连接被关闭。
func TestRTMPSRejectsPlaintext(t *testing.T) {
	addr := startRTMPSServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}

	// 客户端像明文 RTMP 一样发 C0/C1：TLS 服务端会把它当成坏的 ClientHello。
	hello := make([]byte, 1+handshakeSize)
	hello[0] = handshakeVersion
	if _, err := conn.Write(hello); err != nil {
		t.Fatalf("write plaintext handshake: %v", err)
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err == nil && n > 0 && buf[0] == handshakeVersion {
		t.Fatalf("plaintext client got a plaintext RTMP reply %q", buf[:n])
	}
}

// TestSetTLSRejectsBadFiles 验证证书缺失、证书与私钥不匹配时返回错误，
// 服务器保持明文模式。
func TestSetTLSRejectsBadFiles(t *testing.T) {
	s := New(nil, nil, nil)

	missing := filepath.Join(t.TempDir(), "missing.pem")
	if err := s.SetTLS(missing, missing); err == nil {
		t.Fatal("SetTLS accepted missing files")
	}
	if s.tlsConfig != nil {
		t.Fatal("tlsConfig set after failed SetTLS")
	}

	cert1, _ := writeSelfSignedCert(t, t.TempDir())
	_, key2 := writeSelfSignedCert(t, t.TempDir())
	if err := s.SetTLS(cert1, key2); err == nil {
		t.Fatal("SetTLS accepted mismatched cert/key")
	}
	if s.tlsConfig != nil {
		t.Fatal("tlsConfig set after mismatched SetTLS")
	}
}

// TestRTMPSHandshakeTimesOut 慢客户端不发 ClientHello 时，服务端必须在
// TLS 握手超时后关闭连接，不能白占 goroutine。
func TestRTMPSHandshakeTimesOut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10s TLS handshake timeout in -short mode")
	}
	addr := startRTMPSServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(tlsHandshakeTimeout + 3*time.Second)); err != nil {
		t.Fatal(err)
	}

	// 什么都不发，服务端握手应超时并关闭连接。
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("server accepted a silent TLS client (read succeeded)")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("server kept the silent TLS connection open past the handshake timeout")
	}
}

// TestRTMPSBadTLSRecord 明文客户端发非 TLS 数据时，读取应失败并让 Serve
// 继续接受后续连接（单条坏连接不影响监听）。
func TestRTMPSBadTLSRecord(t *testing.T) {
	addr := startRTMPSServer(t)

	bad, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := bad.Write([]byte("not tls at all")); err != nil {
		t.Fatalf("write: %v", err)
	}
	bad.Close()

	// 紧随其后的正常 TLS 客户端仍能完成 RTMP 握手。
	conn := dialRTMPS(t, addr)
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	client := newConn(conn)
	clientHandshake(t, client)
	if err := client.writeMessage(&Message{
		TypeID:  msgCommandAMF0,
		Payload: buildCommand("connect", 1, flv.Obj(flv.Elem("app", flv.Str("live")))),
	}, commandCSID); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	if code := commandCode(readUntilCommand(t, client, "_result")); code != "NetConnection.Connect.Success" {
		t.Fatalf("connect code = %q", code)
	}
}

// TestRTMPSStillServesPlainRTMP 验证同一份 Server 代码的明文路径没有回归：
// 未调用 SetTLS 时 Serve 仍是明文 RTMP。
func TestRTMPSStillServesPlainRTMP(t *testing.T) {
	s := New(nil, nil, nil)
	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Serve(ctx, addr) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errc:
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after cancel")
		}
	})

	var raw net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for {
		var err error
		raw, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer raw.Close()
	client := newConn(raw)
	clientHandshake(t, client)
	if err := client.writeMessage(&Message{
		TypeID:  msgCommandAMF0,
		Payload: buildCommand("connect", 1, flv.Obj(flv.Elem("app", flv.Str("live")))),
	}, commandCSID); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	if code := commandCode(readUntilCommand(t, client, "_result")); code != "NetConnection.Connect.Success" {
		t.Fatalf("connect code = %q", code)
	}
}
