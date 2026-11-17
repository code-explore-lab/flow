package rtsp

// E-6 RTSPS 测试：证书加载、TLS 监听复用现有处理逻辑、明文客户端被握手拒绝。

import (
	"bufio"
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
	"strings"
	"testing"
	"time"
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

// freeRTSPAddr 借一个空闲端口：监听 :0 拿到真实地址后立即关闭。
func freeRTSPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// startRTSPServer 起一个挂了自签证书、走 Serve 的 RTSPS 服务器。
// 返回地址；测试结束时取消 ctx 并等待 Serve 退出。
func startRTSPServer(t *testing.T) string {
	t.Helper()

	certPath, keyPath := writeSelfSignedCert(t, t.TempDir())
	s := New(nil, nil, false)
	if err := s.SetTLS(certPath, keyPath); err != nil {
		t.Fatalf("SetTLS: %v", err)
	}
	if s.tlsConfig == nil {
		t.Fatal("SetTLS succeeded but tlsConfig is nil")
	}

	addr := freeRTSPAddr(t)
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

// dialRTSPS 轮询建立 TLS 连接，等 Serve 真正开始监听（端口刚释放，有窗口）。
func dialRTSPS(t *testing.T, addr string) *tls.Conn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial rtsps %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestServeRTSPS 验证 RTSPS 监听复用现有 RTSP 处理逻辑：TLS 握手成功后
// OPTIONS 正常得到 200。
func TestServeRTSPS(t *testing.T) {
	addr := startRTSPServer(t)
	conn := dialRTSPS(t, addr)
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	resp := sendRTSP(t, br, bw, "OPTIONS rtsps://"+addr+"/live RTSP/1.0\r\nCSeq: 1\r\n\r\n")
	if !strings.HasPrefix(resp, "RTSP/1.0 200 OK\r\n") {
		t.Fatalf("OPTIONS response = %q", firstLine(resp))
	}
}

// dialPlaintext 轮询建立明文 TCP 连接，等 Serve 真正开始监听（端口刚释放，有窗口）。
func dialPlaintext(t *testing.T, addr string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRTSPSRejectsPlaintext 验证明文客户端连 TLS 端口不会拿到 RTSP 响应：
// 显式 TLS 握手失败后连接被关闭。
func TestRTSPSRejectsPlaintext(t *testing.T) {
	addr := startRTSPServer(t)

	conn := dialPlaintext(t, addr)
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}

	req := "OPTIONS rtsp://" + addr + "/live RTSP/1.0\r\nCSeq: 1\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write plaintext request: %v", err)
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err == nil && strings.HasPrefix(string(buf[:n]), "RTSP/") {
		t.Fatalf("plaintext client got a plaintext RTSP reply %q", buf[:n])
	}
}

// TestSetTLSRejectsBadFiles 验证证书缺失、证书与私钥不匹配时返回错误，
// 服务器保持明文模式。
func TestSetTLSRejectsBadFiles(t *testing.T) {
	s := New(nil, nil, false)

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
