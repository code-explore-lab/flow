package rtsp

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
)

func userinfo(user, pass string) *url.Userinfo { return url.UserPassword(user, pass) }

func TestPullAuthNoCredentials(t *testing.T) {
	var a *pullAuth
	if got := a.header("DESCRIBE", "rtsp://h/live"); got != "" {
		t.Errorf("nil auth header = %q, want empty", got)
	}
	if a.challenge(`Basic realm="flow"`) {
		t.Error("nil auth accepted a challenge")
	}
	if got := newPullAuth(nil); got != nil {
		t.Errorf("newPullAuth(nil) = %+v, want nil", got)
	}
}

func TestPullAuthBasic(t *testing.T) {
	a := newPullAuth(userinfo("alice", "s3cret"))
	if !a.challenge(`Basic realm="flow"`) {
		t.Fatal("Basic challenge rejected")
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:s3cret"))
	if got := a.header("DESCRIBE", "rtsp://h/live"); got != want {
		t.Errorf("header = %q, want %q", got, want)
	}
}

func TestPullAuthDigest(t *testing.T) {
	a := newPullAuth(userinfo("alice", "s3cret"))
	if !a.challenge(`Digest realm="flow", nonce="abc123", qop="auth", algorithm=MD5`) {
		t.Fatal("Digest challenge rejected")
	}
	h := a.header("DESCRIBE", "rtsp://h/live")
	for _, part := range []string{
		`username="alice"`, `realm="flow"`, `nonce="abc123"`,
		`uri="rtsp://h/live"`, `qop=auth`, `nc=00000001`, `algorithm=MD5`,
	} {
		if !strings.Contains(h, part) {
			t.Errorf("header %q missing %q", h, part)
		}
	}

	// 用与服务端相同的公式核对 response。
	params := parseAuthParams(strings.TrimPrefix(h, "Digest "))
	nc := params["nc"]
	cnonce := params["cnonce"]
	ha1 := md5Hex("alice:flow:s3cret")
	ha2 := md5Hex("DESCRIBE:rtsp://h/live")
	want := md5Hex(ha1 + ":abc123:" + nc + ":" + cnonce + ":auth:" + ha2)
	if params["response"] != want {
		t.Errorf("response = %q, want %q", params["response"], want)
	}

	// 第二个请求换 cnonce 并递增 nc。
	next := parseAuthParams(strings.TrimPrefix(a.header("SETUP", "rtsp://h/live/trackID=0"), "Digest "))
	if next["nc"] != "00000002" {
		t.Errorf("second nc = %q, want 00000002", next["nc"])
	}
	if next["cnonce"] == cnonce {
		t.Error("cnonce was reused across requests")
	}
}

func TestPullAuthDigestLegacyNoQop(t *testing.T) {
	a := newPullAuth(userinfo("u", "p"))
	if !a.challenge(`Digest realm="flow", nonce="n1"`) {
		t.Fatal("legacy Digest challenge rejected")
	}
	h := a.header("PLAY", "rtsp://h/live")
	if strings.Contains(h, "qop=") || strings.Contains(h, "nc=") {
		t.Errorf("RFC 2069 header should not carry qop/nc: %q", h)
	}
	params := parseAuthParams(strings.TrimPrefix(h, "Digest "))
	want := md5Hex(md5Hex("u:flow:p") + ":n1:" + md5Hex("PLAY:rtsp://h/live"))
	if params["response"] != want {
		t.Errorf("response = %q, want %q", params["response"], want)
	}
}

func TestPullAuthRejectsUnsupportedChallenges(t *testing.T) {
	cases := []string{
		``,
		`Bearer realm="flow"`,
		`Digest realm="flow"`, // 没有 nonce
		`Digest realm="flow", nonce="n", algorithm=SHA-256`, // 只支持 MD5
		`Digest realm="flow", nonce="n", qop="auth-int"`,    // 不支持 auth-int
	}
	for _, h := range cases {
		a := newPullAuth(userinfo("u", "p"))
		if a.challenge(h) {
			t.Errorf("challenge(%q) accepted, want rejection", h)
		}
	}
}

func TestPullAuthDigestQopList(t *testing.T) {
	a := newPullAuth(userinfo("u", "p"))
	if !a.challenge(`Digest realm="flow", nonce="n", qop="auth-int,auth"`) {
		t.Fatal("qop list containing auth should be accepted")
	}
	if !strings.Contains(a.header("DESCRIBE", "rtsp://h/live"), "qop=auth") {
		t.Error("header did not select qop=auth from the list")
	}
}
