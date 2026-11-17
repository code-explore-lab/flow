package limiter

import "testing"

func TestConnLimiterTotal(t *testing.T) {
	l := New(2, 0)
	if ok, _ := l.Allow("1.1.1.1"); !ok {
		t.Fatal("first connection rejected")
	}
	if ok, _ := l.Allow("2.2.2.2"); !ok {
		t.Fatal("second connection rejected")
	}
	if ok, reason := l.Allow("3.3.3.3"); ok || reason == "" {
		t.Fatalf("third connection = (%v, %q), want rejected with reason", ok, reason)
	}
	if got := l.Count(); got != 2 {
		t.Fatalf("Count() = %d, want 2", got)
	}

	l.Release("1.1.1.1")
	if ok, _ := l.Allow("3.3.3.3"); !ok {
		t.Fatal("connection after release rejected")
	}
}

func TestConnLimiterPerIP(t *testing.T) {
	l := New(0, 2)
	if ok, _ := l.Allow("1.1.1.1"); !ok {
		t.Fatal("first same-ip connection rejected")
	}
	if ok, _ := l.Allow("1.1.1.1"); !ok {
		t.Fatal("second same-ip connection rejected")
	}
	if ok, reason := l.Allow("1.1.1.1"); ok || reason == "" {
		t.Fatalf("third same-ip connection = (%v, %q), want rejected", ok, reason)
	}
	// 另一 IP 不受影响。
	if ok, _ := l.Allow("2.2.2.2"); !ok {
		t.Fatal("different ip rejected")
	}
	if got := l.PerIP("1.1.1.1"); got != 2 {
		t.Fatalf("PerIP = %d, want 2", got)
	}

	l.Release("1.1.1.1")
	l.Release("1.1.1.1")
	if got := l.PerIP("1.1.1.1"); got != 0 {
		t.Fatalf("PerIP after release = %d, want 0", got)
	}
}

func TestConnLimiterUnlimited(t *testing.T) {
	l := New(0, 0)
	for i := 0; i < 1000; i++ {
		if ok, _ := l.Allow("1.1.1.1"); !ok {
			t.Fatal("unlimited limiter rejected a connection")
		}
	}
	if got := l.Count(); got != 1000 {
		t.Fatalf("Count() = %d, want 1000", got)
	}
}

func TestConnLimiterNil(t *testing.T) {
	var l *ConnLimiter
	if ok, _ := l.Allow("1.1.1.1"); !ok {
		t.Fatal("nil limiter should allow")
	}
	l.Release("1.1.1.1")
	if l.Count() != 0 || l.PerIP("1.1.1.1") != 0 {
		t.Fatal("nil limiter counters should be zero")
	}
}
