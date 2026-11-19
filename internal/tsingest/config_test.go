package tsingest

import "testing"

func TestParseUDPInputs(t *testing.T) {
	got, err := ParseUDPInputs("live/udp=239.1.1.1:5000, live/udp2=0.0.0.0:5001; live/udp3=:5002\nlive/udp4=[::1]:5003")
	if err != nil {
		t.Fatalf("ParseUDPInputs: %v", err)
	}
	want := []UDPInput{
		{Name: "live/udp", Addr: "239.1.1.1:5000"},
		{Name: "live/udp2", Addr: "0.0.0.0:5001"},
		{Name: "live/udp3", Addr: ":5002"},
		{Name: "live/udp4", Addr: "[::1]:5003"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d inputs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("input %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestParseUDPInputsErrors(t *testing.T) {
	cases := []string{
		"live/udp",                  // 缺少 =
		"=239.1.1.1:5000",           // 空流名
		"live/udp=",                 // 空地址
		"live/udp=239.1.1.1",        // 缺端口
		"live/udp=239.1.1.1:0",      // 非法端口
		"live/udp=239.1.1.1:70000",  // 端口越界
		"live/udp=example.com:5000", // 非 IP
		"a=239.1.1.1:5000,a=:5001",  // 重名
	}
	for _, spec := range cases {
		if _, err := ParseUDPInputs(spec); err == nil {
			t.Errorf("ParseUDPInputs(%q) = nil error, want failure", spec)
		}
	}
	if got, err := ParseUDPInputs(""); err != nil || got != nil {
		t.Errorf("ParseUDPInputs(\"\") = %v, %v; want nil, nil", got, err)
	}
}

func TestParseFileInputs(t *testing.T) {
	got, err := ParseFileInputs("live/a=clips/a.ts;live/b=/tmp/b.ts,live/c=C:\\video\\c.ts")
	if err != nil {
		t.Fatalf("ParseFileInputs: %v", err)
	}
	want := []FileInput{
		{Name: "live/a", Path: "clips/a.ts"},
		{Name: "live/b", Path: "/tmp/b.ts"},
		{Name: "live/c", Path: `C:\video\c.ts`},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d inputs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("input %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestParseFileInputsErrors(t *testing.T) {
	cases := []string{
		"live/a",
		"=clips/a.ts",
		"live/a=",
		"live/a=x.ts,live/a=y.ts",
	}
	for _, spec := range cases {
		if _, err := ParseFileInputs(spec); err == nil {
			t.Errorf("ParseFileInputs(%q) = nil error, want failure", spec)
		}
	}
}
