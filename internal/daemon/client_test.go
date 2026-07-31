package daemon

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultSocketPathIsStableAndPerStore(t *testing.T) {
	a1, err := DefaultSocketPath("s3://bucket/p")
	if err != nil {
		t.Fatal(err)
	}
	a2, _ := DefaultSocketPath("s3://bucket/p")
	b, _ := DefaultSocketPath("s3://bucket/other")
	if a1 != a2 {
		t.Fatalf("unstable: %s vs %s", a1, a2)
	}
	if a1 == b {
		t.Fatal("different stores must get different sockets")
	}
	if !strings.HasSuffix(a1, ".sock") {
		t.Fatalf("socket path = %s", a1)
	}
}

func TestSocketPathHonorsEnv(t *testing.T) {
	t.Setenv("OFFSHOOT_SOCKET", "/tmp/custom.sock")
	p, err := DefaultSocketPath("anything")
	if err != nil || p != "/tmp/custom.sock" {
		t.Fatalf("p=%s err=%v", p, err)
	}
}

func TestCallWithoutDaemonIsClear(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "absent.sock")
	if Running(sock) {
		t.Fatal("Running must be false with no listener")
	}
	_, err := Call(sock, Request{Op: "status"})
	if err == nil {
		t.Fatal("Call must fail when no daemon is listening")
	}
	if !strings.Contains(err.Error(), "no daemon") {
		t.Fatalf("error should name the missing daemon: %v", err)
	}
}

func TestCallRoundTripsAgainstAServer(t *testing.T) {
	srv, _ := newServer(t) // from server_test.go, same package
	resp, err := Call(srv.SocketPath(), Request{Op: "status"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("resp = %+v", resp)
	}
}
