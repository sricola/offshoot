package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/offshoot-db/offshoot/internal/ops"
)

func newServer(t *testing.T) (*Server, *ops.Workspace) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	dir := t.TempDir()
	w, err := ops.Init(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(w, filepath.Join(dir, "sock"))
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv, w
}

// call sends one request on a fresh connection and returns the response.
func call(t *testing.T, sock string, req Request) Response {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(c)).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestServerOpenFlushStatusClose(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket perm = %o, want 600", perm)
	}

	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK || open.Checkout == "" {
		t.Fatalf("open = %+v", open)
	}
	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}

	// Flush may race capture; retry briefly until the row is durable.
	var txid uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main", Name: ""})
		if resp.OK {
			txid = resp.TXID
			ref, _, err := w.Store.GetRef("app", "main")
			if err != nil {
				t.Fatal(err)
			}
			if ref.HeadTXID == txid {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if txid == 0 {
		t.Fatal("flush never succeeded")
	}

	st := call(t, sock, Request{Op: "status"})
	if !st.OK || len(st.Sessions) != 1 {
		t.Fatalf("status = %+v", st)
	}
	if st.Sessions[0].DB != "app" || st.Sessions[0].DurableTXID != txid {
		t.Fatalf("session info = %+v", st.Sessions[0])
	}

	cl := call(t, sock, Request{Op: "close", DB: "app", Branch: "main"})
	if !cl.OK {
		t.Fatalf("close = %+v", cl)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("close must release the lease, holder = %q", ref.LeaseHolder)
	}
}

func TestServerRefusesDoubleOpen(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("first open = %+v", r)
	}
	r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if r.OK {
		t.Fatal("second open of the same branch must be refused")
	}
	if r.Error == "" {
		t.Fatal("a refusal must carry an error message")
	}
}

func TestShutdownReleasesEveryLease(t *testing.T) {
	srv, w := newServer(t)
	if r := call(t, srv.SocketPath(), Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("shutdown must release leases, holder = %q", ref.LeaseHolder)
	}
	if _, err := os.Stat(srv.SocketPath()); !os.IsNotExist(err) {
		t.Fatalf("shutdown must remove the socket, stat err = %v", err)
	}
}
