package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/daemon"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestServeWaitsForShutdownOpToReleaseLeases: the shutdown op runs Shutdown
// on a goroutine, and Serve returns as soon as the listener closes. serve
// returned right then, so the process could exit with sessions still
// closing and their leases held. The daemon-level ordering is pinned
// deterministically by TestShutdownOpWaitsForSessions; this pins serve's
// wiring.
func TestServeWaitsForShutdownOpToReleaseLeases(t *testing.T) {
	testutil.RequireSQLite3(t)
	store := filepath.Join(t.TempDir(), "s")
	call(t, store, "init")
	call(t, store, "create", "app")
	sockDir, err := os.MkdirTemp("", "offshoot-cli-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "w.sock")

	serveDone := make(chan error, 1)
	go func() { serveDone <- run([]string{"-store", store, "serve", "-socket", sock}) }()
	t.Cleanup(func() { run([]string{"-store", store, "session", "shutdown", "-socket", sock}) })
	deadline := time.Now().Add(10 * time.Second)
	for !daemon.Running(sock) {
		if time.Now().After(deadline) {
			t.Fatal("daemon never came up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// run, not call: call swaps os.Stdout, which serve's goroutine reads.
	if err := run([]string{"-store", store, "session", "open", "app", "-socket", sock}); err != nil {
		t.Fatal(err)
	}

	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	session.CloseReleaseHook = func() {
		close(entered)
		<-proceed
	}
	t.Cleanup(func() {
		release()
		session.CloseReleaseHook = nil
	})

	if err := run([]string{"-store", store, "session", "shutdown", "-socket", sock}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the daemon never started closing the session")
	}
	select {
	case err := <-serveDone:
		release()
		t.Fatalf("serve returned (%v) while a session was still closing", err)
	default:
	}
	release()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve returned: %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("serve did not exit after the session closed")
	}
	w, err := ops.Open(store)
	if err != nil {
		t.Fatal(err)
	}
	ref, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if ref.LeaseHolder != "" {
		t.Fatalf("serve exited with the lease still held by %q", ref.LeaseHolder)
	}
}
