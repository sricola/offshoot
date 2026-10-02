package store

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLocalPutReaderIfStreamsBeforeTakingTheLock: the per-key lock is held
// only around the compare and the rename, never while the object's bytes
// stream in. lock() breaks any lock older than 30 s on the assumption its
// holder died, so a lock held across a multi-GB streamed write could be
// broken by a rival mid-write; streaming first keeps the lock in the
// millisecond range that rule was designed for.
func TestLocalPutReaderIfStreamsBeforeTakingTheLock(t *testing.T) {
	root := t.TempDir()
	l, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(root, "data", "big") + ".lock"
	pr, pw := io.Pipe()
	sawLockWhileStreaming := make(chan bool, 1)
	go func() {
		pw.Write([]byte("first half "))
		_, statErr := os.Stat(lockPath)
		sawLockWhileStreaming <- statErr == nil
		pw.Write([]byte("second half"))
		pw.Close()
	}()
	etag, err := l.PutReaderIf("data/big", pr, int64(len("first half second half")), "")
	if err != nil {
		t.Fatal(err)
	}
	if <-sawLockWhileStreaming {
		t.Fatal("the per-key lock was held while the object was still streaming in")
	}
	data, got, err := l.Get("data/big")
	if err != nil || string(data) != "first half second half" || got != etag {
		t.Fatalf("after put: %q etag %q (want %q) err %v", data, got, etag, err)
	}
	if _, err := os.Stat(lockPath); err == nil {
		t.Fatal("lock file left behind")
	}
	if _, err := l.PutReaderIf("data/big", strings.NewReader(""), 0, ""); err == nil {
		t.Fatal("create-only put over an existing key must fail")
	}
}
