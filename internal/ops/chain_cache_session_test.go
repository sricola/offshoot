package ops_test

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
)

// TestCachedChainMissesAfterSessionFlush: a daemon session's flushes move
// the head under the session's own epoch without restamping the checkout's
// sidecar, so the chain the last at-rest checkpoint recorded no longer
// names the head's identity and is not used; once the session closes, the
// next at-rest checkpoint resolves the lineage and its head materializes to
// the checkout. (External package: it needs internal/session.)
func TestCachedChainMissesAfterSessionFlush(t *testing.T) {
	w := newWS(t)
	path := leaseSeededMain(t, w)
	if _, err := os.Stat(path + ".shadow"); err != nil {
		t.Skip("no shadow after a checkpoint: the workspace filesystem cannot clone")
	}
	at, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := w.CachedChainForTest(path, at); !ok {
		t.Fatal("the at-rest checkpoint recorded no usable chain")
	}
	s, err := session.Open(context.Background(), session.Options{WS: w, DB: "app", Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	leaseSQL(t, s.CheckoutPath(), "INSERT INTO t VALUES (2);")
	if _, err := s.Flush("", nil); err != nil {
		s.Close()
		t.Fatal(err)
	}
	flushed, _, err := w.Store.GetRef("app", "main")
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	if got, ok := w.CachedChainForTest(path, flushed); ok {
		s.Close()
		t.Fatalf("the recorded chain answered for the session's head: %+v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var sources []string
	ops.SetObserveChainSourceForTest(func(kind string) { sources = append(sources, kind) })
	t.Cleanup(func() { ops.SetObserveChainSourceForTest(nil) })
	leaseSQL(t, path, "INSERT INTO t VALUES (3);")
	if _, err := w.Checkpoint("app", "main", "after", nil); err != nil {
		t.Fatal(err)
	}
	if want := []string{"resolve"}; !reflect.DeepEqual(sources, want) {
		t.Fatalf("chain sources %q after a session flushed the branch, want %q", sources, want)
	}
	head, err := w.CheckoutAt("app", "main", "after", true)
	if err != nil {
		t.Fatal(err)
	}
	a, err := os.ReadFile(head)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("the checkpoint's head differs from the checkout")
	}
}
