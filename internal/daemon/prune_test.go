package daemon

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// TestPruneOp drives the prune op end to end: by name and by keep count,
// the protected-branch gate and its force, the all-or-nothing refusal of an
// unknown name, and — unlike destroy and rollback — no refusal while this
// daemon has a session open on the branch, since a prune writes only
// checkpoint metadata.
func TestPruneOp(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()

	if r := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "work"}); !r.OK {
		t.Fatalf("fork = %+v", r)
	}
	path, err := w.Checkout("app", "work")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if out, err := testutil.SQLite3CLI(path, "CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		if _, err := w.Checkpoint("app", "work", name, nil); err != nil {
			t.Fatal(err)
		}
	}

	r := call(t, sock, Request{Op: "prune", DB: "app", Branch: "work", Names: []string{"a"}})
	if !r.OK || !reflect.DeepEqual(r.Pruned, []string{"a"}) || r.Kept == nil || *r.Kept != 2 {
		t.Fatalf("prune a = %+v (kept %v), want pruned [a], kept 2", r, r.Kept)
	}
	if r := call(t, sock, Request{Op: "prune", DB: "app", Branch: "work", Names: []string{"b", "nope"}}); r.OK || !strings.Contains(r.Error, `"nope"`) {
		t.Fatalf("prune with an unknown name = %+v, want a refusal naming it", r)
	}
	keep := 1
	r = call(t, sock, Request{Op: "prune", DB: "app", Branch: "work", Keep: &keep})
	if !r.OK || !reflect.DeepEqual(r.Pruned, []string{"fork"}) || *r.Kept != 1 {
		t.Fatalf("prune keep 1 = %+v, want pruned [fork], kept 1", r)
	}
	br := call(t, sock, Request{Op: "branches", DB: "app"})
	if got := branchInfo(t, br, "work").Checkpoints; !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("checkpoints after prune = %v, want [b]", got)
	}
	// keep 0 is a real request, not an omitted field.
	zero := 0
	if r := call(t, sock, Request{Op: "prune", DB: "app", Branch: "work", Keep: &zero}); !r.OK || *r.Kept != 0 {
		t.Fatalf("prune keep 0 = %+v", r)
	}
	if r := call(t, sock, Request{Op: "prune", DB: "app", Branch: "work"}); r.OK {
		t.Fatal("prune with neither names nor keep must refuse")
	}

	// main is protected by default, and open here.
	if r := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("open = %+v", r)
	}
	if r := call(t, sock, Request{Op: "prune", DB: "app", Branch: "main", Names: []string{"init"}}); r.OK || !strings.Contains(r.Error, "protected") {
		t.Fatalf("unforced prune of protected main = %+v, want a protected refusal", r)
	}
	if r := call(t, sock, Request{Op: "prune", DB: "app", Branch: "main", Names: []string{"init"}, Force: true}); !r.OK {
		t.Fatalf("forced prune beside an open session = %+v", r)
	}
	if r := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"}); !r.OK {
		t.Fatalf("flush after the prune = %+v", r)
	}
}
