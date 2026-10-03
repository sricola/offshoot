package daemon

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/dbfile"
)

// cachedUnder lists dbfile's live (non-orphan) descriptors under dir.
func cachedUnder(dir string) []string {
	abs, _ := filepath.Abs(dir)
	var out []string
	for _, e := range dbfile.Entries() {
		if !e.Orphan && strings.HasPrefix(e.Path, abs+string(filepath.Separator)) {
			out = append(out, e.Path)
		}
	}
	sort.Strings(out)
	return out
}

// waitForPins waits until dbfile's registry holds exactly want pins in the
// whole process. dbfile is process-wide, so a pin leaked by an earlier test
// in this binary would otherwise surface here as the wrong descriptors
// surviving an Evict, with nothing pointing at the leak. A brief wait lets
// a raw read the engine is still finishing (its startup rebase) unpin.
func waitForPins(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := dbfile.ReadStats().Pins
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("dbfile pins = %d, want %d; entries: %+v (a pin leaked by an earlier test?)",
				got, want, dbfile.Entries())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestJanitorEvictsUnderFDBudget(t *testing.T) {
	srv, w := newServer(t)
	sock := srv.SocketPath()
	srv.mu.Lock()
	def := srv.fdBudget
	srv.mu.Unlock()
	if def != DefaultFDBudget {
		t.Fatalf("default fd budget = %d, want %d", def, DefaultFDBudget)
	}

	// Three at-rest checkouts: three cached, unpinned descriptors.
	for _, b := range []string{"a", "b", "c"} {
		if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Checkout("app", b); err != nil {
			t.Fatal(err)
		}
	}
	// One open session: its capture engine pins main's checkout.
	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	mainPath := w.CheckoutPath("app", "main")
	deadline := time.Now().Add(10 * time.Second)
	for n, _ := dbfile.PinsAt(mainPath); n < 1; n, _ = dbfile.PinsAt(mainPath) {
		if time.Now().After(deadline) {
			t.Fatal("the session's engine never pinned its checkout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The engine's hold is the only pin in the process.
	waitForPins(t, 1)
	// LRU order among the at-rest checkouts: a oldest, then c, b newest.
	for _, b := range []string{"a", "c", "b"} {
		s, err := dbfile.Reader(w.CheckoutPath("app", b))
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	before := dbfile.ReadStats().EvictedBudget
	srv.SetFDBudget(2)
	srv.janitorTick(time.Hour)

	abs := func(p string) string { a, _ := filepath.Abs(p); return a }
	want := []string{abs(w.CheckoutPath("app", "b")), abs(mainPath)}
	sort.Strings(want)
	if got := cachedUnder(filepath.Dir(mainPath)); !reflect.DeepEqual(got, want) {
		t.Fatalf("cached checkouts after a budget-2 pass = %v, want %v (the pinned session plus the most recent)", got, want)
	}
	if got := dbfile.ReadStats().EvictedBudget - before; got < 2 {
		t.Fatalf("budget evictions = %d, want >= 2", got)
	}

	// The pinned session kept capturing: a write still becomes durable.
	if out, err := exec.Command("sqlite3", open.Checkout,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	flushed := false
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end) && !flushed; {
		resp := call(t, sock, Request{Op: "flush", DB: "app", Branch: "main"})
		ref, _, err := w.Store.GetRef("app", "main")
		flushed = resp.OK && err == nil && ref.HeadTXID == resp.TXID && resp.TXID > 1
		if !flushed {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !flushed {
		t.Fatal("the session could not flush after the budget pass")
	}

	var buf bytes.Buffer
	if err := srv.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"# TYPE offshoot_dbfile_descriptors gauge",
		"# TYPE offshoot_dbfile_pins gauge",
		"# TYPE offshoot_dbfile_stranded_pinned gauge",
		"# TYPE offshoot_dbfile_evicted_total counter",
		`offshoot_dbfile_evicted_total{reason="stranded"}`,
		`offshoot_dbfile_evicted_total{reason="budget"}`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition lacks %q", want)
		}
	}
}

// TestJanitorBudgetZeroIsUnlimited: -fd-budget 0 runs no budget pass, but
// stranded reclaim still runs.
func TestJanitorBudgetZeroIsUnlimited(t *testing.T) {
	srv, w := newServer(t)
	for _, b := range []string{"a", "b", "gone"} {
		if _, err := w.Fork("app", "main", b, "", 0, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Checkout("app", b); err != nil {
			t.Fatal(err)
		}
	}
	// A checkout file removed behind ops' back, the way evicting the
	// read-only cache removes files: only the janitor's full sweep finds it.
	gone, _ := filepath.Abs(w.CheckoutPath("app", "gone"))
	if got := cachedUnder(filepath.Dir(gone)); !slices.Contains(got, gone) {
		t.Fatalf("cached checkouts = %v, want %s among them", got, gone)
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	srv.SetFDBudget(0)
	before := dbfile.ReadStats()
	srv.janitorTick(time.Hour)
	after := dbfile.ReadStats()
	if after.EvictedBudget != before.EvictedBudget {
		t.Fatalf("budget 0 evicted %d descriptor(s)", after.EvictedBudget-before.EvictedBudget)
	}
	if after.EvictedStranded == before.EvictedStranded {
		t.Fatal("budget 0 also skipped stranded reclaim")
	}
	for _, e := range dbfile.Entries() {
		if e.Path == gone {
			t.Fatalf("the removed checkout's descriptor is still held: %+v", e)
		}
	}
	cached := cachedUnder(filepath.Dir(w.CheckoutPath("app", "a")))
	for _, b := range []string{"a", "b"} {
		p, _ := filepath.Abs(w.CheckoutPath("app", b))
		if !slices.Contains(cached, p) {
			t.Fatalf("cached checkouts = %v, want %s still cached", cached, p)
		}
	}
}

// TestStatusReportsDescriptors: the status op carries dbfile's descriptor
// count, and only status does. dbfile is process-wide, so a bare ">= 1"
// would pass on descriptors earlier tests left behind; instead the count
// must match dbfile's own, and the open session's checkout must be one of
// the descriptors counted.
func TestStatusReportsDescriptors(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()
	open := call(t, sock, Request{Op: "open", DB: "app", Branch: "main"})
	if !open.OK {
		t.Fatalf("open = %+v", open)
	}
	co, _ := filepath.Abs(open.Checkout)
	deadline := time.Now().Add(10 * time.Second)
	for !slices.Contains(cachedUnder(filepath.Dir(co)), co) {
		if time.Now().After(deadline) {
			t.Fatalf("the session's checkout %s never got a dbfile descriptor; entries: %+v", co, dbfile.Entries())
		}
		time.Sleep(10 * time.Millisecond)
	}

	lo := dbfile.ReadStats().Descriptors()
	resp := call(t, sock, Request{Op: "status"})
	hi := dbfile.ReadStats().Descriptors()
	if !resp.OK || resp.DBFileDescriptors == nil {
		t.Fatalf("status carries no dbfile_descriptors: %+v", resp)
	}
	if n := *resp.DBFileDescriptors; n < 1 || n < min(lo, hi) || n > max(lo, hi) {
		t.Fatalf("dbfile_descriptors = %d, want dbfile's own count (%d before the call, %d after)", n, lo, hi)
	}
	b, _ := json.Marshal(resp)
	if !strings.Contains(string(b), `"dbfile_descriptors":`) {
		t.Fatalf("wire name changed: %s", b)
	}
	if dbs := call(t, sock, Request{Op: "dbs"}); dbs.DBFileDescriptors != nil {
		t.Fatal("dbfile_descriptors leaked into a non-status op")
	}
}
