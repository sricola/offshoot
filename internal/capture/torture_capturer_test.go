//go:build torture

package capture

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/replay"
	"github.com/sricola/offshoot/internal/testutil"
)

// TestTortureCapturerKill is the harness TestTortureWriterKill does not
// have: the CAPTURER is SIGKILLed, not bounced through a graceful context
// cancel. The capture engine runs in a child process (this test binary
// re-executed as TestTortureCapturerHelper) against a stock sqlite3 CLI
// writer; every round the child is killed with SIGKILL at a random moment
// mid-traffic, a fresh child is started on the same state directory and
// replica (so tryResume sees whatever the kill left behind: a torn
// replica write, a stale state file, an unflushed consumed offset), more
// writes land, and the child is then asked to drain and exit cleanly
// (SIGTERM). After every round the replica's `.dump` must equal the
// source's. A resumed child that re-applies or skips a transaction, or a
// child that resumes where it should have rebased, diverges here.
//
// What it still does not cover: power loss (the kernel keeps every write
// the child issued), and killing the WRITER at the same instant (the
// writer harness covers that separately).
func TestTortureCapturerKill(t *testing.T) {
	testutil.RequireSQLite3(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	replica := filepath.Join(dir, "replica.db")
	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB, n INTEGER);").CombinedOutput(); err != nil {
		t.Fatalf("init: %v: %s", err, out)
	}

	dur := 2 * time.Minute
	if s := os.Getenv("OFFSHOOT_TORTURE_CAPTURER_SECONDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("OFFSHOOT_TORTURE_CAPTURER_SECONDS=%q: %v", s, err)
		}
		dur = time.Duration(n) * time.Second
	}
	deadline := time.Now().Add(dur)

	write := func() {
		t.Helper()
		if out, err := exec.Command("sqlite3", src, writerSQL).CombinedOutput(); err != nil {
			t.Fatalf("writer: %v: %s", err, out)
		}
	}

	var rounds, kills, resumed, rebased, startsAfterKill int
	for time.Now().Before(deadline) {
		rounds++
		// 1. A child captures while the writer works; kill it mid-traffic.
		c := startHelper(t, src, state, replica)
		write()
		time.Sleep(time.Duration(rand.Intn(150)) * time.Millisecond)
		write()
		c.kill()
		kills++

		// 2. A fresh child inherits the state dir and the (possibly torn)
		// replica, decides resume vs rebase, and captures more writes.
		c = startHelper(t, src, state, replica)
		startsAfterKill++
		write()
		write()
		// 3. Graceful drain + exit, then the dumps must match exactly.
		c.cmd.Process.Signal(syscall.SIGTERM)
		rep := c.waitExit(t, 30*time.Second)
		if rep.resumed {
			resumed++
		}
		rebased += rep.rebased
		sd, e1 := replay.Dump(src)
		rd, e2 := replay.Dump(replica)
		if e1 != nil || e2 != nil {
			t.Fatalf("round %d: dump: src=%v replica=%v", rounds, e1, e2)
		}
		if sd != rd {
			t.Fatalf("round %d: replica diverged from source after a capturer SIGKILL + restart (child resumed=%v rebased=%d)", rounds, rep.resumed, rep.rebased)
		}
	}
	t.Logf("capturer torture complete: %d rounds, %d capturer SIGKILLs, %d post-kill starts of which %d resumed and %d rebased (a rebase after a kill is the expected safe path; a resume is only correct when the kill left a clean state), zero divergence",
		rounds, kills, startsAfterKill, resumed, rebased)
	if kills < 3 {
		t.Fatalf("only %d rounds ran; the harness needs several to mean anything", kills)
	}
}

type helperProc struct {
	cmd      *exec.Cmd
	ready    chan struct{}
	scanDone chan struct{}
	report   helperReport
}

type helperReport struct {
	resumed bool
	rebased int
}

// startHelper re-executes this test binary as TestTortureCapturerHelper on
// the given paths and waits for its "ready" line (the engine's startup
// resume-or-rebase verdict has settled and it is tailing the WAL). A
// goroutine owns the child's stdout for the child's whole life and parses
// the two lines the parent cares about, so no caller ever blocks on a
// channel the child's exit may already have closed.
func startHelper(t *testing.T, src, state, replica string) *helperProc {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestTortureCapturerHelper$", "-test.v")
	cmd.Env = append(os.Environ(),
		"OFFSHOOT_TORTURE_HELPER=1",
		"OFFSHOOT_TORTURE_SRC="+src,
		"OFFSHOOT_TORTURE_STATE="+state,
		"OFFSHOOT_TORTURE_REPLICA="+replica,
	)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	h := &helperProc{cmd: cmd, ready: make(chan struct{}), scanDone: make(chan struct{})}
	go func() {
		defer close(h.scanDone)
		readyClosed := false
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			l := sc.Text()
			switch {
			case l == "helper: ready" && !readyClosed:
				readyClosed = true
				close(h.ready)
			case strings.HasPrefix(l, "helper: exit "):
				var r, b string
				fmt.Sscanf(strings.TrimPrefix(l, "helper: exit "), "resumed=%s rebased=%s", &r, &b)
				h.report.resumed = r == "true"
				h.report.rebased, _ = strconv.Atoi(b)
			}
		}
	}()
	select {
	case <-h.ready:
	case <-h.scanDone:
		cmd.Wait()
		t.Fatalf("helper exited before reporting ready")
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("helper did not become ready within 30s")
	}
	return h
}

// kill SIGKILLs the child and reaps it; the scanner goroutine ends on its
// own when the child's stdout closes.
func (h *helperProc) kill() {
	h.cmd.Process.Signal(syscall.SIGKILL)
	h.cmd.Wait()
	<-h.scanDone
}

// waitExit collects the helper's exit report after a SIGTERM.
func (h *helperProc) waitExit(t *testing.T, timeout time.Duration) helperReport {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.cmd.Wait() }()
	select {
	case err := <-done:
		<-h.scanDone
		if err != nil {
			t.Fatalf("helper exited with error after SIGTERM: %v", err)
		}
		return h.report
	case <-time.After(timeout):
		h.cmd.Process.Kill()
		<-done
		t.Fatalf("helper did not exit within %s of SIGTERM", timeout)
	}
	return h.report
}

// TestTortureCapturerHelper is the child process of TestTortureCapturerKill:
// it runs the capture engine on the paths in the environment until SIGTERM,
// then drains and exits. It skips when run directly.
func TestTortureCapturerHelper(t *testing.T) {
	if os.Getenv("OFFSHOOT_TORTURE_HELPER") != "1" {
		t.Skip("child process of TestTortureCapturerKill")
	}
	src := os.Getenv("OFFSHOOT_TORTURE_SRC")
	state := os.Getenv("OFFSHOOT_TORTURE_STATE")
	replica := os.Getenv("OFFSHOOT_TORTURE_REPLICA")
	rep := replay.New(replica)
	e := NewEngine(Options{DBPath: src, StateDir: state, Sink: tortureSink{rep}})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
	defer rcancel()
	if err := e.WaitReady(rctx); err != nil {
		fmt.Println("helper: not ready:", err)
		os.Exit(2)
	}
	fmt.Println("helper: ready")
	if err := <-done; err != nil {
		fmt.Println("helper: run error:", err)
		os.Exit(3)
	}
	fmt.Printf("helper: exit resumed=%v rebased=%d\n", e.Resumed(), e.Rebased())
}
