// Command torture runs the offshoot capture engine against stock sqlite3 CLI
// writers under random kill -9, verifying dump equivalence every round.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/offshoot-db/offshoot/internal/capture"
	"github.com/offshoot-db/offshoot/internal/replay"
	"github.com/offshoot-db/offshoot/internal/wal"
)

const writerSQL = `PRAGMA busy_timeout=5000;
BEGIN; INSERT INTO t (v, n) VALUES (randomblob(200), 1); COMMIT;
BEGIN; UPDATE t SET n = n + 1 WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 5); COMMIT;
BEGIN; INSERT INTO t (v, n) SELECT randomblob(100), 0 FROM t LIMIT 3; COMMIT;
BEGIN; DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY random() LIMIT 1); COMMIT;`

type sink struct{ r *replay.Replica }

func (s sink) Rebase(p string) error                { return s.r.Rebase(p) }
func (s sink) Apply(ps uint32, f []wal.Frame) error { return s.r.Apply(ps, f) }

func main() {
	dur := flag.Duration("d", 10*time.Minute, "total duration")
	dir := flag.String("dir", "", "work dir (default: temp)")
	flag.Parse()

	if *dir == "" {
		d, err := os.MkdirTemp("", "offshoot-torture-*")
		if err != nil {
			log.Fatal(err)
		}
		*dir = d
	}
	src := filepath.Join(*dir, "src.db")
	if out, err := exec.Command("sqlite3", src,
		"PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v BLOB, n INTEGER);").CombinedOutput(); err != nil {
		log.Fatalf("init: %v: %s", err, out)
	}

	rep := replay.New(filepath.Join(*dir, "replica.db"))
	e := capture.NewEngine(capture.Options{DBPath: src, StateDir: *dir, Sink: sink{rep}})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := e.Run(ctx); err != nil {
			log.Fatalf("engine: %v", err)
		}
	}()
	defer cancel()

	deadline := time.Now().Add(*dur)
	rounds, kills := 0, 0
	for time.Now().Before(deadline) {
		rounds++
		cmd := exec.Command("sqlite3", src, writerSQL)
		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}
		if rand.Intn(2) == 0 {
			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			cmd.Process.Signal(syscall.SIGKILL)
			kills++
		}
		cmd.Wait()
		if !converge(src, rep, 15*time.Second) {
			sd, _ := replay.Dump(src)
			rd, _ := replay.Dump(rep.Path())
			os.WriteFile(filepath.Join(*dir, "source.dump"), []byte(sd), 0o644)
			os.WriteFile(filepath.Join(*dir, "replica.dump"), []byte(rd), 0o644)
			log.Fatalf("DIVERGED at round %d (kills=%d rebases=%d); dumps in %s",
				rounds, kills, e.Rebased(), *dir)
		}
		if rounds%50 == 0 {
			fmt.Printf("round %d ok (kills=%d rebases=%d)\n", rounds, kills, e.Rebased())
		}
	}
	fmt.Printf("PASS: %d rounds, %d kills, %d rebases, dir=%s\n", rounds, kills, e.Rebased(), *dir)
}

func converge(src string, rep *replay.Replica, d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		sd, e1 := replay.Dump(src)
		rd, e2 := replay.Dump(rep.Path())
		if e1 == nil && e2 == nil && sd == rd {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
