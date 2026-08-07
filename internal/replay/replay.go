// Package replay reconstructs a SQLite database from a base snapshot plus
// captured WAL transactions.
package replay

import (
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/sricola/offshoot/internal/wal"
)

type Replica struct {
	path string
}

func New(path string) *Replica       { return &Replica{path: path} }
func (r *Replica) Path() string      { return r.path }

func (r *Replica) Rebase(snapshotPath string) error {
	in, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(r.path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	// A rebased replica must not carry stale sidecar files.
	os.Remove(r.path + "-wal")
	os.Remove(r.path + "-shm")
	return out.Close()
}

func (r *Replica) Apply(pageSize uint32, frames []wal.Frame) error {
	if len(frames) == 0 {
		return fmt.Errorf("replay: empty transaction")
	}
	last := frames[len(frames)-1]
	if last.Header.CommitSize == 0 {
		return fmt.Errorf("replay: transaction does not end in a commit frame")
	}
	f, err := os.OpenFile(r.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, fr := range frames {
		off := int64(fr.Header.Pgno-1) * int64(pageSize)
		if _, err := f.WriteAt(fr.Data, off); err != nil {
			return err
		}
	}
	if err := f.Truncate(int64(last.Header.CommitSize) * int64(pageSize)); err != nil {
		return err
	}
	return f.Sync()
}

func Dump(dbPath string) (string, error) {
	out, err := exec.Command("sqlite3", dbPath, ".dump").Output()
	if err != nil {
		return "", fmt.Errorf("sqlite3 .dump %s: %w", dbPath, err)
	}
	return string(out), nil
}
