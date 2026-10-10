package ops

import (
	"io"
	"os"
)

// copyFile is the plain byte copy the clone, export and materialize tests
// use to stage a database file under a new path. Test-only: production
// code reaches every copy through the reflink/clone paths in
// internal/reflink and never calls this.
func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
