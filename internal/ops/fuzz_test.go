package ops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// FuzzReadSidecar feeds arbitrary bytes to readSidecar as a checkout's .sum
// sidecar. The property: never panic; either ok=false with a zero record
// (the "unknown" bucket every caller treats as a miss) or ok=true with a
// non-empty Hash from valid JSON, and the record is a fixed point of the
// format — marshaled and read back, it is the same record.
func FuzzReadSidecar(f *testing.F) {
	// A real sidecar, stamped by the production writer over a real file.
	dir := f.TempDir()
	path := filepath.Join(dir, "seed.sqlite")
	if err := os.WriteFile(path, make([]byte, 4096), 0o644); err != nil {
		f.Fatal(err)
	}
	if err := writeSum(path, "0123456789abcdef0123456789abcdef", 3, 7, 0x8000000000001234, "chain-id"); err != nil {
		f.Fatal(err)
	}
	real, err := os.ReadFile(path + ".sum")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(real)
	if err := StampSumHashOnly(path, "deadbeef", "lineage", 1, 2, 0, ""); err != nil {
		f.Fatal(err)
	}
	if hashOnly, err := os.ReadFile(path + ".sum"); err == nil {
		f.Add(hashOnly)
	}
	for _, s := range []string{
		"", "{}", `{"hash":""}`, "e3b0c44298fc1c149afbf4c8996fb924", // legacy bare hash
		`{"hash":"x","epoch":-1}`, `{"hash":"x","txid":1e30}`, `{"hash":"x","shadow":"yes"}`,
		`{"hash":"x","chain":[]}`, `{"hash":"x","chain":["data/l/1/snapshot-0000000000000001.ltx"]}`, `{"hash":"x","chain":"k"}`,
		`{"hash":"x","mtime_ns":9223372036854775807,"stamped_ns":-9223372036854775808}`,
		`[1,2,3]`, `{"hash":"x"} trailing`, "\xff\xfe",
	} {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		p := filepath.Join(t.TempDir(), "c.sqlite")
		if err := os.WriteFile(p+".sum", data, 0o644); err != nil {
			t.Fatal(err)
		}
		rec, ok := readSidecar(p)
		if !ok {
			if !reflect.DeepEqual(rec, sumRecord{}) {
				t.Fatalf("ok=false with a non-zero record %+v", rec)
			}
			return
		}
		if rec.Hash == "" {
			t.Fatal("ok=true with an empty hash")
		}
		if !json.Valid(data) {
			t.Fatal("ok=true for bytes that are not valid JSON")
		}
		again, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p+".sum", again, 0o644); err != nil {
			t.Fatal(err)
		}
		rec2, ok2 := readSidecar(p)
		if !ok2 || !reflect.DeepEqual(rec2, rec) {
			t.Fatalf("record is not a fixed point of the format: %+v -> %s -> %+v (ok=%v)", rec, again, rec2, ok2)
		}
	})
}
