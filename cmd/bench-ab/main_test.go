package main

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tarOf(t *testing.T, entries ...*tar.Header) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range entries {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := tw.Write(make([]byte, h.Size)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func TestExtractTarKeepsModesAndRefusesEscapes(t *testing.T) {
	dest := t.TempDir()
	buf := tarOf(t,
		&tar.Header{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o755},
		&tar.Header{Name: "d/run.sh", Typeflag: tar.TypeReg, Mode: 0o755, Size: 3},
		&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"},
	)
	if err := extractTar(buf, dest); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(dest, "d", "run.sh"))
	if err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("run.sh: %v %v", fi, err)
	}
	if _, err := os.Lstat(filepath.Join(dest, "link")); !os.IsNotExist(err) {
		t.Fatalf("symlink entry was created: %v", err)
	}

	escape := tarOf(t, &tar.Header{Name: "../evil", Typeflag: tar.TypeReg, Mode: 0o644, Size: 1})
	if err := extractTar(escape, dest); err == nil {
		t.Fatal("an entry escaping the destination was extracted")
	}
}

func TestParseSpecs(t *testing.T) {
	specs, err := parseSpecs("failure_repro:1,simulation:8")
	if err != nil || len(specs) != 2 || specs[1] != (spec{"simulation", 8}) {
		t.Fatalf("parseSpecs: %+v %v", specs, err)
	}
	for _, bad := range []string{"", "failure_repro", "failure_repro:0", "x:y", "a:1,a:2"} {
		if _, err := parseSpecs(bad); err == nil {
			t.Fatalf("parseSpecs(%q) accepted", bad)
		}
	}
}
