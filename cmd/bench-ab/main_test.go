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

func TestRunOrderAlternates(t *testing.T) {
	want := map[int][2]string{
		1: {"before", "after"},
		2: {"after", "before"},
		3: {"before", "after"},
		4: {"after", "before"},
	}
	first := map[string]int{}
	for round := 1; round <= 4; round++ {
		got := runOrder(round)
		if len(got) != 2 || got[0] != want[round][0] || got[1] != want[round][1] {
			t.Fatalf("round %d order %v, want %v", round, got, want[round])
		}
		first[got[0]]++
	}
	if first["before"] != first["after"] {
		t.Fatalf("four rounds are unbalanced: %v", first)
	}
}

func TestTailLines(t *testing.T) {
	if got := tailLines("a\nb\nc\n", 2); got != "b\nc" {
		t.Fatalf("tailLines = %q", got)
	}
	if got := tailLines("a\n", 20); got != "a" {
		t.Fatalf("tailLines short = %q", got)
	}
}

func TestOnlyGatingMetricsDecide(t *testing.T) {
	// wall and eval look regressed (every after run well above every before
	// run, past the 15% threshold); fork, checkout and checkpoint are
	// identical before and after. Only the gating metrics may set
	// anyRegressed, so the overall result must be false, and every
	// non-gating row must read "info" rather than "regressed".
	before := []sample{
		{WallS: 45, ForkP50: 10, CheckoutP50: 20, CheckpointP50: 30, EvalP50: 5},
		{WallS: 45, ForkP50: 10, CheckoutP50: 20, CheckpointP50: 30, EvalP50: 5},
	}
	after := []sample{
		{WallS: 60, ForkP50: 10, CheckoutP50: 20, CheckpointP50: 30, EvalP50: 7},
		{WallS: 60, ForkP50: 10, CheckoutP50: 20, CheckpointP50: 30, EvalP50: 7},
	}
	rows, anyRegressed, err := evaluateMetrics(metrics, before, after, 0.15)
	if err != nil {
		t.Fatal(err)
	}
	if anyRegressed {
		t.Fatal("a non-gating regression (wall, eval) set anyRegressed")
	}
	for _, row := range rows {
		if !row.Metric.Gating && row.Verdict != "info" {
			t.Fatalf("%s: non-gating verdict = %q, want info", row.Metric.Name, row.Verdict)
		}
		if row.Metric.Gating && row.Verdict != "ok" {
			t.Fatalf("%s: gating verdict = %q, want ok", row.Metric.Name, row.Verdict)
		}
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
