// Command bench-ab compares branchbench between a committed git ref (the
// "before" side, usually the latest release tag) and whatever is checked out
// in the current working tree (the "after" side, uncommitted changes
// included). It exists because a per-checkpoint regression shipped in
// v0.2.16 and was found three releases later by building cmd/branchbench
// from each tag and alternating runs by hand; this tool is that procedure.
//
// It builds cmd/branchbench twice — once from `git archive <before>`,
// extracted into the out directory and built against that tree's own go.mod,
// and once from the working tree — then runs the two binaries alternately,
// round by round, on the same machine, so drift during the series (thermal
// state, background load, page cache) moves both sides together.
//
// For each workflow it compares wall time and the depth-1 p50 of fork,
// checkout, checkpoint and eval. A metric regressed when the after median
// exceeds the before median by more than -threshold AND the smallest after
// sample exceeds the largest before sample; the mirrored rule labels a metric
// improved. Any regression makes the exit status 1.
//
// The default workflows are failure_repro at concurrency 1 and simulation at
// concurrency 8. failure_repro at concurrency 1 is the sensitive one: with a
// single worker nothing contends with a step, so a per-checkpoint cost shows
// up in the checkpoint p50 undiluted. simulation at concurrency 8 covers the
// contended path.
//
// Usage:
//
//	go run ./cmd/bench-ab -before v0.2.19
//	go run ./cmd/bench-ab -before HEAD -rounds 2 -workflows failure_repro:1
package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// spec is one item of -workflows: a branchbench workflow and the
// concurrency to run it at.
type spec struct {
	Name        string
	Concurrency int
}

// metric names one figure of a sample and how to read it.
type metric struct {
	Name string
	Get  func(sample) float64
}

var metrics = []metric{
	{"wall (s)", func(s sample) float64 { return s.WallS }},
	{"fork p50 (ms)", func(s sample) float64 { return s.ForkP50 }},
	{"checkout p50 (ms)", func(s sample) float64 { return s.CheckoutP50 }},
	{"checkpoint p50 (ms)", func(s sample) float64 { return s.CheckpointP50 }},
	{"eval p50 (ms)", func(s sample) float64 { return s.EvalP50 }},
}

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if code == 0 {
			code = 2
		}
	}
	os.Exit(code)
}

func run() (int, error) {
	before := flag.String("before", "", "git ref to compare the working tree against (required; empty means nothing to compare)")
	rounds := flag.Int("rounds", 3, "alternating rounds per workflow; each round runs before then after")
	threshold := flag.Float64("threshold", 0.15, "fraction by which the after median must exceed the before median to count as a regression")
	workflowsFlag := flag.String("workflows", "failure_repro:1,simulation:8", "comma-separated <workflow>:<concurrency> items")
	outFlag := flag.String("out", "", "directory for the builds, raw outputs and report.md (default: a new temp dir)")
	summary := flag.String("summary", os.Getenv("GITHUB_STEP_SUMMARY"), "file to append the Markdown report to (default $GITHUB_STEP_SUMMARY)")
	flag.Parse()

	if *before == "" {
		fmt.Println("bench-ab: no before ref given (no tag to compare against); nothing to do")
		return 0, nil
	}
	if *rounds < 2 {
		return 2, fmt.Errorf("bench-ab: -rounds must be at least 2, got %d", *rounds)
	}
	if *threshold <= 0 || *threshold >= 1 {
		return 2, fmt.Errorf("bench-ab: -threshold must be between 0 and 1, got %v", *threshold)
	}
	specs, err := parseSpecs(*workflowsFlag)
	if err != nil {
		return 2, err
	}

	commit, err := gitOutput("rev-parse", "--verify", *before+"^{commit}")
	if err != nil {
		return 2, fmt.Errorf("bench-ab: %q is not a commit: %w", *before, err)
	}
	beforeShort, err := gitOutput("rev-parse", "--short", commit)
	if err != nil {
		return 2, err
	}
	afterShort, err := gitOutput("rev-parse", "--short", "HEAD")
	if err != nil {
		return 2, err
	}
	if porcelain, err := gitOutput("status", "--porcelain"); err != nil {
		return 2, err
	} else if porcelain != "" {
		afterShort += "-dirty"
	}

	// The out directory is created after the dirty check, so an -out inside
	// the working tree does not mark it dirty.
	out := *outFlag
	if out == "" {
		if out, err = os.MkdirTemp("", "bench-ab-"); err != nil {
			return 2, err
		}
	} else if err := os.MkdirAll(out, 0o755); err != nil {
		return 2, err
	}
	if out, err = filepath.Abs(out); err != nil {
		return 2, err
	}

	srcBefore := filepath.Join(out, "src-before")
	if err := os.RemoveAll(srcBefore); err != nil {
		return 2, err
	}
	if err := os.Mkdir(srcBefore, 0o755); err != nil {
		return 2, err
	}
	fmt.Fprintf(os.Stderr, "bench-ab: extracting %s (%s) into %s\n", *before, beforeShort, srcBefore)
	if err := gitArchive(commit, srcBefore); err != nil {
		return 2, err
	}
	bins := map[string]string{
		"before": filepath.Join(out, "bb-before"),
		"after":  filepath.Join(out, "bb-after"),
	}
	fmt.Fprintln(os.Stderr, "bench-ab: building the before branchbench")
	if err := goBuild(srcBefore, bins["before"]); err != nil {
		return 2, err
	}
	fmt.Fprintln(os.Stderr, "bench-ab: building the after branchbench from the working tree")
	if err := goBuild("", bins["after"]); err != nil {
		return 2, err
	}

	loadBefore := loadAverage()
	// samples[workflow][side] holds one sample per round.
	samples := map[string]map[string][]sample{}
	for _, sp := range specs {
		samples[sp.Name] = map[string][]sample{}
		for round := 1; round <= *rounds; round++ {
			for _, side := range []string{"before", "after"} {
				fmt.Fprintf(os.Stderr, "bench-ab: %s c=%d round %d/%d %s\n", sp.Name, sp.Concurrency, round, *rounds, side)
				s, err := runBench(bins[side], sp, out, side, round)
				if err != nil {
					return 2, err
				}
				samples[sp.Name][side] = append(samples[sp.Name][side], s)
			}
		}
	}
	loadAfter := loadAverage()

	var b strings.Builder
	fmt.Fprintf(&b, "### bench-ab: before `%s` (%s) vs after %s\n\n", *before, beforeShort, afterShort)
	fmt.Fprintf(&b, "%d alternating rounds per workflow on one machine, threshold %.0f%%. "+
		"A metric regressed when the after median exceeds the before median by more than the threshold "+
		"and every after run is slower than every before run; improved is the mirror of that rule.\n\n",
		*rounds, *threshold*100)
	fmt.Fprintf(&b, "Load average before the series: %s\n\n", loadBefore)
	b.WriteString("| workflow | metric | before (rounds) | after (rounds) | before median | after median | change | verdict |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	anyRegressed := false
	for _, sp := range specs {
		for _, m := range metrics {
			bv := values(samples[sp.Name]["before"], m)
			av := values(samples[sp.Name]["after"], m)
			res, err := compareRounds(bv, av, *threshold)
			if err != nil {
				return 2, err
			}
			v := "ok"
			switch {
			case res.Regressed:
				v = "regressed"
				anyRegressed = true
			case res.Improved:
				v = "improved"
			}
			fmt.Fprintf(&b, "| %s:%d | %s | %s | %s | %.1f | %.1f | %s | %s |\n",
				sp.Name, sp.Concurrency, m.Name, join(bv), join(av), res.BeforeMedian, res.AfterMedian,
				change(res.BeforeMedian, res.AfterMedian), v)
		}
	}
	fmt.Fprintf(&b, "\nLoad average after the series: %s\n", loadAfter)
	report := b.String()

	fmt.Print(report)
	if err := os.WriteFile(filepath.Join(out, "report.md"), []byte(report), 0o644); err != nil {
		return 2, err
	}
	if *summary != "" {
		f, err := os.OpenFile(*summary, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return 2, err
		}
		_, werr := f.WriteString(report + "\n")
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return 2, werr
		}
	}
	fmt.Printf("\nbench-ab: raw outputs and report.md are in %s\n", out)
	if anyRegressed {
		return 1, nil
	}
	return 0, nil
}

// parseSpecs parses the -workflows flag.
func parseSpecs(s string) ([]spec, error) {
	var specs []spec
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, conc, ok := strings.Cut(item, ":")
		if !ok || name == "" {
			return nil, fmt.Errorf("bench-ab: workflow item %q is not <workflow>:<concurrency>", item)
		}
		c, err := strconv.Atoi(conc)
		if err != nil || c < 1 {
			return nil, fmt.Errorf("bench-ab: workflow item %q has a concurrency that is not a positive integer", item)
		}
		// Samples and raw files are keyed by workflow name, so a name may
		// appear once.
		for _, prev := range specs {
			if prev.Name == name {
				return nil, fmt.Errorf("bench-ab: workflow %q appears more than once in -workflows", name)
			}
		}
		specs = append(specs, spec{Name: name, Concurrency: c})
	}
	if len(specs) == 0 {
		return nil, errors.New("bench-ab: -workflows names no workflow")
	}
	return specs, nil
}

// gitOutput runs git in the current directory and returns its trimmed
// stdout, folding stderr into the error.
func gitOutput(args ...string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

// gitArchive extracts the tree of commit into dest with Go's archive/tar, so
// the tool needs no tar binary. Only directories and regular files are
// created; every other entry type (the pax global header git writes, and
// symlinks, of which the repository has none) is skipped. An entry whose
// cleaned path leaves dest is an error.
func gitArchive(commit, dest string) error {
	var stderr bytes.Buffer
	cmd := exec.Command("git", "archive", "--format=tar", commit)
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	extractErr := extractTar(stdout, dest)
	// Drain what is left so git does not block on a full pipe when the
	// extraction stopped early.
	_, _ = io.Copy(io.Discard, stdout)
	waitErr := cmd.Wait()
	if extractErr != nil {
		return extractErr
	}
	if waitErr != nil {
		return fmt.Errorf("git archive %s: %w: %s", commit, waitErr, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// extractTar writes the directories and regular files of the tar stream r
// under dest, keeping each entry's permission bits.
func extractTar(r io.Reader, dest string) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("bench-ab: reading git archive: %w", err)
		}
		target := filepath.Join(dest, hdr.Name)
		rel, err := filepath.Rel(dest, target)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(hdr.Name) {
			return fmt.Errorf("bench-ab: archive entry %q escapes %s", hdr.Name, dest)
		}
		mode := os.FileMode(hdr.Mode).Perm()
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, mode|0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
			if err != nil {
				return err
			}
			_, cerr := io.Copy(f, tr)
			if err := f.Close(); cerr == nil {
				cerr = err
			}
			if cerr != nil {
				return cerr
			}
		}
	}
}

// goBuild builds ./cmd/branchbench into the absolute path bin, in dir (the
// current directory when dir is empty), so the build uses that tree's go.mod.
func goBuild(dir, bin string) error {
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/branchbench")
	cmd.Dir = dir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		where := dir
		if where == "" {
			where = "the working tree"
		}
		return fmt.Errorf("bench-ab: go build ./cmd/branchbench in %s: %w", where, err)
	}
	return nil
}

// runBench runs one branchbench and returns its table row for sp. The run's
// stdout, then a "--- stderr ---" line, then its stderr, are kept in
// raw-<workflow>-<side>-<round>.txt under out, so a failed run or a parse
// failure can be diagnosed from the artifact.
func runBench(bin string, sp spec, out, side string, round int) (sample, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(bin, "-workflows", sp.Name, "-concurrency", strconv.Itoa(sp.Concurrency))
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	raw := filepath.Join(out, fmt.Sprintf("raw-%s-%s-%d.txt", sp.Name, side, round))
	content := stdout.String() + "\n--- stderr ---\n" + stderr.String()
	if err := os.WriteFile(raw, []byte(content), 0o644); err != nil {
		return sample{}, err
	}
	if runErr != nil {
		return sample{}, fmt.Errorf("bench-ab: %s branchbench %s round %d failed: %w (see %s)", side, sp.Name, round, runErr, raw)
	}
	s, err := findRow(stdout.String(), sp.Name)
	if err != nil {
		return sample{}, fmt.Errorf("%w (see %s)", err, raw)
	}
	return s, nil
}

// loadAverage returns the 1, 5 and 15 minute load averages as text, or a
// note that they are unavailable. It is best effort and never fails.
func loadAverage() string {
	switch runtime.GOOS {
	case "linux":
		b, err := os.ReadFile("/proc/loadavg")
		if err == nil {
			if f := strings.Fields(string(b)); len(f) >= 3 {
				return strings.Join(f[:3], " ")
			}
		}
	case "darwin":
		b, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
		if err == nil {
			s := strings.Trim(strings.TrimSpace(string(b)), "{}")
			return strings.Join(strings.Fields(s), " ")
		}
	}
	return "unavailable"
}

// values reads one metric from each sample.
func values(ss []sample, m metric) []float64 {
	vs := make([]float64, len(ss))
	for i, s := range ss {
		vs[i] = m.Get(s)
	}
	return vs
}

// join renders samples as "1.7, 1.8, 1.7".
func join(vs []float64) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = strconv.FormatFloat(v, 'f', 1, 64)
	}
	return strings.Join(parts, ", ")
}

// change renders (after/before - 1) * 100 with one decimal and a sign.
func change(before, after float64) string {
	if before == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%+.1f%%", (after/before-1)*100)
}
