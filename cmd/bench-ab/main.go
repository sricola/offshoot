// Command bench-ab compares branchbench between a committed git ref (the
// "before" side, usually the latest release tag) and whatever is checked out
// in the current working tree (the "after" side, uncommitted changes
// included). It exists because a per-checkpoint regression shipped in
// v0.2.16 and was found three releases later by building cmd/branchbench
// from each tag and alternating runs by hand; this tool is that procedure.
//
// It builds cmd/branchbench twice — once from `git archive <before>`,
// extracted into the out directory and built against that tree's own go.mod,
// and once from the module root of the working tree — then runs the two
// binaries alternately, round by round, on the same machine, so drift during
// the series (thermal state, background load, page cache) moves both sides
// together. The tool may be started from any directory inside the module.
//
// Before the measured rounds of each workflow, one warm-up run of the after
// binary is made and discarded, so no measured run directly follows the two
// CPU-heavy builds. The order inside a round alternates: odd rounds run
// before then after, even rounds after then before, because the second run
// of a pair was observed to be a few percent faster on identical code. The
// default of 4 rounds balances the two orders; an odd -rounds leaves one
// round unbalanced. Every branchbench run gets -timeout set to -run-timeout;
// one minute after that, if it has not exited, it is sent SIGTERM, and if it
// still has not exited 30s after that, it is killed outright.
//
// For each workflow it compares wall time and the depth-1 p50 of fork,
// checkout, checkpoint and eval, but only fork, checkout and checkpoint p50
// gate the verdict (and so the exit code): a metric regressed when the after
// median exceeds the before median by more than -threshold AND the smallest
// after sample exceeds the largest before sample; the mirrored rule labels a
// metric improved. wall and eval are informational only and always report
// "info", never regressed or improved, because the wall cell has 0.1 s
// resolution (coarse for short workflows) and eval times the workload's own
// query rather than offshoot's.
//
// Exit status, of the built binary: 0 when no -before ref is given or
// nothing regressed; 1 when at least one gating metric regressed; 2 on bad
// flags, an unknown ref, a build failure, or a branchbench run that failed,
// timed out or printed no parsable row. `go run` folds any non-zero status
// into its own exit 1 (printing "exit status N"), and make folds a failing
// recipe into make's exit 2, so make bench-ab and the nightly build the
// binary first and run that (bin/bench-ab, $RUNNER_TEMP/bench-ab). Under
// make, a regression still shows as make's exit 2; the report tells a
// regression from a tool error.
//
// The tool has no default before ref: without -before it prints a line and
// exits 0. make bench-ab and the nightly supply the latest release tag
// (git describe --tags --abbrev=0 --match 'v[0-9]*').
//
// The default workflows are failure_repro at concurrency 1 and simulation at
// concurrency 8. failure_repro at concurrency 1 is the sensitive one: with a
// single worker nothing contends with a step, so a per-checkpoint cost shows
// up in the checkpoint p50 undiluted. simulation at concurrency 8 covers the
// contended path.
//
// Usage:
//
//	go build -o bin/bench-ab ./cmd/bench-ab
//	./bin/bench-ab -before v0.2.19
//	./bin/bench-ab -before HEAD -rounds 2 -workflows failure_repro:1
//
// Each raw file starts with a line written by bench-ab naming the side, the
// ref it was built from and the round (see rawHeader), because branchbench's
// own header takes its version from the cwd's git describe, which is the
// after side's for both binaries.
package main

import (
	"archive/tar"
	"bytes"
	"context"
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
	"syscall"
	"time"
)

// spec is one item of -workflows: a branchbench workflow and the
// concurrency to run it at.
type spec struct {
	Name        string
	Concurrency int
}

// metric names one figure of a sample, how to read it, and whether it
// gates the overall verdict. Only a gating metric can turn a regression
// into exit code 1; a non-gating metric is informational and always
// reports "info" in the verdict column, regardless of what compareRounds
// found for it.
type metric struct {
	Name   string
	Get    func(sample) float64
	Gating bool
}

var metrics = []metric{
	{"wall (s)", func(s sample) float64 { return s.WallS }, false},
	{"fork p50 (ms)", func(s sample) float64 { return s.ForkP50 }, true},
	{"checkout p50 (ms)", func(s sample) float64 { return s.CheckoutP50 }, true},
	{"checkpoint p50 (ms)", func(s sample) float64 { return s.CheckpointP50 }, true},
	{"eval p50 (ms)", func(s sample) float64 { return s.EvalP50 }, false},
}

// reportRow is one line of the report table: a metric's comparison result
// plus the verdict label that is actually printed, which is "info" for a
// non-gating metric no matter what compareRounds found.
type reportRow struct {
	Metric  metric
	Result  result
	Verdict string // "ok", "regressed", "improved", or "info"
}

// evaluateMetrics compares every metric in metrics between beforeSamples and
// afterSamples for one workflow and returns one reportRow per metric, plus
// whether any gating metric regressed. Only fork, checkout and checkpoint
// p50 gate the overall verdict (and so the exit code); wall and eval are
// informational, always reporting "info" and never setting anyRegressed,
// because wall is rounded to 0.1s and eval times the workload's own query
// rather than offshoot's.
func evaluateMetrics(metrics []metric, beforeSamples, afterSamples []sample, threshold float64) (rows []reportRow, anyRegressed bool, err error) {
	for _, m := range metrics {
		res, err := compareRounds(values(beforeSamples, m), values(afterSamples, m), threshold)
		if err != nil {
			return nil, false, err
		}
		label := "ok"
		switch {
		case res.Regressed:
			label = "regressed"
		case res.Improved:
			label = "improved"
		}
		if !m.Gating {
			label = "info"
		} else if label == "regressed" {
			anyRegressed = true
		}
		rows = append(rows, reportRow{Metric: m, Result: res, Verdict: label})
	}
	return rows, anyRegressed, nil
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
	rounds := flag.Int("rounds", 4, "measured rounds per workflow; odd rounds run before then after, even rounds after then before, so an even count balances the order")
	runTimeout := flag.Duration("run-timeout", 15*time.Minute, "branchbench -timeout for each run; one minute after it the run is sent SIGTERM, and 30s later it is killed")
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
	if *runTimeout <= 0 {
		return 2, fmt.Errorf("bench-ab: -run-timeout must be positive, got %v", *runTimeout)
	}
	specs, err := parseSpecs(*workflowsFlag)
	if err != nil {
		return 2, err
	}

	root, err := moduleRoot()
	if err != nil {
		return 2, err
	}
	commit, err := gitOutput(root, "rev-parse", "--verify", *before+"^{commit}")
	if err != nil {
		return 2, fmt.Errorf("bench-ab: %q is not a commit: %w", *before, err)
	}
	beforeShort, err := gitOutput(root, "rev-parse", "--short", commit)
	if err != nil {
		return 2, err
	}
	afterShort, err := gitOutput(root, "rev-parse", "--short", "HEAD")
	if err != nil {
		return 2, err
	}
	if porcelain, err := gitOutput(root, "status", "--porcelain"); err != nil {
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
	if err := gitArchive(root, commit, srcBefore); err != nil {
		return 2, err
	}
	refs := map[string]string{"before": beforeShort, "after": afterShort}
	bins := map[string]string{
		"before": filepath.Join(out, "bb-before"),
		"after":  filepath.Join(out, "bb-after"),
	}
	fmt.Fprintln(os.Stderr, "bench-ab: building the before branchbench")
	if err := goBuild(srcBefore, bins["before"]); err != nil {
		return 2, err
	}
	fmt.Fprintln(os.Stderr, "bench-ab: building the after branchbench from the working tree")
	if err := goBuild(root, bins["after"]); err != nil {
		return 2, err
	}

	loadBefore := loadAverage()
	// samples[workflow][side] holds one sample per round.
	samples := map[string]map[string][]sample{}
	for _, sp := range specs {
		samples[sp.Name] = map[string][]sample{}
		fmt.Fprintf(os.Stderr, "bench-ab: %s c=%d warm-up (after, discarded)\n", sp.Name, sp.Concurrency)
		if _, err := runBench(bins["after"], sp, out, "after", refs["after"], "warmup", *runTimeout); err != nil {
			return 2, err
		}
		for round := 1; round <= *rounds; round++ {
			for _, side := range runOrder(round) {
				fmt.Fprintf(os.Stderr, "bench-ab: %s c=%d round %d/%d %s\n", sp.Name, sp.Concurrency, round, *rounds, side)
				s, err := runBench(bins[side], sp, out, side, refs[side], strconv.Itoa(round), *runTimeout)
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
		"and every after run is slower than every before run; improved is the mirror of that rule. "+
		"Only fork, checkout and checkpoint p50 gate the verdict and the exit code; wall and eval are "+
		"informational and always read \"info\" below, never regressed or improved.\n\n",
		*rounds, *threshold*100)
	fmt.Fprintf(&b, "Order: one discarded warm-up run of the after build per workflow, then odd rounds before → after "+
		"and even rounds after → before%s.\n\n", orderNote(*rounds))
	fmt.Fprintf(&b, "Load average before the series: %s\n\n", loadBefore)
	b.WriteString("| workflow | metric | before (rounds) | after (rounds) | before median | after median | change | verdict |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	anyRegressed := false
	for _, sp := range specs {
		rows, regressed, err := evaluateMetrics(metrics, samples[sp.Name]["before"], samples[sp.Name]["after"], *threshold)
		if err != nil {
			return 2, err
		}
		if regressed {
			anyRegressed = true
		}
		for _, row := range rows {
			fmt.Fprintf(&b, "| %s:%d | %s | %s | %s | %.1f | %.1f | %s | %s |\n",
				sp.Name, sp.Concurrency, row.Metric.Name, join(row.Result.Before), join(row.Result.After),
				row.Result.BeforeMedian, row.Result.AfterMedian,
				change(row.Result.BeforeMedian, row.Result.AfterMedian), row.Verdict)
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

// runOrder returns the order of the two sides in a round: odd rounds run
// before then after, even rounds after then before, so with an even number
// of rounds each side runs first equally often.
func runOrder(round int) []string {
	if round%2 == 1 {
		return []string{"before", "after"}
	}
	return []string{"after", "before"}
}

// orderNote is the report's remark on whether the round count balances the
// two orders.
func orderNote(rounds int) string {
	if rounds%2 == 0 {
		return " (balanced)"
	}
	return " (an odd round count leaves one round unbalanced, with before first)"
}

// moduleRoot returns the directory of the main module that holds
// cmd/branchbench, resolved from the current directory, so the tool runs
// from any subdirectory of the repository.
func moduleRoot() (string, error) {
	var stderr bytes.Buffer
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	dir := strings.TrimSpace(string(out))
	if err != nil {
		return "", fmt.Errorf("bench-ab: run inside the offshoot module: go list -m failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if dir == "" || strings.Contains(dir, "\n") {
		return "", fmt.Errorf("bench-ab: run inside the offshoot module: go list -m found no single main module from the current directory")
	}
	if fi, err := os.Stat(filepath.Join(dir, "cmd", "branchbench")); err != nil || !fi.IsDir() {
		return "", fmt.Errorf("bench-ab: module root %s has no cmd/branchbench; run inside the offshoot module", dir)
	}
	return dir, nil
}

// gitOutput runs git in dir and returns its trimmed stdout, folding stderr
// into the error.
func gitOutput(dir string, args ...string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
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
// cleaned path leaves dest is an error. git runs in dir.
func gitArchive(dir, commit, dest string) error {
	var stderr bytes.Buffer
	cmd := exec.Command("git", "archive", "--format=tar", commit)
	cmd.Dir = dir
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
		// filepath.Join cleans the entry's name, so a name with ".."
		// components resolves to wherever it points; the extraction
		// proceeds only when the cleaned target is strictly inside dest
		// (the prefix check against dest plus a separator is the shape
		// CodeQL's zip-slip query recognises as the sanitiser).
		root := filepath.Clean(dest)
		target := filepath.Join(dest, hdr.Name)
		if target == root {
			continue // the archive's own root directory entry
		}
		if filepath.IsAbs(hdr.Name) || !strings.HasPrefix(target, root+string(filepath.Separator)) {
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

// goBuild builds ./cmd/branchbench into the absolute path bin, in dir, so
// the build uses that tree's go.mod.
func goBuild(dir, bin string) error {
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/branchbench")
	cmd.Dir = dir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bench-ab: go build ./cmd/branchbench in %s: %w", dir, err)
	}
	return nil
}

// runBench runs one branchbench and returns its table row for sp. ref is
// the short revision the binary was built from, round a round number or
// "warmup". A rawHeader line, then the run's stdout, then a "--- stderr ---"
// line, then its stderr, are kept in raw-<workflow>-<side>-<round>.txt under
// out, so a failed run or a parse failure can be diagnosed from the
// artifact; the error also carries the last lines of stderr, so a CI log is
// readable without the artifact. branchbench gets -timeout runTimeout, and
// one minute after that, if it has not exited, it is sent SIGTERM rather
// than killed outright, so it has WaitDelay (below) to remove its store
// before the hard kill fires.
func runBench(bin string, sp spec, out, side, ref, round string, runTimeout time.Duration) (sample, error) {
	ctx, cancel := context.WithTimeout(context.Background(), runTimeout+time.Minute)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, bin, "-workflows", sp.Name, "-concurrency", strconv.Itoa(sp.Concurrency),
		"-timeout", runTimeout.String())
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Ask nicely first: SIGTERM instead of CommandContext's default Kill,
	// so branchbench's own cleanup (removing its store) gets a chance to
	// run before WaitDelay expires and the hard kill follows.
	cmd.Cancel = func() error {
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	cmd.WaitDelay = 30 * time.Second
	runErr := cmd.Run()
	raw := filepath.Join(out, fmt.Sprintf("raw-%s-%s-%s.txt", sp.Name, side, round))
	content := rawHeader(side, ref, round) + stdout.String() + "\n--- stderr ---\n" + stderr.String()
	if err := os.WriteFile(raw, []byte(content), 0o644); err != nil {
		return sample{}, err
	}
	tail := tailLines(stderr.String(), 20)
	if ctx.Err() == context.DeadlineExceeded {
		return sample{}, fmt.Errorf("bench-ab: %s branchbench %s round %s was killed after %v (see %s); stderr tail:\n%s",
			side, sp.Name, round, runTimeout+time.Minute, raw, tail)
	}
	if runErr != nil {
		return sample{}, fmt.Errorf("bench-ab: %s branchbench %s round %s failed: %w (see %s); stderr tail:\n%s",
			side, sp.Name, round, runErr, raw, tail)
	}
	s, err := findRow(stdout.String(), sp.Name)
	if err != nil {
		return sample{}, fmt.Errorf("%w (see %s); stderr tail:\n%s", err, raw, tail)
	}
	return s, nil
}

// rawHeader is the first line of each raw file: the side, the short ref its
// binary was built from, and the round. branchbench's own header carries
// the cwd's git describe, the after side's for both binaries, so without
// this line a before file would carry the after build's version
// unexplained.
func rawHeader(side, ref, round string) string {
	return fmt.Sprintf("# bench-ab side=%s ref=%s round=%s\n", side, ref, round)
}

// tailLines returns the last n lines of s.
func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
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
