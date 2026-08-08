// Command offshoot is the branchable-SQLite CLI (Plan 2: local mode).
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sricola/offshoot/internal/daemon"
	"github.com/sricola/offshoot/internal/mcp"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/session"
	"github.com/sricola/offshoot/internal/store"
)

// version is the offshoot release version, embedded at build time via
// -ldflags "-X main.version=...". release.yml sets it to the pushed tag
// (e.g. v0.1.0); local `go build`/`go run` leave it at "dev".
var version = "dev"

const usage = `offshoot — branch SQLite like git (local mode)

Usage:
  offshoot init                      create a store in ./.offshoot
  offshoot create <db> [--from f]    new database (branch main), or import file f
  offshoot checkout <db>[@branch]    materialize a working copy; prints its path
  offshoot checkout <db>[@branch] --at <checkpoint> --read-only [--force]
                                     materialize a checkpoint into a separate,
                                     immutable read-only cache path (never the
                                     writable checkout); repeat calls are a
                                     cache hit unless --force re-materializes
  offshoot export <db>[@branch[@checkpoint]] <out.db> [--force]
                                     copy a checkpoint (or head) out to a
                                     plain SQLite file anywhere; refuses to
                                     overwrite an existing out.db unless
                                     --force; no sidecar, no lease — out.db
                                     has no ongoing relationship to the store
  offshoot diff <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary]
                                     materialize both sides read-only and run
                                     sqldiff over them; --summary prints a
                                     table-level row-count comparison instead
                                     (no sqldiff needed) — row counts only;
                                     equal counts with different values
                                     report as same — use full sqldiff mode
                                     for content; either target may omit the
                                     checkpoint for the branch's current
                                     head; the two sides may name the same
                                     db or different ones
  offshoot checkpoint <db>[@branch] <name> [--meta k=v ...]
                                     snapshot the checkout as a named checkpoint
  offshoot fork <db>[@branch] <new> [--at cp] [--ttl duration] [--meta k=v ...]
                                     branch from head or a checkpoint;
                                     --meta is repeatable (capped: 32 keys,
                                     64-byte keys, 512-byte values) and is
                                     stored on the checkpoint (checkpoint) or
                                     the new branch (fork)
  offshoot touch <db>[@branch] [--ttl duration|none]   reset a branch's activity clock, optionally (re)setting its TTL
  offshoot rollback <db>[@branch] --to <cp>       repoint a branch at a checkpoint
  offshoot promote <db>@<src> --onto <target> [--force]   repoint target at src's head
  offshoot compact <db>[@branch]     make a shared fork self-contained (its
                                     ancestor's storage becomes reclaimable
                                     by gc); no-op if already self-contained
  offshoot destroy <db>[@branch] [--force]   delete a branch (requires --force for protected)
  offshoot gc [--grace duration]     garbage collect unreachable lineages (default grace: 1h)
  offshoot path <db>[@branch]        print the checkout path
  offshoot status [-ro-cache-budget BYTES]
                                     print all branches, each with its state and
                                     storage class (shared = base-pointer fork,
                                     near-zero added storage; materialized =
                                     self-contained full copy), plus
                                     a checkouts-ro usage summary; -ro-cache-budget
                                     is display-only (echoes serve's own flag;
                                     not persisted anywhere)
  offshoot lease list                       list every branch's lease
  offshoot lease acquire <db>[@branch] [--ttl 30s]   claim or renew a lease
  offshoot lease release <db>[@branch]      release a lease
  offshoot serve [-socket PATH] [-reap-every d] [-gc-grace d] [-flush-every d]
                 [-snapshot-every N] [-ro-cache-budget BYTES] [-http ADDR] [-token TOKEN] [-http-allow-non-loopback]
                                             run the daemon until SIGINT/SIGTERM;
                                             -flush-every ships every open session's
                                             work on a cadence even if it's never
                                             flushed explicitly (default 30s; 0 disables);
                                             -snapshot-every N sets every open session's
                                             full-snapshot cadence (default 16, unchanged
                                             if omitted; must be >= 1) — more segments per
                                             snapshot means cheaper flushes but more replay
                                             per read, see docs/reference.md
                                             -ro-cache-budget bounds checkouts-ro
                                             (default 0 = unlimited); the janitor
                                             LRU-evicts (by a .last-used touch-on-hit
                                             marker) once usage exceeds it, on the
                                             same -reap-every cadence; a bare integer
                                             is bytes, or use a K/M/G/T suffix
                                             (power-of-1024); checkouts/ (writable,
                                             leased) is never evicted
                                             -http ADDR (e.g. 127.0.0.1:8080) additionally
                                             starts an HTTP listener (POST /rpc, GET
                                             /metrics, GET /healthz, GET /debug/pprof/*,
                                             all but /healthz requiring "Authorization:
                                             Bearer <token>"); off by default. -token
                                             (or OFFSHOOT_TOKEN) sets the token; omitted,
                                             one is generated and printed ONCE to stderr
                                             (loopback binds only — treat that output as
                                             sensitive). -http-allow-non-loopback
                                             acknowledges binding beyond localhost, and
                                             additionally REQUIRES an explicit token
  offshoot mcp [-default-ttl d|none] [-socket PATH]
                                             serve the MCP tool set on stdio for an agent;
                                             forked branches get this TTL unless the fork
                                             call overrides it (default 24h; 0/none disables
                                             — reaping still needs a running janitor, i.e.
                                             offshoot serve, or a manual offshoot gc);
                                             -socket names the daemon to ride when one is up
                                             (default: same derivation offshoot serve uses) —
                                             checkpoint/fork/checkout on a branch with a
                                             session already open there use it for live
                                             capture; everything else, and any branch with no
                                             open session, still runs entirely at rest
  offshoot session open <db>[@branch] [-socket PATH]      open a session; prints the checkout path
  offshoot session flush <db>[@branch] [name] [-socket PATH]   flush to a durable snapshot; prints the txid
  offshoot session status [-socket PATH]                  list open sessions and their durable txid
  offshoot session close <db>[@branch] [-socket PATH]     close a session, releasing its lease
  offshoot session shutdown [-socket PATH]                ask the daemon to shut down gracefully
  offshoot session dbs [-socket PATH]                     list every database this store has
  offshoot version                   print the offshoot version and Go runtime info

  -socket PATH on a session subcommand must match the -socket PATH (if any)
  given to the serve that's running, or OFFSHOOT_SOCKET; otherwise it is
  derived from the store spec the same way on both sides.

Store location: -store SPEC or OFFSHOOT_STORE, default ./.offshoot
  SPEC is a directory path, file:///abs/path, or s3://bucket/prefix
  S3: OFFSHOOT_S3_ENDPOINT, OFFSHOOT_S3_REGION, OFFSHOOT_S3_PATH_STYLE;
      credentials from the AWS SDK default chain (env, shared config, IAM role)
  Remote stores keep checkouts in OFFSHOOT_CHECKOUTS (default: user cache dir)
`

func storeSpec(args []string) (string, []string) {
	spec := os.Getenv("OFFSHOOT_STORE")
	if spec == "" {
		spec = ".offshoot"
	}
	out := args[:0]
	for i := 0; i < len(args); i++ {
		if args[i] == "-store" && i+1 < len(args) {
			spec = args[i+1]
			i++
			continue
		}
		out = append(out, args[i])
	}
	return spec, out
}

// socketOverride extracts a "-socket PATH" flag from args (in any position),
// mirroring storeSpec's parsing so -store and -socket compose the same way.
// It returns the socket path (empty if none was given) and the remaining
// args with that flag removed. `serve` and `session` share this so that a
// daemon started with `-socket PATH` and the `session` subcommands that talk
// to it always agree on where the socket is.
//
// A trailing "-socket" with no PATH following it is a malformed flag, not a
// positional argument: it is rejected here with an error rather than being
// silently passed through in the remaining args. Without this check, `serve`
// happened to reject it too, but only by accident — its caller separately
// rejects any nonempty leftover args — while `session` has no such leftover
// check and would silently swallow the flag, e.g. treating `session flush
// app -socket` as an ordinary flush with `-socket` ignored instead of an
// error.
func socketOverride(args []string) (string, []string, error) {
	sock := ""
	out := args[:0]
	for i := 0; i < len(args); i++ {
		if args[i] == "-socket" {
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("-socket requires a PATH argument")
			}
			sock = args[i+1]
			i++
			continue
		}
		out = append(out, args[i])
	}
	return sock, out, nil
}

// extractFlag pulls a "name value" pair out of args, in any position,
// mirroring socketOverride's parsing style. ok reports whether the flag was
// present at all; err is non-nil only when the flag is present with no
// value following it.
func extractFlag(args []string, name string) (value string, rest []string, ok bool, err error) {
	out := args[:0]
	for i := 0; i < len(args); i++ {
		if args[i] == name {
			if i+1 >= len(args) {
				return "", nil, false, fmt.Errorf("%s requires a value", name)
			}
			value, ok = args[i+1], true
			i++
			continue
		}
		out = append(out, args[i])
	}
	return value, out, ok, nil
}

// extractBoolFlag pulls every occurrence of a bare boolean flag (e.g.
// "--force", "--read-only") out of args, in any position, mirroring
// extractFlag's parsing style but for a flag that takes no value. Returns
// whether it was present at all, and the remaining args with every
// occurrence removed.
func extractBoolFlag(args []string, name string) (bool, []string) {
	found := false
	out := args[:0]
	for _, a := range args {
		if a == name {
			found = true
			continue
		}
		out = append(out, a)
	}
	return found, out
}

// extractMetaFlags pulls every repeatable "--meta k=v" pair out of args, in
// any position, mirroring extractFlag's parsing style. Returns nil (not an
// empty map) when no --meta flag was given, so a caller can pass the result
// straight through as ops.Workspace.Fork/Checkpoint's meta param, where nil
// means "no metadata" — the same convention those functions themselves use.
func extractMetaFlags(args []string) (map[string]string, []string, error) {
	var meta map[string]string
	out := args[:0]
	for i := 0; i < len(args); i++ {
		if args[i] == "--meta" {
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("--meta requires a k=v argument")
			}
			kv := args[i+1]
			eq := strings.IndexByte(kv, '=')
			if eq < 0 {
				return nil, nil, fmt.Errorf("--meta %q: want k=v", kv)
			}
			if meta == nil {
				meta = map[string]string{}
			}
			meta[kv[:eq]] = kv[eq+1:]
			i++
			continue
		}
		out = append(out, args[i])
	}
	return meta, out, nil
}

// parseTTLFlag turns a --ttl flag's raw value into a duration: "none" means
// explicitly no TTL (0), anything else is parsed with time.ParseDuration.
func parseTTLFlag(raw string) (time.Duration, error) {
	if raw == "none" {
		return 0, nil
	}
	return time.ParseDuration(raw)
}

// parseForkTTLFlag turns fork's --ttl flag into a duration. Unlike
// parseTTLFlag (touch), fork has no "none" sentinel: a brand-new branch has
// no existing TTL to explicitly clear, so a caller that wants no TTL simply
// omits --ttl. A non-positive duration ("none", "0s", "-1h", ...) is
// refused rather than silently treated as no TTL — swallowing it would fork
// a TTL-less branch while the caller believes it asked for one.
func parseForkTTLFlag(raw string) (time.Duration, error) {
	if raw == "none" {
		return 0, fmt.Errorf(`fork has no "none" concept; omit --ttl for no TTL`)
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf(`fork --ttl %q must be positive; omit --ttl for no TTL`, raw)
	}
	return d, nil
}

// parseDefaultTTLFlag extracts mcp's -default-ttl flag from args and parses
// it, defaulting to 24h when the flag is absent. It reuses parseTTLFlag, so
// "none" and a literal zero duration ("0", "0s", ...) both disable the
// default identically; a negative duration is rejected as a usage error
// (mirroring -flush-every's own non-positive-is-a-mistake rule) rather than
// silently aliased to "disabled". Returns the parsed default TTL and the
// remaining (flag-stripped) args, so the caller can still reject leftover
// arguments itself.
func parseDefaultTTLFlag(args []string) (time.Duration, []string, error) {
	raw, rest, _, err := extractFlag(args, "-default-ttl")
	if err != nil {
		return 0, nil, err
	}
	if raw == "" {
		raw = "24h"
	}
	d, err := parseTTLFlag(raw)
	if err != nil {
		return 0, nil, fmt.Errorf("-default-ttl: %w", err)
	}
	if d < 0 {
		return 0, nil, fmt.Errorf("-default-ttl %q must be zero, \"none\" (both disable it), or positive", raw)
	}
	return d, rest, nil
}

// parseByteSize parses a -ro-cache-budget-shaped flag value: "" (the flag
// omitted) is 0 (unlimited, that flag's own default); otherwise a bare
// non-negative integer is taken as a byte count directly — bytes are the
// CONTRACT this flag and offshoot_ro_cache_bytes/EvictROCache all speak, and
// the only form ever guaranteed stable. As a convenience, a trailing
// power-of-1024 size suffix is also accepted, case-insensitively: "K"/"KB",
// "M"/"MB", "G"/"GB", "T"/"TB" (each multiplies by 1024^n — NOT the SI
// decimal 1000-based convention some tools use for the same letters; when
// in doubt, pass raw bytes). A bare trailing "B" (no K/M/G/T prefix) is
// also accepted as a no-op multiplier, purely so "500B" and "500" parse
// identically rather than one of them erroring on a redundant unit.
func parseByteSize(raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	s := strings.TrimSpace(raw)
	upper := strings.ToUpper(s)
	mult := int64(1)
	for _, suf := range []struct {
		suffix string
		mult   int64
	}{
		{"TB", 1 << 40}, {"T", 1 << 40},
		{"GB", 1 << 30}, {"G", 1 << 30},
		{"MB", 1 << 20}, {"M", 1 << 20},
		{"KB", 1 << 10}, {"K", 1 << 10},
		{"B", 1},
	} {
		if strings.HasSuffix(upper, suf.suffix) {
			mult = suf.mult
			s = s[:len(s)-len(suf.suffix)]
			break
		}
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("%q: missing a numeric value", raw)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q: %w", raw, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("%q: must be zero or positive (zero means unlimited)", raw)
	}
	// n * mult on int64 can overflow: e.g. "8388608T" (n=8388608,
	// mult=1<<40) wraps past MaxInt64 into a negative value, which callers
	// treat as "unlimited" — the opposite of what an operator asking for a
	// huge-but-finite budget meant. Other magnitudes wrap into small
	// positive values, silently producing a tiny budget and surprise
	// eviction instead of any error at all. Catch it before multiplying
	// rather than after, since the wrapped product itself no longer carries
	// any signal that it wrapped.
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("%q: size too large", raw)
	}
	return n * mult, nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "offshoot:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	spec, args := storeSpec(args)
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	cmd, rest := args[0], args[1:]

	if cmd == "init" {
		_, err := ops.Init(spec)
		if err == nil {
			fmt.Println("initialized store at", spec)
		}
		return err
	}

	if cmd == "version" {
		if len(rest) != 0 {
			return fmt.Errorf("usage: offshoot version")
		}
		fmt.Printf("offshoot %s %s %s/%s\n", version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		return nil
	}

	w, err := ops.Open(spec)
	if err != nil {
		return fmt.Errorf("open store %s: %w (run 'offshoot init'?)", spec, err)
	}
	switch cmd {
	case "create":
		switch {
		case len(rest) == 1:
			return w.Create(rest[0])
		case len(rest) == 3 && rest[1] == "--from":
			return w.CreateFrom(rest[0], rest[2])
		default:
			return fmt.Errorf("usage: offshoot create <db> [--from file]")
		}
	case "checkpoint":
		meta, rest, err := extractMetaFlags(rest)
		if err != nil {
			return fmt.Errorf("usage: offshoot checkpoint <db>[@branch] <name> [--meta k=v ...]: %w", err)
		}
		if len(rest) != 2 {
			return fmt.Errorf("usage: offshoot checkpoint <db>[@branch] <name> [--meta k=v ...]")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		txid, err := w.Checkpoint(db, branch, rest[1], meta)
		if err != nil {
			return err
		}
		fmt.Printf("checkpoint %q at txid %d\n", rest[1], txid)
		return nil
	case "fork":
		fs := rest
		at, fs, _, err := extractFlag(fs, "--at")
		if err != nil {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration] [--meta k=v ...]: %w", err)
		}
		ttlRaw, fs, hasTTL, err := extractFlag(fs, "--ttl")
		if err != nil {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration] [--meta k=v ...]: %w", err)
		}
		meta, fs, err := extractMetaFlags(fs)
		if err != nil {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration] [--meta k=v ...]: %w", err)
		}
		ttl := time.Duration(0)
		if hasTTL {
			// fork has no "none" sentinel the way touch does (a brand-new
			// branch has no existing TTL to explicitly clear), and a
			// non-positive duration is refused rather than silently treated
			// as no TTL — see parseForkTTLFlag.
			ttl, err = parseForkTTLFlag(ttlRaw)
			if err != nil {
				return err
			}
		}
		if len(fs) != 2 {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration] [--meta k=v ...]")
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		txid, err := w.Fork(db, branch, fs[1], at, ttl, meta)
		if err != nil {
			return err
		}
		fmt.Printf("forked %s@%s -> %s@%s at txid %d\n", db, branch, db, fs[1], txid)
		return nil
	case "touch":
		fs := rest
		ttlRaw, fs, hasTTL, err := extractFlag(fs, "--ttl")
		if err != nil {
			return fmt.Errorf("usage: offshoot touch <db>[@branch] [--ttl duration|none]: %w", err)
		}
		if len(fs) != 1 {
			return fmt.Errorf("usage: offshoot touch <db>[@branch] [--ttl duration|none]")
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		var ttl *time.Duration
		if hasTTL {
			d, err := parseTTLFlag(ttlRaw)
			if err != nil {
				return err
			}
			ttl = &d
		}
		ref, err := w.Touch(db, branch, ttl, time.Now())
		if err != nil {
			return err
		}
		out := ref.TTL
		if out == "" {
			out = "none"
		}
		fmt.Printf("touched %s@%s ttl=%s touched_at=%s\n", db, branch, out, ref.TouchedAt)
		return nil
	case "rollback":
		if len(rest) != 3 || rest[1] != "--to" {
			return fmt.Errorf("usage: offshoot rollback <db>[@branch] --to <checkpoint>")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		p, err := w.Rollback(db, branch, rest[2])
		if err != nil {
			return err
		}
		fmt.Println(p)
		return nil
	case "promote":
		force := false
		fs := rest[:0]
		for _, a := range rest {
			if a == "--force" {
				force = true
				continue
			}
			fs = append(fs, a)
		}
		if len(fs) != 3 || fs[1] != "--onto" {
			return fmt.Errorf("usage: offshoot promote <db>@<source> --onto <target> [--force]")
		}
		db, srcBranch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		txid, err := w.Promote(db, srcBranch, fs[2], force)
		if err != nil {
			return err
		}
		fmt.Printf("promoted %s@%s -> %s@%s at txid %d\n", db, srcBranch, db, fs[2], txid)
		return nil
	case "compact":
		if len(rest) != 1 {
			return fmt.Errorf("usage: offshoot compact <db>[@branch]")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		txid, err := w.Compact(db, branch)
		if err != nil {
			return err
		}
		fmt.Printf("compacted %s@%s to txid %d\n", db, branch, txid)
		return nil
	case "path":
		if len(rest) != 1 {
			return fmt.Errorf("usage: offshoot path <db>[@branch]")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		fmt.Println(w.CheckoutPath(db, branch))
		return nil
	case "checkout":
		usageErr := fmt.Errorf("usage: offshoot checkout <db>[@branch] [--at checkpoint --read-only [--force]]")
		fs := rest
		at, fs, _, err := extractFlag(fs, "--at")
		if err != nil {
			return fmt.Errorf("usage: offshoot checkout <db>[@branch] [--at checkpoint --read-only [--force]]: %w", err)
		}
		readOnly, fs := extractBoolFlag(fs, "--read-only")
		force, fs := extractBoolFlag(fs, "--force")
		if len(fs) != 1 {
			return usageErr
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		switch {
		case at != "" && readOnly:
			path, err := w.CheckoutAt(db, branch, at, force)
			if err != nil {
				return err
			}
			fmt.Println(path)
			return nil
		case at != "" || readOnly:
			return fmt.Errorf("offshoot checkout: --at and --read-only must be given together")
		case force:
			return fmt.Errorf("offshoot checkout: --force only applies alongside --at --read-only")
		default:
			path, err := w.Checkout(db, branch)
			if err != nil {
				return err
			}
			fmt.Println(path)
			return nil
		}
	case "export":
		force, rest := extractBoolFlag(rest, "--force")
		if len(rest) != 2 {
			return fmt.Errorf("usage: offshoot export <db>[@branch[@checkpoint]] <out.db> [--force]")
		}
		db, branch, checkpoint, err := ops.ParseExportTarget(rest[0])
		if err != nil {
			return err
		}
		if err := w.Export(db, branch, checkpoint, rest[1], force); err != nil {
			return err
		}
		fmt.Println(rest[1])
		return nil
	case "diff":
		summary, rest := extractBoolFlag(rest, "--summary")
		if len(rest) != 2 {
			return fmt.Errorf("usage: offshoot diff <db>[@branch[@checkpoint]] <db>[@branch[@checkpoint]] [--summary]")
		}
		return runDiff(w, os.Stdout, rest[0], rest[1], summary)
	case "destroy":
		force := false
		fs := rest[:0]
		for _, a := range rest {
			if a == "--force" {
				force = true
				continue
			}
			fs = append(fs, a)
		}
		if len(fs) != 1 {
			return fmt.Errorf("usage: offshoot destroy <db>[@branch] [--force]")
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		return w.Destroy(db, branch, force)
	case "gc":
		grace := time.Hour
		if len(rest) == 2 && rest[0] == "--grace" {
			d, err := time.ParseDuration(rest[1])
			if err != nil {
				return err
			}
			grace = d
		}
		// A reap failure on one branch (e.g. its checkout has an open
		// connection, "database is busy") must not stop GC from running:
		// GC is independent lineage cleanup, and skipping it entirely would
		// let one unreapable branch wedge garbage collection forever.
		// Report the failure and press on.
		reaped, reapErr := w.Reap(time.Now())
		if len(reaped) > 0 {
			fmt.Printf("gc: reaped %v\n", reaped)
		}
		if reapErr != nil {
			fmt.Fprintf(os.Stderr, "offshoot: gc: reap: %v\n", reapErr)
		}
		// Milestone 4 Task 6b: self-heal any Destroy claim (Ref.Deleting)
		// stranded by a crashed `destroy`/`gc` call, same "report and press
		// on" convention as the reap failure above — this is at-rest cleanup
		// independent of everything else `gc` does, so one branch's stale
		// claim must not block it either.
		healed, healErr := w.ClearStaleDeleteClaims(time.Now())
		if len(healed) > 0 {
			fmt.Printf("gc: cleared stale delete claim(s) %v\n", healed)
		}
		if healErr != nil {
			fmt.Fprintf(os.Stderr, "offshoot: gc: clear stale delete claims: %v\n", healErr)
		}
		tombstoned, deleted, err := w.GC(grace)
		if err != nil {
			return err
		}
		fmt.Printf("gc: tombstoned %d, deleted %d objects\n", tombstoned, deleted)
		return nil
	case "status":
		// -ro-cache-budget is purely for DISPLAY here — like every other
		// serve-time tuning knob (-reap-every, -gc-grace, -flush-every), the
		// budget itself is never persisted to the store, so this at-rest
		// command (no daemon involved — see below) has no other way to know
		// what a running daemon was actually started with. Passing the same
		// value here as the daemon's own -ro-cache-budget lets an operator
		// see usage against it without a live daemon connection; omitted
		// (the common case) just reports usage with no budget context.
		roCacheBudgetStr, rest, _, err := extractFlag(rest, "-ro-cache-budget")
		if err != nil {
			return err
		}
		roCacheBudget, err := parseByteSize(roCacheBudgetStr)
		if err != nil {
			return fmt.Errorf("-ro-cache-budget: %w", err)
		}
		if len(rest) != 0 {
			return fmt.Errorf("usage: offshoot status [-ro-cache-budget BYTES]")
		}
		sts, err := w.Status()
		if err != nil {
			return err
		}
		for _, s := range sts {
			flags := ""
			if s.Protected {
				flags += " protected"
			}
			if s.CheckedOut {
				flags += " checked-out"
			}
			// storage= is the copy-on-write cost class, reported honestly on
			// every line: "shared" means this branch is a base-pointer fork
			// (near-zero added storage, reading through an ancestor's durable
			// objects); "materialized" means a fully self-contained lineage.
			// fork shares; promote/rollback/compact each materialize a full
			// copy — the asymmetry is deliberate and worth seeing per branch.
			storage := "materialized"
			if s.Shared {
				storage = "shared"
			}
			line := fmt.Sprintf("%s@%s state=%s storage=%s txid=%d checkpoints=[%s]%s",
				s.DB, s.Branch, s.State, storage, s.HeadTXID, strings.Join(s.Checkpoints, ","), flags)
			if s.TTL != "" {
				line += fmt.Sprintf(" ttl=%s remaining=%s", s.TTL, s.TTLRemaining)
			}
			fmt.Println(line)
		}
		// Milestone 4 Task 5: ro-cache usage summary. Computed directly off
		// checkouts-ro (ops.Workspace.ROCacheUsage), the same at-rest read
		// every other line above already uses — no daemon required, exactly
		// like the rest of this command.
		roBytes, roCount, err := w.ROCacheUsage()
		if err != nil {
			return err
		}
		if roCacheBudget > 0 {
			fmt.Printf("ro-cache: %d entries, %d bytes used, budget %d bytes\n", roCount, roBytes, roCacheBudget)
		} else {
			fmt.Printf("ro-cache: %d entries, %d bytes used (budget: unlimited)\n", roCount, roBytes)
		}
		return nil
	case "lease":
		if len(rest) == 0 {
			return fmt.Errorf("usage: offshoot lease list|acquire|release [args]")
		}
		switch rest[0] {
		case "list":
			infos, err := w.Leases()
			if err != nil {
				return err
			}
			for _, in := range infos {
				state := "held"
				if in.Expired {
					state = "expired"
				}
				fmt.Printf("%s@%s %s by %s epoch=%d until %s\n",
					in.DB, in.Branch, state, in.Holder, in.Epoch,
					in.Expiry.Format(time.RFC3339))
			}
			return nil
		case "acquire":
			args := rest[1:]
			ttl := ops.DefaultLeaseTTL
			if len(args) == 3 && args[1] == "--ttl" {
				d, err := time.ParseDuration(args[2])
				if err != nil {
					return err
				}
				ttl = d
				args = args[:1]
			}
			if len(args) != 1 {
				return fmt.Errorf("usage: offshoot lease acquire <db>[@branch] [--ttl 30s]")
			}
			db, branch, err := ops.ParseTarget(args[0])
			if err != nil {
				return err
			}
			l, err := w.AcquireLease(db, branch, ops.LocalHolder(), ttl)
			if err != nil {
				return err
			}
			fmt.Printf("acquired %s@%s as %s (epoch %d) until %s\n",
				db, branch, l.Holder, l.Epoch, l.Expiry.Format(time.RFC3339))
			fmt.Println("note: this command exits immediately; the lease expires unless a" +
				" long-running holder renews it")
			return nil
		case "release":
			if len(rest) != 2 {
				return fmt.Errorf("usage: offshoot lease release <db>[@branch]")
			}
			db, branch, err := ops.ParseTarget(rest[1])
			if err != nil {
				return err
			}
			infos, err := w.Leases()
			if err != nil {
				return err
			}
			for _, in := range infos {
				if in.DB == db && in.Branch == branch {
					return w.ReleaseLease(store.Lease{
						DB: db, Branch: branch, Holder: in.Holder, Epoch: in.Epoch,
					})
				}
			}
			return fmt.Errorf("offshoot: no lease on %s@%s", db, branch)
		default:
			return fmt.Errorf("unknown lease subcommand %q", rest[0])
		}
	case "mcp":
		sock, rest, err := socketOverride(rest)
		if err != nil {
			return fmt.Errorf("usage: offshoot mcp [-default-ttl DURATION|none] [-socket PATH]: %w", err)
		}
		defaultTTL, rest, err := parseDefaultTTLFlag(rest)
		if err != nil {
			return fmt.Errorf("usage: offshoot mcp [-default-ttl DURATION|none] [-socket PATH]: %w", err)
		}
		if len(rest) != 0 {
			return fmt.Errorf("usage: offshoot mcp [-default-ttl DURATION|none] [-socket PATH]")
		}
		// sock == "" here (the common case: no -socket given) is resolved by
		// NewOffshootTools itself via daemon.DefaultSocketPath(spec) — the
		// same default `offshoot serve` binds to for this store — so a bare
		// `offshoot mcp` and a bare `offshoot serve` against the same store
		// agree on where to look without either side hardcoding the path.
		ts := mcp.NewOffshootTools(w, spec, defaultTTL, sock)
		srv := mcp.NewServer(os.Stdin, os.Stdout, ts)
		return srv.Serve(context.Background())
	case "serve":
		const serveUsage = "usage: offshoot serve [-socket PATH] [-reap-every DURATION] [-gc-grace DURATION] " +
			"[-flush-every DURATION] [-snapshot-every N] [-ro-cache-budget BYTES] [-http ADDR] [-token TOKEN] [-http-allow-non-loopback]"
		sock, rest, err := socketOverride(rest)
		if err != nil {
			return fmt.Errorf("%s: %w", serveUsage, err)
		}
		reapEveryStr, rest, _, err := extractFlag(rest, "-reap-every")
		if err != nil {
			return err
		}
		if reapEveryStr == "" {
			reapEveryStr = "1m"
		}
		reapEvery, err := time.ParseDuration(reapEveryStr)
		if err != nil {
			return fmt.Errorf("-reap-every: %w", err)
		}
		gcGraceStr, rest, _, err := extractFlag(rest, "-gc-grace")
		if err != nil {
			return err
		}
		if gcGraceStr == "" {
			gcGraceStr = "15m"
		}
		gcGrace, err := time.ParseDuration(gcGraceStr)
		if err != nil {
			return fmt.Errorf("-gc-grace: %w", err)
		}
		flushEveryStr, rest, _, err := extractFlag(rest, "-flush-every")
		if err != nil {
			return err
		}
		if flushEveryStr == "" {
			flushEveryStr = "30s"
		}
		flushEvery, err := time.ParseDuration(flushEveryStr)
		if err != nil {
			return fmt.Errorf("-flush-every: %w", err)
		}
		if flushEvery < 0 {
			return fmt.Errorf("-flush-every %q must be zero or positive; zero disables auto-flush "+
				"(there is no \"none\" alias — a negative value is almost certainly a mistake)", flushEveryStr)
		}
		snapshotEveryStr, rest, snapshotEveryGiven, err := extractFlag(rest, "-snapshot-every")
		if err != nil {
			return err
		}
		var snapshotEvery int
		if snapshotEveryGiven {
			n, err := strconv.Atoi(snapshotEveryStr)
			if err != nil {
				return fmt.Errorf("-snapshot-every: %w", err)
			}
			if n < 1 {
				return fmt.Errorf("-snapshot-every %d must be >= 1 (there is no \"unlimited\" — "+
					"omit the flag for the default of %d)", n, session.DefaultSnapshotEvery)
			}
			snapshotEvery = n
		}
		roCacheBudgetStr, rest, _, err := extractFlag(rest, "-ro-cache-budget")
		if err != nil {
			return err
		}
		roCacheBudget, err := parseByteSize(roCacheBudgetStr)
		if err != nil {
			return fmt.Errorf("-ro-cache-budget: %w", err)
		}
		httpAddr, rest, _, err := extractFlag(rest, "-http")
		if err != nil {
			return err
		}
		tokenFlag, rest, _, err := extractFlag(rest, "-token")
		if err != nil {
			return err
		}
		allowNonLoopback, rest := extractBoolFlag(rest, "-http-allow-non-loopback")
		if len(rest) != 0 {
			return errors.New(serveUsage)
		}
		// -token/-http-allow-non-loopback only mean anything alongside
		// -http; given without it, they'd otherwise be silently ignored —
		// almost certainly a mistake (e.g. a typo'd or dropped -http ADDR
		// in a script), and one that would be surprising to notice only by
		// its ABSENCE (no auth, no HTTP listener at all) rather than an
		// explicit error at startup.
		if httpAddr == "" {
			if tokenFlag != "" {
				return fmt.Errorf("-token given without -http; it has no effect unless -http ADDR is also set")
			}
			if allowNonLoopback {
				return fmt.Errorf("-http-allow-non-loopback given without -http; it has no effect unless -http ADDR is also set")
			}
		}

		// Milestone 4 Task 3: HTTP is off by default (httpAddr == ""); when
		// requested, validate the bind BEFORE touching the socket/daemon at
		// all, so a misconfigured -http fails fast as a startup error and
		// never leaves a half-started daemon behind. Token precedence:
		// -token, then OFFSHOOT_TOKEN, else auto-generated (loopback binds
		// only — see daemon.ValidateHTTPBind, called both here and again,
		// defensively, inside StartHTTP itself).
		token := tokenFlag
		if token == "" {
			token = os.Getenv("OFFSHOOT_TOKEN")
		}
		hasExplicitToken := token != ""
		autoGenerated := false
		if httpAddr != "" {
			if err := daemon.ValidateHTTPBind(httpAddr, allowNonLoopback, hasExplicitToken); err != nil {
				return err
			}
			if !hasExplicitToken {
				t, err := daemon.GenerateToken()
				if err != nil {
					return err
				}
				token = t
				autoGenerated = true
			}
		}

		if sock == "" {
			p, err := daemon.DefaultSocketPath(spec)
			if err != nil {
				return err
			}
			sock = p
		}
		srv, err := daemon.NewServer(w, sock)
		if err != nil {
			return err
		}
		srv.SetVersion(version)
		srv.StartJanitor(reapEvery, gcGrace)
		// safe-by-default cadence: the daemon ships work on a cadence even
		// if the agent it's serving never calls flush; -flush-every 0
		// disables this, restoring manual-only (session.Options' own
		// default).
		srv.SetFlushEvery(flushEvery)
		// Milestone 4 Task 6a: 0 (the default, -snapshot-every not given)
		// means "let session.Open apply its own default" — see
		// Server.SetSnapshotEvery's doc comment. Only set when the flag was
		// actually given (snapshotEveryGiven), so a bare `offshoot serve`
		// keeps behaving exactly as it did before this flag existed.
		if snapshotEveryGiven {
			srv.SetSnapshotEvery(snapshotEvery)
		}
		// Milestone 4 Task 5: 0 (the default, unset) means unlimited — the
		// janitor still computes and reports checkouts-ro usage every pass,
		// it just never evicts. See SetROCacheBudget's doc comment.
		srv.SetROCacheBudget(roCacheBudget)
		if httpAddr != "" {
			if err := srv.StartHTTP(daemon.HTTPConfig{
				Addr:             httpAddr,
				Token:            token,
				AllowNonLoopback: allowNonLoopback,
				AutoGenerated:    autoGenerated,
			}); err != nil {
				return err
			}
		}
		fmt.Println("offshoot serving on", sock)
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
		errc := make(chan error, 1)
		go func() { errc <- srv.Serve() }()
		select {
		case <-sigc:
			fmt.Println("offshoot: shutting down, releasing leases")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return srv.Shutdown(ctx)
		case err := <-errc:
			return err
		}
	case "session":
		sock, rest, err := socketOverride(rest)
		if err != nil {
			return fmt.Errorf("usage: offshoot session open|flush|status|close|shutdown ... [-socket PATH]: %w", err)
		}
		if len(rest) == 0 {
			return fmt.Errorf("usage: offshoot session open|flush|status|close|shutdown ... [-socket PATH]")
		}
		if sock == "" {
			p, err := daemon.DefaultSocketPath(spec)
			if err != nil {
				return err
			}
			sock = p
		}
		sub, args := rest[0], rest[1:]
		target := func() (string, string, error) {
			if len(args) < 1 {
				return "", "", fmt.Errorf("usage: offshoot session %s <db>[@branch]", sub)
			}
			return ops.ParseTarget(args[0])
		}
		switch sub {
		case "open":
			db, branch, err := target()
			if err != nil {
				return err
			}
			resp, err := daemon.Call(sock, daemon.Request{Op: "open", DB: db, Branch: branch})
			if err != nil {
				return err
			}
			fmt.Println(resp.Checkout)
			return nil
		case "flush":
			db, branch, err := target()
			if err != nil {
				return err
			}
			name := ""
			if len(args) == 2 {
				name = args[1]
			}
			resp, err := daemon.Call(sock, daemon.Request{Op: "flush", DB: db, Branch: branch, Name: name})
			if err != nil {
				return err
			}
			fmt.Printf("durable through txid %d\n", resp.TXID)
			return nil
		case "status":
			resp, err := daemon.Call(sock, daemon.Request{Op: "status"})
			if err != nil {
				return err
			}
			for _, in := range resp.Sessions {
				line := fmt.Sprintf("%s@%s durable=%d epoch=%d holder=%s checkout=%s lag=%d",
					in.DB, in.Branch, in.DurableTXID, in.Epoch, in.Holder, in.Checkout, in.CaptureLag)
				if in.LastFlushAt != "" {
					line += " last_flush=" + in.LastFlushAt
				}
				if in.DurableAge != "" {
					line += " age=" + in.DurableAge
				}
				if in.FlushError != "" {
					line += " flush_error=" + in.FlushError
				}
				if in.Error != "" {
					line += " ERROR=" + in.Error
				}
				fmt.Println(line)
			}
			return nil
		case "close":
			db, branch, err := target()
			if err != nil {
				return err
			}
			_, err = daemon.Call(sock, daemon.Request{Op: "close", DB: db, Branch: branch})
			return err
		case "shutdown":
			_, err := daemon.Call(sock, daemon.Request{Op: "shutdown"})
			return err
		case "dbs":
			resp, err := daemon.Call(sock, daemon.Request{Op: "dbs"})
			if err != nil {
				return err
			}
			for _, db := range resp.Databases {
				fmt.Println(db)
			}
			return nil
		default:
			return fmt.Errorf("unknown session subcommand %q", sub)
		}
	default:
		return fmt.Errorf("unknown command %q\n%s", cmd, usage)
	}
}
