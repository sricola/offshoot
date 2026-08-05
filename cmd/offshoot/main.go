// Command offshoot is the branchable-SQLite CLI (Plan 2: local mode).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/offshoot-db/offshoot/internal/daemon"
	"github.com/offshoot-db/offshoot/internal/mcp"
	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/store"
)

const usage = `offshoot — branch SQLite like git (local mode)

Usage:
  offshoot init                      create a store in ./.offshoot
  offshoot create <db> [--from f]    new database (branch main), or import file f
  offshoot checkout <db>[@branch]    materialize a working copy; prints its path
  offshoot checkpoint <db>[@branch] <name>   snapshot the checkout as a named checkpoint
  offshoot fork <db>[@branch] <new> [--at cp] [--ttl duration|none]   branch from head or a checkpoint
  offshoot touch <db>[@branch] [--ttl duration|none]   reset a branch's activity clock, optionally (re)setting its TTL
  offshoot rollback <db>[@branch] --to <cp>       repoint a branch at a checkpoint
  offshoot promote <db>@<src> --onto <target> [--force]   repoint target at src's head
  offshoot destroy <db>[@branch] [--force]   delete a branch (requires --force for protected)
  offshoot gc [--grace duration]     garbage collect unreachable lineages (default grace: 1h)
  offshoot path <db>[@branch]        print the checkout path
  offshoot status                    print all branches and their state
  offshoot lease list                       list every branch's lease
  offshoot lease acquire <db>[@branch] [--ttl 30s]   claim or renew a lease
  offshoot lease release <db>[@branch]      release a lease
  offshoot serve [-socket PATH]             run the daemon until SIGINT/SIGTERM
  offshoot mcp                              serve the MCP tool set on stdio for an agent
  offshoot session open <db>[@branch] [-socket PATH]      open a session; prints the checkout path
  offshoot session flush <db>[@branch] [name] [-socket PATH]   flush to a durable snapshot; prints the txid
  offshoot session status [-socket PATH]                  list open sessions and their durable txid
  offshoot session close <db>[@branch] [-socket PATH]     close a session, releasing its lease
  offshoot session shutdown [-socket PATH]                ask the daemon to shut down gracefully

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

// parseTTLFlag turns a --ttl flag's raw value into a duration: "none" means
// explicitly no TTL (0), anything else is parsed with time.ParseDuration.
func parseTTLFlag(raw string) (time.Duration, error) {
	if raw == "none" {
		return 0, nil
	}
	return time.ParseDuration(raw)
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
		if len(rest) != 2 {
			return fmt.Errorf("usage: offshoot checkpoint <db>[@branch] <name>")
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		txid, err := w.Checkpoint(db, branch, rest[1])
		if err != nil {
			return err
		}
		fmt.Printf("checkpoint %q at txid %d\n", rest[1], txid)
		return nil
	case "fork":
		fs := rest
		at, fs, _, err := extractFlag(fs, "--at")
		if err != nil {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration|none]: %w", err)
		}
		ttlRaw, fs, hasTTL, err := extractFlag(fs, "--ttl")
		if err != nil {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration|none]: %w", err)
		}
		ttl := time.Duration(0)
		if hasTTL {
			ttl, err = parseTTLFlag(ttlRaw)
			if err != nil {
				return err
			}
		}
		if len(fs) != 2 {
			return fmt.Errorf("usage: offshoot fork <db>[@branch] <new-branch> [--at checkpoint] [--ttl duration|none]")
		}
		db, branch, err := ops.ParseTarget(fs[0])
		if err != nil {
			return err
		}
		txid, err := w.Fork(db, branch, fs[1], at, ttl)
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
	case "checkout", "path":
		if len(rest) != 1 {
			return fmt.Errorf("usage: offshoot %s <db>[@branch]", cmd)
		}
		db, branch, err := ops.ParseTarget(rest[0])
		if err != nil {
			return err
		}
		if cmd == "path" {
			fmt.Println(w.CheckoutPath(db, branch))
			return nil
		}
		path, err := w.Checkout(db, branch)
		if err != nil {
			return err
		}
		fmt.Println(path)
		return nil
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
		tombstoned, deleted, err := w.GC(grace)
		if err != nil {
			return err
		}
		fmt.Printf("gc: tombstoned %d, deleted %d lineages\n", tombstoned, deleted)
		return nil
	case "status":
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
			line := fmt.Sprintf("%s@%s txid=%d checkpoints=[%s]%s",
				s.DB, s.Branch, s.HeadTXID, strings.Join(s.Checkpoints, ","), flags)
			if s.TTL != "" {
				line += fmt.Sprintf(" ttl=%s remaining=%s", s.TTL, s.TTLRemaining)
			}
			fmt.Println(line)
		}
		return nil
	case "lease":
		if len(rest) == 0 {
			return fmt.Errorf("usage: offshoot lease list|acquire|release ...")
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
		if len(rest) != 0 {
			return fmt.Errorf("usage: offshoot mcp")
		}
		ts := mcp.NewOffshootTools(w, spec)
		srv := mcp.NewServer(os.Stdin, os.Stdout, ts)
		return srv.Serve(context.Background())
	case "serve":
		sock, rest, err := socketOverride(rest)
		if err != nil {
			return fmt.Errorf("usage: offshoot serve [-socket PATH]: %w", err)
		}
		if len(rest) != 0 {
			return fmt.Errorf("usage: offshoot serve [-socket PATH]")
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
				line := fmt.Sprintf("%s@%s durable=%d epoch=%d holder=%s checkout=%s",
					in.DB, in.Branch, in.DurableTXID, in.Epoch, in.Holder, in.Checkout)
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
		default:
			return fmt.Errorf("unknown session subcommand %q", sub)
		}
	default:
		return fmt.Errorf("unknown command %q\n%s", cmd, usage)
	}
}
