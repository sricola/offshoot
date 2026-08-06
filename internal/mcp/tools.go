package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/offshoot-db/offshoot/internal/daemon"
	"github.com/offshoot-db/offshoot/internal/ops"
	"github.com/offshoot-db/offshoot/internal/store"
)

// OffshootTools exposes offshoot's branch lifecycle to an agent over MCP:
// fork before risky work, checkpoint on success, roll back on failure,
// promote a winning attempt, destroy a losing one.
type OffshootTools struct {
	ws *ops.Workspace
	// socket is the daemon socket every tool call probes fresh (see
	// daemonStatus/openSession) — never cached, since the daemon may start
	// or stop between calls. Always concrete (never ""): NewOffshootTools
	// resolves it once at construction.
	socket string
	// defaultTTL is applied to a fork whose call omits `ttl` entirely;
	// zero means forks have no TTL unless one is given explicitly. An
	// explicit `ttl:"none"` on the call always overrides this, and an
	// explicit `ttl:"<duration>"` always wins over it too — see
	// resolveForkTTL.
	defaultTTL time.Duration
}

// NewOffshootTools binds a tool set to a workspace, store spec, and daemon
// socket. socket, if non-empty, is the exact socket every tool call probes
// (see daemonStatus); "" falls back to daemon.DefaultSocketPath(spec) — the
// same default `offshoot serve` binds to for this store — resolved once
// here rather than per call, since neither the spec nor an OFFSHOOT_SOCKET
// override changes for the lifetime of a tool set. A resolution failure
// (e.g. no cache dir) leaves socket empty, which every call's dial then
// fails fast against — the same silent at-rest fallback as a daemon that
// simply isn't running.
//
// defaultTTL is the TTL offshoot_fork applies when a call omits `ttl`;
// zero means agent-created forks are immortal unless a call passes `ttl`
// itself (the CLI's `offshoot mcp -default-ttl` flag is how an operator
// sets this — see cmd/offshoot/main.go).
//
// Per-call daemon routing (checkpoint → daemon flush when the branch has an
// open session, fork → daemon fork whenever a daemon is up, checkout →
// the open session's own checkout path) is documented on each handler; see
// checkpoint, fork, and checkout below. Every other tool is a ref-level
// operation the daemon adds nothing to and always runs at rest.
func NewOffshootTools(ws *ops.Workspace, spec string, defaultTTL time.Duration, socket string) *OffshootTools {
	if socket == "" {
		if p, err := daemon.DefaultSocketPath(spec); err == nil {
			socket = p
		}
	}
	return &OffshootTools{ws: ws, socket: socket, defaultTTL: defaultTTL}
}

// statusProbeTimeout bounds daemonStatus's wait for a "status" reply.
// daemon.Call's own dial timeout (2s, see internal/daemon/client.go) only
// bounds connecting; once connected, its Decode blocks until the daemon
// actually writes a response, with no timeout of its own. Every tool call
// probes status fresh (see NewOffshootTools's per-call, never-cached
// contract), so a daemon that accepts connections but never answers —
// wedged, deadlocked, whatever — would otherwise hang every subsequent MCP
// call at this detection step alone. The bound lives here, local to this
// one probe, rather than as a client.go change: an actual flush/fork
// request (see checkpoint/fork below) is a real operation the agent is
// waiting on the result of and must not be truncated the same way.
const statusProbeTimeout = 2 * time.Second

// daemonStatus probes the daemon at t.socket, fresh on every call — the
// daemon may start after this tool set was constructed, so this is never
// cached. ok is false whenever the daemon isn't reachable (not running,
// still starting, a dial timeout, doesn't answer within statusProbeTimeout,
// anything) or answers with ok=false; callers treat that uniformly as a
// silent fallback to at-rest behavior, never as an error surfaced to the
// agent. On a timeout, the underlying daemon.Call goroutine is abandoned
// (daemon.Call itself has no cancellation) rather than joined — an
// acceptable leak against a daemon this wedged, bounded by that goroutine's
// own eventual connection close or process exit.
func (t *OffshootTools) daemonStatus() (resp daemon.Response, ok bool) {
	type result struct {
		resp daemon.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := daemon.Call(t.socket, daemon.Request{Op: "status"})
		done <- result{resp, err}
	}()
	select {
	case r := <-done:
		if r.err != nil || !r.resp.OK {
			return daemon.Response{}, false
		}
		return r.resp, true
	case <-time.After(statusProbeTimeout):
		return daemon.Response{}, false
	}
}

// findSession scans an already-fetched status resp for db@branch's
// SessionInfo, regardless of health — present covers a live session AND a
// fenced/unhealthy one (SessionInfo.Error != "", which stays in the
// daemon's session map rather than disappearing — see server.go's
// opStatus). Callers that need "trustworthy for live capture" must use
// healthySession instead; callers that only need "the daemon has SOME
// claim on this branch" (refuseIfSessionOpen) use this directly.
func findSession(resp daemon.Response, db, branch string) (info daemon.SessionInfo, ok bool) {
	for _, s := range resp.Sessions {
		if s.DB == db && s.Branch == branch {
			return s, true
		}
	}
	return daemon.SessionInfo{}, false
}

// healthySession is findSession filtered to a session actually trustworthy
// for live capture: present AND not fenced/unhealthy. A session that lost
// its lease (fenced — e.g. another writer reclaimed it) stops writing but
// stays listed in "status" until it's closed, so its mere presence in
// findSession is not enough: handing an agent that session's checkout path
// as "live" (checkout) or routing a checkpoint through it (checkpoint)
// would promise continuous capture from a session that has actually
// stopped.
func healthySession(resp daemon.Response, db, branch string) (info daemon.SessionInfo, ok bool) {
	info, ok = findSession(resp, db, branch)
	if !ok || info.Error != "" {
		return daemon.SessionInfo{}, false
	}
	return info, true
}

// openSession reports the daemon's SessionInfo for db@branch if the daemon
// is reachable and has a HEALTHY session open there (see healthySession);
// ok=false covers every other case — daemon unreachable, no session at
// all, or a session that's fenced/unhealthy — uniformly, per this
// package's at-rest-fallback contract. checkpoint uses this directly, since
// falling back to an at-rest checkpoint is always safe here regardless of
// why (ops.Workspace.Checkpoint's raw open/close only risks a live
// in-process session, and MCP and the daemon are always separate
// processes — see Checkpoint's own doc comment); checkout needs the raw
// (error-included) lookup too, to explain a fenced-session fallback, so it
// calls daemonStatus/findSession/healthySession itself instead of this.
func (t *OffshootTools) openSession(db, branch string) (info daemon.SessionInfo, ok bool) {
	resp, up := t.daemonStatus()
	if !up {
		return daemon.SessionInfo{}, false
	}
	return healthySession(resp, db, branch)
}

// refuseIfSessionOpen refuses opName on db@branch (ok=true, with a
// ToolResult naming the session) if the daemon has ANY session entry for
// that branch — healthy or fenced — in its "status" response; ok=false
// (proceed at rest) if the daemon isn't reachable or has no such entry.
//
// This mirrors internal/daemon's own refuseIfClaimed guard (server.go),
// which the daemon already enforces against ITS OWN client for exactly
// these kinds of ops (checkout/destroy/rollback/promote-target): they
// repoint or delete a branch outright, so — unlike fork/checkpoint, which
// flush an open session's writes first and proceed — there is no live
// engine here to flush first; refusal is the only safe response. MCP calls
// ops.Workspace directly for rollback/promote/destroy, bypassing the
// daemon (and its refuseIfClaimed guard) entirely, so without this an
// at-rest call from this package could clear a lease or repoint/delete a
// branch out from under a session the daemon still believes it owns.
//
// A session still "reserved" (an in-flight daemon "open" not yet resolved
// into a live session) is invisible here: the daemon's "status" op only
// reports fully-open (or since-fenced) sessions, never a bare reservation
// (see opStatus) — narrower than the daemon's own in-process
// refuseIfClaimed, and a gap this package shares with any other
// out-of-band store client.
func (t *OffshootTools) refuseIfSessionOpen(db, branch, opName string) (ToolResult, bool) {
	resp, up := t.daemonStatus()
	if !up {
		return ToolResult{}, false
	}
	info, found := findSession(resp, db, branch)
	if !found {
		return ToolResult{}, false
	}
	if info.Error != "" {
		return ErrorResult("a daemon session (holder %q) is open on %s@%s but unhealthy (%s); "+
			"close it before %s — e.g. `offshoot session close %s@%s`",
			info.Holder, db, branch, info.Error, opName, db, branch), true
	}
	return ErrorResult("a daemon session (holder %q) is open on %s@%s; close it before %s — "+
		"e.g. `offshoot session close %s@%s`", info.Holder, db, branch, opName, db, branch), true
}

// prop describes one property of a tool's JSON Schema input: its argument
// name, its real JSON Schema type (so a bool-typed Go field like `force`
// is advertised as "boolean", not "string" — a model that follows the
// schema and sends `"force":"true"` should not get a confusing unmarshal
// error), whether it's required, and — for an optional property whose
// handler applies a default when absent — the default value to advertise.
type prop struct {
	name     string
	jsonType string
	required bool
	def      any
}

// reqStr and optStr build required/optional string properties, the common
// case (database, branch names, checkpoint names).
func reqStr(name string) prop { return prop{name: name, jsonType: "string", required: true} }
func optStr(name string) prop { return prop{name: name, jsonType: "string"} }

// optStrDefault builds an optional string property that documents the
// value the handler substitutes when the argument is omitted (e.g. `branch`
// defaults to "main" — see branchOr).
func optStrDefault(name string, def string) prop {
	return prop{name: name, jsonType: "string", def: def}
}

// optBool builds an optional boolean property (e.g. `force`).
func optBool(name string) prop { return prop{name: name, jsonType: "boolean"} }

// schema builds a JSON Schema object describing a tool's arguments from a
// list of properties, each carrying its own type/required/default.
func schema(props ...prop) map[string]any {
	properties := map[string]any{}
	var required []string
	for _, p := range props {
		def := map[string]any{"type": p.jsonType}
		if p.def != nil {
			def["default"] = p.def
		}
		properties[p.name] = def
		if p.required {
			required = append(required, p.name)
		}
	}
	s := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

// Tools returns the seven lifecycle tools this server exposes. Descriptions
// are the agent's only documentation: each explains not just what the tool
// does but when an agent should reach for it.
func (t *OffshootTools) Tools() []Tool {
	return []Tool{
		{
			Name: "offshoot_list",
			Description: "List every database and branch offshoot is tracking, with each " +
				"branch's head transaction id, named checkpoints, and whether it is " +
				"protected. Call this first to orient yourself: to see what databases " +
				"exist, what branches an attempt could fork from, or which checkpoints " +
				"are available to roll back to or fork from.",
			InputSchema: schema(),
		},
		{
			Name: "offshoot_checkout",
			Description: "Materialize a database branch to a local SQLite file and return " +
				"the path to open. Call this before reading or writing a branch's data " +
				"directly with a SQL client. If a daemon session is already open on this " +
				"branch (opened by a harness, not by this tool — this tool never opens " +
				"one itself), the result is that session's live checkout path instead: " +
				"writes there are captured continuously, and offshoot_checkpoint against " +
				"it flushes live rather than writing a fresh snapshot. Otherwise this is a " +
				"plain at-rest materialization, even if a daemon happens to be running. " +
				"Call offshoot_checkpoint to name the current state so it can be rolled " +
				"back to or forked from later. `branch` defaults to \"main\" if omitted.",
			InputSchema: schema(reqStr("database"), optStrDefault("branch", "main")),
		},
		{
			Name: "offshoot_checkpoint",
			Description: "Name the current state of a branch's checkout so it can be " +
				"returned to later. Call this after a batch of changes you might want " +
				"to keep or roll back to individually — e.g. after a migration step " +
				"succeeds, or before starting a riskier change on the same branch. " +
				"If a daemon session is open on this branch, this is a live flush " +
				"(cheap, only the diff since the last checkpoint, no pause in writes); " +
				"otherwise it's a full-snapshot checkpoint of the checkout file. " +
				"`branch` defaults to \"main\" if omitted.",
			InputSchema: schema(reqStr("database"), reqStr("name"), optStrDefault("branch", "main")),
		},
		{
			Name: "offshoot_fork",
			Description: "Create an isolated copy of a database branch before attempting " +
				"risky or destructive work (schema migrations, bulk deletes, " +
				"experiments). Forking is instant and costs nothing until you write. " +
				"Prefer forking over backing up by hand. Forks from the branch's " +
				"current head by default, or from a named checkpoint via `at`. If a " +
				"daemon session is open on the source branch, its unflushed writes are " +
				"flushed first, so the fork always includes everything written so far. " +
				"`branch` (the source) defaults to \"main\" if omitted. " +
				forkTTLDescription(t.defaultTTL) + " " + forkTTLJanitorNote,
			InputSchema: schema(reqStr("database"), reqStr("new_branch"),
				optStrDefault("branch", "main"), optStr("at"),
				optStrDefault("ttl", ttlDefaultDisplay(t.defaultTTL))),
		},
		{
			Name: "offshoot_rollback",
			Description: "Return a branch to a previously named checkpoint, discarding " +
				"everything written since. Call this when an attempt on a branch has " +
				"gone wrong and you want to restore known-good state rather than " +
				"manually undoing changes. Reports the checkout path to reopen after " +
				"the rollback. `branch` defaults to \"main\" if omitted. If a daemon " +
				"session is open on this branch, the call is refused instead of " +
				"proceeding, since rollback repoints the branch's storage out from " +
				"under a session the daemon still believes it owns — close the " +
				"session first (e.g. `offshoot session close`) and retry.",
			InputSchema: schema(reqStr("database"), reqStr("to"), optStrDefault("branch", "main")),
		},
		{
			Name: "offshoot_promote",
			Description: "Ship a winning attempt: repoint the target branch (often `main`) " +
				"at the source branch's current head, which resets the target's " +
				"checkpoint history to just the new promote checkpoint. Call this once " +
				"you've validated a forked attempt and are ready to make it the branch " +
				"of record. Protected branches (main is protected by default) refuse promotion " +
				"unless `force` is set — treat that refusal as confirmation you need, " +
				"not a bug. If a daemon session is open on the TARGET branch, the call " +
				"is refused instead of proceeding — `force` does not override this — " +
				"since promoting repoints the target's storage out from under a session " +
				"the daemon still believes it owns; close the session first (e.g. " +
				"`offshoot session close`) and retry. An open session on the SOURCE " +
				"does not block the call, but the promoted state is the source's " +
				"last-flushed/checkpointed head, not any write still unflushed in that " +
				"live session — flush or checkpoint the source first if you need its " +
				"very latest state promoted.",
			InputSchema: schema(reqStr("database"), reqStr("source"), reqStr("target"), optBool("force")),
		},
		{
			Name: "offshoot_destroy",
			Description: "Permanently discard a branch and its checkout. Call this to " +
				"clean up a failed or abandoned attempt once you're done with it. " +
				"Protected branches refuse destruction unless `force` is set — treat " +
				"that refusal as confirmation you need, not a bug. If a daemon session " +
				"is open on this branch, the call is refused instead of proceeding — " +
				"`force` does not override this — since destroy deletes the branch's " +
				"storage out from under a session the daemon still believes it owns; " +
				"close the session first (e.g. `offshoot session close`) and retry.",
			InputSchema: schema(reqStr("database"), reqStr("branch"), optBool("force")),
		},
	}
}

// branchOr defaults an optional branch argument to "main", offshoot's
// convention for the default branch.
func branchOr(branch string) string {
	if branch == "" {
		return "main"
	}
	return branch
}

// named pairs an argument's label (as it should read in an error message,
// e.g. "database" or "new_branch") with its value, for validateNames.
type named struct{ label, value string }

// namedArg builds a named for validateNames.
func namedArg(label, value string) named { return named{label: label, value: value} }

// validateNames is the single gate every MCP tool handler runs each
// name-shaped argument (database, branch, new_branch, source, target, to,
// checkpoint name, ...) through before calling into ops. The MCP tool
// handlers take database/branch straight from agent-supplied JSON with no
// ops.ParseTarget in between (unlike the CLI, which always derives db/branch
// through ParseTarget), so this is the surface's only gate against a
// traversal- or otherwise malformed name reaching ops. ops itself validates
// too (defense in depth, for any future non-MCP caller), but this handler-
// side check is what turns a bad name into a clear ErrorResult naming the
// offending argument and value, rather than an ops-internal error.
//
// It returns the ErrorResult to return immediately and ok=true on the first
// invalid argument, or ok=false once every argument has passed.
func validateNames(args ...named) (result ToolResult, ok bool) {
	for _, a := range args {
		if err := store.ValidateName(a.value); err != nil {
			return ErrorResult("invalid %s %q: %v", a.label, a.value, err), true
		}
	}
	return ToolResult{}, false
}

// Call dispatches name to its handler. The returned error is reserved for
// "this is not a real tool" (an RPC-level fault); every operational failure
// (bad name, no such checkpoint, protected branch) is reported as an
// ErrorResult so the agent — not the transport — sees it.
func (t *OffshootTools) Call(ctx context.Context, name string, args json.RawMessage) (ToolResult, error) {
	if len(args) == 0 {
		// Some clients omit `arguments` entirely for a tool whose schema has
		// no required fields (e.g. offshoot_list); treat that the same as
		// an explicit empty object rather than failing every handler's
		// json.Unmarshal on a zero-length input.
		args = json.RawMessage(`{}`)
	}
	switch name {
	case "offshoot_list":
		return t.list(args)
	case "offshoot_checkout":
		return t.checkout(args)
	case "offshoot_checkpoint":
		return t.checkpoint(args)
	case "offshoot_fork":
		return t.fork(args)
	case "offshoot_rollback":
		return t.rollback(args)
	case "offshoot_promote":
		return t.promote(args)
	case "offshoot_destroy":
		return t.destroy(args)
	default:
		return ToolResult{}, fmt.Errorf("mcp: unknown tool %q", name)
	}
}

func (t *OffshootTools) list(args json.RawMessage) (ToolResult, error) {
	statuses, err := t.ws.Status()
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	if len(statuses) == 0 {
		return TextResult("no databases yet; create one with the offshoot CLI (`offshoot create <name>`)"), nil
	}
	var b []byte
	for _, s := range statuses {
		line := fmt.Sprintf("%s@%s head=%d checkpoints=%v protected=%v checked_out=%v\n",
			s.DB, s.Branch, s.HeadTXID, s.Checkpoints, s.Protected, s.CheckedOut)
		b = append(b, line...)
	}
	return TextResult("%s", string(b)), nil
}

type checkoutArgs struct {
	Database string `json:"database"`
	Branch   string `json:"branch"`
}

// checkout materializes db@branch, or — if a daemon session is already open
// AND HEALTHY on that exact branch — reports the session's own live
// checkout path instead of materializing a fresh at-rest copy. No MCP tool
// ever opens a session itself: this only takes the live path when
// something else (a harness, the SDKs, `offshoot session open`) already
// has. A session that's present but fenced/unhealthy (lost its lease, e.g.
// to another writer — see healthySession) is deliberately NOT handed over:
// its checkout path is no longer being captured, so returning it would
// promise a durability guarantee that session can no longer keep. That case
// falls to the same at-rest materialization as "no session," but the
// response says why, naming the session's error, rather than silently
// looking identical to "no daemon at all."
//
// Detection is per call, never cached (daemonStatus), and this issues at
// most one status round trip for the whole call — both the live-session
// check above and the at-rest note below read the same resp — so a slow
// daemon costs one probe here, not two.
func (t *OffshootTools) checkout(args json.RawMessage) (ToolResult, error) {
	var a checkoutArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" {
		return ErrorResult("database is required"), nil
	}
	branch := branchOr(a.Branch)
	if r, bad := validateNames(namedArg("database", a.Database), namedArg("branch", branch)); bad {
		return r, nil
	}

	resp, up := t.daemonStatus()
	if up {
		if info, ok := healthySession(resp, a.Database, branch); ok {
			msg := fmt.Sprintf("%s@%s has an open daemon session; its live checkout is %s",
				a.Database, branch, info.Checkout)
			msg += "\nwrite there directly — the daemon captures every commit continuously; " +
				"call offshoot_checkpoint to name a point you can roll back to or fork from"
			return TextResult("%s", msg), nil
		}
	}

	path, err := t.ws.Checkout(a.Database, branch)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	msg := fmt.Sprintf("checked out %s@%s at %s", a.Database, branch, path)
	msg += "\nthis checkout is not yet checkpointed: nothing written here can be rolled " +
		"back to or forked from until you call offshoot_checkpoint"
	if up {
		if info, found := findSession(resp, a.Database, branch); found && info.Error != "" {
			msg += fmt.Sprintf("\nwarning: a daemon session (holder %q) is open on this branch "+
				"but unhealthy (%s); this is an at-rest checkout, not that fenced session's file, "+
				"until a new session replaces it", info.Holder, info.Error)
		} else {
			msg += "\na daemon is running for this store, but no session is open on this " +
				"branch, so this checkout is at rest: offshoot_checkpoint here will write a " +
				"full snapshot, not a live flush, until something opens a session on it"
		}
	}
	return TextResult("%s", msg), nil
}

type checkpointArgs struct {
	Database string `json:"database"`
	Branch   string `json:"branch"`
	Name     string `json:"name"`
}

// checkpoint names the current state of db@branch. If a daemon session is
// open on that exact branch, this is a live capture: the daemon's own
// "flush" op ships whatever the session has captured so far under the given
// name, with no quiesce and no risk of colliding with the session's lease
// (ops.Workspace.Checkpoint's raw open/close of the checkout file is unsafe
// against a live in-process session — this path never reaches it while one
// is open). No open session falls back to that at-rest snapshot exactly as
// before.
func (t *OffshootTools) checkpoint(args json.RawMessage) (ToolResult, error) {
	var a checkpointArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" || a.Name == "" {
		return ErrorResult("database and name are required"), nil
	}
	branch := branchOr(a.Branch)
	if r, bad := validateNames(namedArg("database", a.Database), namedArg("branch", branch),
		namedArg("name", a.Name)); bad {
		return r, nil
	}
	if _, ok := t.openSession(a.Database, branch); ok {
		resp, err := daemon.Call(t.socket, daemon.Request{Op: "flush", DB: a.Database, Branch: branch, Name: a.Name})
		if err != nil {
			return ErrorResult("%v", err), nil
		}
		return TextResult("checkpointed %s@%s as %q at txid %d — captured live from the open daemon session, no pause in writes",
			a.Database, branch, a.Name, resp.TXID), nil
	}
	txid, err := t.ws.Checkpoint(a.Database, branch, a.Name)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	return TextResult("checkpointed %s@%s as %q at txid %d", a.Database, branch, a.Name, txid), nil
}

type forkArgs struct {
	Database  string `json:"database"`
	Branch    string `json:"branch"`
	NewBranch string `json:"new_branch"`
	At        string `json:"at"`
	// TTL is a Go duration string ("2h"), "none" for no TTL (overrides any
	// configured default), or "" to fall back to OffshootTools.defaultTTL —
	// see resolveForkTTL.
	TTL string `json:"ttl"`
}

// forkTTLJanitorNote is appended to offshoot_fork's Description (and, in
// spirit, its response text — see forkTTLSummary) per the PM amendment: a
// TTL alone does not reap anything. Reaping is a janitor's job
// (`offshoot serve`'s background sweep); a daemonless MCP setup — the
// common case, since `offshoot mcp` needs no daemon — only reaps expired
// branches when `offshoot gc` is run by hand.
const forkTTLJanitorNote = "TTL reaping only happens while a janitor is running " +
	"(`offshoot serve`); a daemonless setup sweeps expired branches only when " +
	"`offshoot gc` is run."

// forkTTLDescription is the Description clause explaining fork's default TTL
// behavior for the given defaultTTL, so the model can reason about what a
// call that omits `ttl` will actually get.
func forkTTLDescription(defaultTTL time.Duration) string {
	if defaultTTL <= 0 {
		return "Forks have no TTL unless you pass one: `ttl` accepts a Go duration " +
			"string (e.g. \"2h\") after which the branch expires if not promoted or " +
			"touched."
	}
	return fmt.Sprintf("Forked branches expire %s after their last activity by "+
		"default, unless promoted or touched; pass `ttl:\"none\"` to keep one "+
		"indefinitely, or `ttl` as a Go duration string (e.g. \"2h\") to override.",
		defaultTTL.String())
}

// ttlDefaultDisplay renders defaultTTL the way the fork schema's `ttl`
// property advertises its default: "none" when no default is configured,
// otherwise its canonical time.Duration.String() form — the same rendering
// a ref's own TTL round-trips as (see the README's TTLs section).
func ttlDefaultDisplay(defaultTTL time.Duration) string {
	if defaultTTL <= 0 {
		return "none"
	}
	return defaultTTL.String()
}

// resolveForkTTL determines the TTL to apply to a fresh fork from the call's
// raw `ttl` argument and the tool set's configured default. An explicit
// `ttl` always wins over defaultTTL: "" (the argument omitted) falls back to
// defaultTTL, "none" explicitly disables TTL even under a configured
// default, and anything else is parsed as a Go duration. Fork has no "none"
// concept the way touch does — a brand-new branch has no existing TTL to
// preserve or clear — so a parsed non-positive duration is refused rather
// than silently treated as no TTL (matching ops.Workspace.Fork's own rule).
func resolveForkTTL(raw string, defaultTTL time.Duration) (time.Duration, error) {
	if raw == "" {
		return defaultTTL, nil
	}
	if raw == "none" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("ttl %q is not a valid duration: use a Go duration string "+
			"like \"2h\" or \"30m\", or \"none\" for no TTL (%v)", raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("ttl %q must be positive; use \"none\" for no TTL", raw)
	}
	return d, nil
}

// forkTTLSummary describes the TTL just applied to a fresh fork, for the
// tool's response text. Per the PM amendment, this (not just the ref
// written to the store) is what carries the applied TTL and computed expiry
// into the agent's transcript, so the model can reason about them later
// without a separate offshoot_list/offshoot_checkout round trip. Expiry is
// computed the same way Status does: ops.ReapDeadline from the ref's own
// TouchedAt, not from time.Now() here, so it reflects what was actually
// durably written.
func forkTTLSummary(ws *ops.Workspace, db, branch string, ttl time.Duration) string {
	if ttl <= 0 {
		return "ttl=none (never expires)"
	}
	ref, _, err := ws.Store.GetRef(db, branch)
	if err != nil {
		// The fork itself already succeeded; a re-read failure here only
		// means the response can't include a computed expiry — the janitor
		// caveat below doesn't depend on that re-read at all (it's a static
		// fact about how reaping works, not a value derived from the ref),
		// so it must not degrade along with expires_at.
		return fmt.Sprintf("ttl=%s; %s", ttl, forkTTLJanitorNote)
	}
	deadline, ok := ops.ReapDeadline(ref)
	if !ok {
		return fmt.Sprintf("ttl=%s; %s", ref.TTL, forkTTLJanitorNote)
	}
	return fmt.Sprintf("ttl=%s expires_at=%s; %s", ref.TTL, deadline.Format(time.RFC3339), forkTTLJanitorNote)
}

// fork creates new_branch from branch's head (or checkpoint `at`). If a
// daemon is up (whether or not it has a session open on the source — the
// daemon's own "fork" op handles both), the fork is routed through it, so
// an open source session's unflushed writes are flushed first and always
// land in the new branch. No daemon routes to the plain at-rest fork, as
// before. Either way, the response's TTL/expiry summary is computed by
// re-reading the ref straight from the store (forkTTLSummary), not from
// whichever path performed the fork.
func (t *OffshootTools) fork(args json.RawMessage) (ToolResult, error) {
	var a forkArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" || a.NewBranch == "" {
		return ErrorResult("database and new_branch are required"), nil
	}
	branch := branchOr(a.Branch)
	toValidate := []named{namedArg("database", a.Database), namedArg("branch", branch),
		namedArg("new_branch", a.NewBranch)}
	if a.At != "" {
		toValidate = append(toValidate, namedArg("at", a.At))
	}
	if r, bad := validateNames(toValidate...); bad {
		return r, nil
	}
	ttl, err := resolveForkTTL(a.TTL, t.defaultTTL)
	if err != nil {
		return ErrorResult("%v", err), nil
	}

	var txid uint64
	if _, up := t.daemonStatus(); up {
		req := daemon.Request{Op: "fork", DB: a.Database, Branch: branch, Name: a.NewBranch, From: a.At}
		if ttl > 0 {
			req.TTL = ttl.String()
		}
		resp, err := daemon.Call(t.socket, req)
		if err != nil {
			return ErrorResult("%v", err), nil
		}
		txid = resp.TXID
	} else {
		txid, err = t.ws.Fork(a.Database, branch, a.NewBranch, a.At, ttl)
		if err != nil {
			return ErrorResult("%v", err), nil
		}
	}
	msg := fmt.Sprintf("forked %s@%s to %s@%s at txid %d", a.Database, branch, a.Database, a.NewBranch, txid)
	msg += "; " + forkTTLSummary(t.ws, a.Database, a.NewBranch, ttl)
	return TextResult("%s", msg), nil
}

type rollbackArgs struct {
	Database string `json:"database"`
	Branch   string `json:"branch"`
	To       string `json:"to"`
}

// rollback repoints db@branch at checkpoint `to`, entirely at rest —
// unlike fork/checkpoint, there is no daemon op this could ride, and
// unlike checkout there's no live path to hand back either: a rollback
// repoints the branch's lineage outright. If the daemon has ANY session
// (healthy or fenced) open on branch, this refuses rather than proceeding
// (see refuseIfSessionOpen) — an at-rest rollback would repoint the branch
// out from under a session the daemon still believes owns its checkout.
func (t *OffshootTools) rollback(args json.RawMessage) (ToolResult, error) {
	var a rollbackArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" || a.To == "" {
		return ErrorResult("database and to are required"), nil
	}
	branch := branchOr(a.Branch)
	if r, bad := validateNames(namedArg("database", a.Database), namedArg("branch", branch),
		namedArg("to", a.To)); bad {
		return r, nil
	}
	if r, refused := t.refuseIfSessionOpen(a.Database, branch, "rolling it back"); refused {
		return r, nil
	}
	path, err := t.ws.Rollback(a.Database, branch, a.To)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	return TextResult("rolled back %s@%s to checkpoint %q; checkout at %s", a.Database, branch, a.To, path), nil
}

type promoteArgs struct {
	Database string `json:"database"`
	Source   string `json:"source"`
	Target   string `json:"target"`
	Force    bool   `json:"force"`
}

// promote repoints db@target at db@source's head, entirely at rest. If the
// daemon has ANY session (healthy or fenced) open on the TARGET, this
// refuses rather than proceeding (see refuseIfSessionOpen) — promote
// clears/repoints the target's ref outright, which would fence a session
// the daemon still believes owns it. The SOURCE is deliberately not
// guarded the same way: promote only reads its current head, and an
// unflushed write sitting in an open source session is a staleness
// quirk (identical to promote's pre-existing at-rest behavior), not a
// fencing hazard — there's nothing here for the daemon's own "flush the
// source first" semantics (opPromote) to ride, since this path never
// touches the daemon at all. daemon_test.go's
// TestPromoteFromOpenSourceProceedsAtRest pins exactly this: an open
// source session doesn't refuse, and the promoted state is the source's
// last-flushed head, not the unflushed write.
func (t *OffshootTools) promote(args json.RawMessage) (ToolResult, error) {
	var a promoteArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" || a.Source == "" || a.Target == "" {
		return ErrorResult("database, source, and target are required"), nil
	}
	if r, bad := validateNames(namedArg("database", a.Database), namedArg("source", a.Source),
		namedArg("target", a.Target)); bad {
		return r, nil
	}
	if r, refused := t.refuseIfSessionOpen(a.Database, a.Target, "promoting onto it"); refused {
		return r, nil
	}
	txid, err := t.ws.Promote(a.Database, a.Source, a.Target, a.Force)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	return TextResult("promoted %s@%s onto %s@%s at txid %d", a.Database, a.Source, a.Database, a.Target, txid), nil
}

type destroyArgs struct {
	Database string `json:"database"`
	Branch   string `json:"branch"`
	Force    bool   `json:"force"`
}

// destroy deletes db@branch entirely at rest. If the daemon has ANY
// session (healthy or fenced) open on branch, this refuses rather than
// proceeding (see refuseIfSessionOpen) — an at-rest destroy would delete
// the branch (and, per ops.Destroy, clear its lease) out from under a
// session the daemon still believes owns it.
func (t *OffshootTools) destroy(args json.RawMessage) (ToolResult, error) {
	var a destroyArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" || a.Branch == "" {
		return ErrorResult("database and branch are required"), nil
	}
	if r, bad := validateNames(namedArg("database", a.Database), namedArg("branch", a.Branch)); bad {
		return r, nil
	}
	if r, refused := t.refuseIfSessionOpen(a.Database, a.Branch, "destroying it"); refused {
		return r, nil
	}
	if err := t.ws.Destroy(a.Database, a.Branch, a.Force); err != nil {
		return ErrorResult("%v", err), nil
	}
	return TextResult("destroyed %s@%s", a.Database, a.Branch), nil
}
