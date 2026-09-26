package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sricola/offshoot/internal/daemon"
	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
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
	// allowForce gates an agent-supplied `force` against a PROTECTED branch
	// in promote/destroy: false (the default) refuses it outright, before
	// any mutation, regardless of ops' own force handling; true lets it
	// through to ops as before. See SetAllowForce.
	allowForce bool
}

// SetAllowForce sets whether this tool set honors an agent-supplied `force`
// against a protected branch in offshoot_promote/offshoot_destroy. The
// zero-value OffshootTools has this false: an MCP agent cannot force onto
// or destroy a protected branch (main, by default) unless the operator
// running this server opted in, e.g. via `offshoot mcp -allow-force` (see
// cmd/offshoot/main.go). Force against an UNPROTECTED branch never needed
// this flag and is unaffected either way.
func (t *OffshootTools) SetAllowForce(v bool) {
	t.allowForce = v
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

// refuseForceOnProtected implements the -allow-force gate for promote
// (branch is the TARGET) and destroy (branch is the branch being
// destroyed): an agent-supplied force is honored as-is once this server was
// started with -allow-force (SetAllowForce(true)), same as before this gate
// existed. Without that flag, MCP never honors force at all: force against
// a PROTECTED branch is refused here — before either handler calls into
// ops, i.e. before any mutation — rather than left to ops' own protected
// check; force against an UNPROTECTED branch is still downgraded to false
// in effectiveForce, so ops runs exactly as an unforced call would. This
// matters beyond the protected check: ops.Destroy also consults force to
// bypass a live lease (see gc.go's Destroy), and downgrading here means
// that bypass is unavailable too, same as the protected one — the caller
// (destroy, and promote for symmetry) is expected to notice a resulting
// ops error asking for --force and rewrap it pointing at -allow-force; see
// wrapForceRefusal.
//
// If refused is true, res is the ToolResult the caller should return
// immediately. Otherwise effectiveForce is what the caller should pass to
// ops in place of the raw argument.
//
// A GetRef failure (e.g. a transient backend error, or no such branch)
// fails CLOSED: effectiveForce is false, never the caller's raw force. This
// gate cannot confirm the branch is safe to force, so it must not let force
// through on an error just because ops's own GetRef, a moment later, might
// have succeeded (e.g. a transient blip that clears) — the real error
// (including "no such branch") still surfaces from the downstream ops call,
// same as before this gate existed, just never with force intact.
func (t *OffshootTools) refuseForceOnProtected(db, branch string, force bool) (res ToolResult, refused bool, effectiveForce bool) {
	if !force || t.allowForce {
		return ToolResult{}, false, force
	}
	ref, _, err := t.ws.Store.GetRef(db, branch)
	if err != nil {
		return ToolResult{}, false, false
	}
	if ref.Protected {
		return ErrorResult("%s@%s is protected and this MCP server does not allow force "+
			"(start it with `offshoot mcp -allow-force` to permit it). Ask the human to "+
			"promote/destroy from the CLI, or work on a fork.", db, branch), true, false
	}
	return ToolResult{}, false, false
}

// wrapForceRefusal rewraps an ops error that asked for "--force" (the
// protected-branch and, for destroy, live-lease checks in internal/ops both
// phrase their refusal that way — see gc.go's Destroy and ops.go's
// PromoteWith) into one that points at the actual lever an MCP agent has:
// -allow-force on this server, not `force` on the call (refuseForceOnProtected
// already downgrades `force` to false whenever this server wasn't started
// with -allow-force, so a raw "use --force" from ops would otherwise read as
// a lie — the agent DID pass force:true). Called only when the caller asked
// for force but this server isn't honoring it; every other ops error passes
// through untouched.
func wrapForceRefusal(err error, force, allowForce bool) error {
	if err == nil || !force || allowForce || !strings.Contains(err.Error(), "use --force") {
		return err
	}
	return fmt.Errorf("%v — force through this MCP server needs `offshoot mcp -allow-force`; ask the human", err)
}

// guardProtectedSafetyFork refuses destroying db@branch, changing its TTL,
// or promoting onto it, through MCP without -allow-force when branch is
// ITSELF the safety fork (<target>-pre-rollback or <target>-pre-promote) of
// some OTHER branch that is protected — e.g. a plain `offshoot_destroy
// main-pre-rollback` when `main` is protected, or `offshoot_promote` with
// `target: "main-pre-rollback"` (which would repoint, i.e. destroy, the
// undo point just as surely as an actual destroy). Without this guard,
// neither refuseForceOnProtected nor ops' own protected check catches this:
// both check whether the branch NAMED in the call is protected, and a
// safety fork itself is never protected, only the branch it was taken from
// is. Callers pass the branch actually being destroyed/touched/promoted
// onto, not the protected target.
//
// It reads branch's own ref for its Meta marker (RollbackBackupMetaKey or
// PromoteBackupMetaKey names the branch the fork was taken from). branch not
// existing at all (store.ErrNotFound) means ok=false (not refused): branch
// genuinely isn't anyone's safety fork, and the caller's own subsequent ops
// call surfaces its own "no such branch" error — there is nothing here to
// fail closed ABOUT. Any OTHER error reading branch's own ref, though, fails
// CLOSED the same way the target read below does: this guard cannot tell
// whether branch carries a marker or not, and a false "not a safety fork"
// from a transient blip is exactly the failure mode the target-read fix
// closed — refusing here for the same reason keeps the two reads consistent
// rather than only protecting one of the two lookups this function makes.
//
// Once a marker names a target, this also fails CLOSED on that target ref's
// own read: unlike refuseForceOnProtected (which downgrades force to false
// and lets ops' own unforced protected check backstop a failed read), there
// is no downstream backstop here — the safety fork itself is never
// protected, so an ops call against it sails through regardless. A
// transient error reading the target's ref must not silently let a
// protected branch's undo point be destroyed/repointed/TTL-changed just
// because this one read blipped.
func (t *OffshootTools) guardProtectedSafetyFork(db, branch string) (ToolResult, bool) {
	ref, _, err := t.ws.Store.GetRef(db, branch)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ToolResult{}, false
		}
		return ErrorResult("cannot read %s@%s (%v); refusing without -allow-force", db, branch, err), true
	}
	target := ref.Meta[ops.RollbackBackupMetaKey]
	if target == "" {
		target = ref.Meta[ops.PromoteBackupMetaKey]
	}
	if target == "" {
		return ToolResult{}, false
	}
	tref, _, err := t.ws.Store.GetRef(db, target)
	if err != nil {
		return ErrorResult("cannot verify whether %s@%s is protected (%v); refusing to touch "+
			"its safety fork without -allow-force", db, target, err), true
	}
	if !tref.Protected {
		return ToolResult{}, false
	}
	return ErrorResult("%s@%s is the safety fork of protected %s@%s; without -allow-force it cannot "+
		"be destroyed, promoted onto, or have its TTL changed through MCP — ask the human", db, branch, db, target), true
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
	// extra carries additional JSON Schema keywords beyond "type"/"default"
	// (e.g. "additionalProperties" for an object-typed property like
	// `meta`), merged into the property's schema by schema() below.
	extra map[string]any
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

// optInt builds an optional integer property (e.g. `max_bytes`).
func optInt(name string) prop { return prop{name: name, jsonType: "integer"} }

// optMeta builds the optional `meta` property: a small string->string map
// stored on the new branch (fork) or the named checkpoint (checkpoint),
// capped by ops.ValidateMeta. Advertised as an object of strings so a
// model does not send a JSON-encoded string.
func optMeta(name string) prop {
	return prop{name: name, jsonType: "object", extra: map[string]any{"additionalProperties": map[string]any{"type": "string"}}}
}

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
		for k, v := range p.extra {
			def[k] = v
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

// annotate builds a fully explicit ToolAnnotations. openWorldHint is always
// false: every offshoot tool acts only on its own store, never on the open
// internet. All four hints are set on purpose — the spec defaults
// destructiveHint to TRUE, so an unannotated fork would read as destructive
// to a host that honors the hints.
func annotate(title string, readOnly, destructive, idempotent bool) *ToolAnnotations {
	f := false
	return &ToolAnnotations{
		Title:           title,
		ReadOnlyHint:    &readOnly,
		DestructiveHint: &destructive,
		IdempotentHint:  &idempotent,
		OpenWorldHint:   &f,
	}
}

// Tools returns the nine lifecycle tools this server exposes. Descriptions
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
			Annotations: annotate("List databases and branches", true, false, true),
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
			Annotations: annotate("Materialize a branch to a SQLite file", false, false, true),
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
				"`branch` defaults to \"main\" if omitted. Optional `meta` " +
				"(string->string, at most 32 keys) tags the result with your run id, " +
				"git SHA, or agent name for later lookup.",
			InputSchema: schema(reqStr("database"), reqStr("name"), optStrDefault("branch", "main"),
				optMeta("meta")),
			Annotations: annotate("Checkpoint a branch", false, false, false),
		},
		{
			Name: "offshoot_fork",
			Description: "Create an isolated copy of a database branch before attempting " +
				"risky or destructive work (schema migrations, bulk deletes, " +
				"experiments). Fork storage starts with two small metadata objects, " +
				"regardless of database size. Forking a named checkpoint does not read " +
				"database contents; the default at-head fork hashes the checkout to warn " +
				"about uncheckpointed changes. " +
				"Prefer forking over backing up by hand. Forks from the branch's " +
				"current head by default, or from a named checkpoint via `at`. If a " +
				"daemon session is open on the source branch, its unflushed writes are " +
				"flushed first, so the fork always includes everything written so far. " +
				"`branch` (the source) defaults to \"main\" if omitted. " +
				forkTTLDescription(t.defaultTTL) + " " + forkTTLJanitorNote + " " +
				"Optional `meta` (string->string, at most 32 keys) tags the result " +
				"with your run id, git SHA, or agent name for later lookup.",
			InputSchema: schema(reqStr("database"), reqStr("new_branch"),
				optStrDefault("branch", "main"), optStr("at"),
				optStrDefault("ttl", ttlDefaultDisplay(t.defaultTTL)), optMeta("meta")),
			Annotations: annotate("Fork a branch", false, false, false),
		},
		{
			Name: "offshoot_rollback",
			Description: "Return a branch to a previously named checkpoint, discarding " +
				"everything written since. Call this when an attempt on a branch has " +
				"gone wrong and you want to restore known-good state rather than " +
				"manually undoing changes. Reports the checkout path to reopen after " +
				"the rollback. `branch` defaults to \"main\" if omitted. The branch's " +
				"previous head is kept first as a TTL'd safety fork `<branch>-pre-rollback` " +
				"(one per branch, replaced by the next rollback; the safety fork lives at " +
				"least 24h), undone by a promote of that fork back onto `branch` — through " +
				"MCP only if `branch` is unprotected or this server allows force; otherwise " +
				"that's the human's CLI promote. If a daemon " +
				"session is open on this branch, the call is refused instead of " +
				"proceeding, since rollback repoints the branch's storage out from " +
				"under a session the daemon still believes it owns — close the " +
				"session first (e.g. `offshoot session close`) and retry.",
			InputSchema: schema(reqStr("database"), reqStr("to"), optStrDefault("branch", "main")),
			Annotations: annotate("Roll a branch back to a checkpoint", false, true, false),
		},
		{
			Name: "offshoot_promote",
			Description: "Ship a winning attempt: repoint the target branch (often `main`) " +
				"at the source branch's current head, which resets the target's " +
				"checkpoint history to just the new promote checkpoint. The target's " +
				"previous head is kept first as a shared safety fork named " +
				"`<target>-pre-promote` (TTL'd, at least 24h; one per target, replaced by " +
				"the next promote), so a promote is undone by promoting that fork back onto " +
				"the target. Call this once " +
				"you've validated a forked attempt and are ready to make it the branch " +
				"of record. Protected branches (main is protected by default) refuse promotion. " +
				"`force` is honored only when the server was started with -allow-force; " +
				"otherwise a protected target refuses and the answer is to ask the human to " +
				"promote from the CLI, or work on a fork. A protected branch's own safety fork " +
				"(`<branch>-pre-rollback`/`<branch>-pre-promote`) cannot be a target without " +
				"-allow-force either, even though the fork itself is never protected — " +
				"promoting onto it would repoint (destroy) the undo point it exists to " +
				"preserve. If a daemon session is open on the TARGET branch, the call " +
				"is refused instead of proceeding — `force` does not override this — " +
				"since promoting repoints the target's storage out from under a session " +
				"the daemon still believes it owns; close the session first (e.g. " +
				"`offshoot session close`) and retry. An open session on the SOURCE " +
				"does not block the call, but the promoted state is the source's " +
				"last-flushed/checkpointed head, not any write still unflushed in that " +
				"live session — flush or checkpoint the source first if you need its " +
				"very latest state promoted.",
			InputSchema: schema(reqStr("database"), reqStr("source"), reqStr("target"), optBool("force")),
			Annotations: annotate("Promote a branch onto a target", false, true, false),
		},
		{
			Name: "offshoot_destroy",
			Description: "Permanently discard a branch and its checkout. Call this to " +
				"clean up a failed or abandoned attempt once you're done with it. " +
				"`force` is honored only when the server was started with -allow-force; " +
				"otherwise a protected branch or a live lease refuses and the answer is to " +
				"ask the human to destroy from the CLI, or work on a fork. If a daemon session " +
				"is open on this branch, the call is refused instead of proceeding — " +
				"`force` does not override this — since destroy deletes the branch's " +
				"storage out from under a session the daemon still believes it owns; " +
				"close the session first (e.g. `offshoot session close`) and retry.",
			InputSchema: schema(reqStr("database"), reqStr("branch"), optBool("force")),
			Annotations: annotate("Destroy a branch", false, true, false),
		},
		{
			Name: "offshoot_touch",
			Description: "Reset a branch's activity clock so its TTL does not expire mid-task, and " +
				"optionally change the TTL. Call this when an attempt on a TTL'd fork is taking " +
				"longer than expected, or before handing a fork to a long-running step. `ttl` " +
				"omitted keeps the current TTL; a Go duration like \"2h\" sets it; \"none\" clears " +
				"it so the branch never expires (prefer a longer duration over \"none\" — " +
				"branches without a TTL are only removed by an explicit destroy). A TTL is " +
				"enforced by the daemon's janitor, by `offshoot gc`, and — when no daemon is " +
				"running — by this server's own timer (`offshoot mcp -reap-every`, default " +
				"60s), so shortening a TTL takes effect within about a minute.",
			InputSchema: schema(reqStr("database"), optStrDefault("branch", "main"), optStr("ttl")),
			Annotations: annotate("Extend a branch's life", false, false, true),
		},
		{
			Name: "offshoot_diff",
			Description: "Compare two branches (or checkpoints, `branch@checkpoint`) of one " +
				"database and report, per table, rows added, removed, and changed plus " +
				"schema changes — without sqldiff. Call this to decide which attempt to " +
				"promote, to check what a migration changed against a checkpoint, or to " +
				"compare an attempt with a golden checkpoint. `table` narrows to one table. " +
				"`full` also returns the SQL statements that turn left into right (needs " +
				"sqldiff on the host), capped at `max_bytes` (default 32768, at most " +
				"262144) with `truncated` set when cut; prefer the summary first and " +
				"`full` with `table` for a drill-down. Read-only: never touches a live " +
				"checkout, never takes a lease; a head-side branch reads its last durable " +
				"(flushed/checkpointed) state.",
			InputSchema: schema(reqStr("database"), reqStr("left"), reqStr("right"),
				optStr("table"), optBool("full"), optInt("max_bytes")),
			Annotations: annotate("Compare two branches or checkpoints", true, false, true),
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
	case "offshoot_touch":
		return t.touch(args)
	case "offshoot_diff":
		return t.diff(args)
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
		return StructuredResult(map[string]any{"branches": []map[string]any{}},
			"no databases yet; create one with the offshoot CLI (`offshoot create <name>`)"), nil
	}
	var b []byte
	rows := make([]map[string]any, 0, len(statuses))
	for _, s := range statuses {
		line := fmt.Sprintf("%s@%s head=%d checkpoints=%v protected=%v checked_out=%v\n",
			s.DB, s.Branch, s.HeadTXID, s.Checkpoints, s.Protected, s.CheckedOut)
		b = append(b, line...)
		rows = append(rows, map[string]any{
			"database": s.DB, "branch": s.Branch, "head_txid": s.HeadTXID,
			"checkpoints": s.Checkpoints, "protected": s.Protected, "checked_out": s.CheckedOut,
		})
	}
	return StructuredResult(map[string]any{"branches": rows}, "%s", string(b)), nil
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
			return StructuredResult(map[string]any{
				"database": a.Database, "branch": branch, "path": info.Checkout, "live": true,
			}, "%s", msg), nil
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
	return StructuredResult(map[string]any{
		"database": a.Database, "branch": branch, "path": path, "live": false,
	}, "%s", msg), nil
}

type checkpointArgs struct {
	Database string            `json:"database"`
	Branch   string            `json:"branch"`
	Name     string            `json:"name"`
	Meta     map[string]string `json:"meta"`
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
		resp, err := daemon.Call(t.socket, daemon.Request{Op: "flush", DB: a.Database, Branch: branch, Name: a.Name, Meta: a.Meta})
		if err != nil {
			return ErrorResult("%v", err), nil
		}
		return StructuredResult(map[string]any{
			"database": a.Database, "branch": branch, "name": a.Name, "txid": resp.TXID, "live": true,
		}, "checkpointed %s@%s as %q at txid %d — captured live from the open daemon session, no pause in writes",
			a.Database, branch, a.Name, resp.TXID), nil
	}
	txid, err := t.ws.Checkpoint(a.Database, branch, a.Name, a.Meta)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	return StructuredResult(map[string]any{
		"database": a.Database, "branch": branch, "name": a.Name, "txid": txid, "live": false,
	}, "checkpointed %s@%s as %q at txid %d", a.Database, branch, a.Name, txid), nil
}

type forkArgs struct {
	Database  string `json:"database"`
	Branch    string `json:"branch"`
	NewBranch string `json:"new_branch"`
	At        string `json:"at"`
	// TTL is a Go duration string ("2h"), "none" for no TTL (overrides any
	// configured default), or "" to fall back to OffshootTools.defaultTTL —
	// see resolveForkTTL.
	TTL  string            `json:"ttl"`
	Meta map[string]string `json:"meta"`
}

// forkTTLJanitorNote is appended to offshoot_fork's Description (and, in
// spirit, its response text — see forkTTLSummaryFromFields) per the PM
// amendment: a TTL alone does not reap anything. Reaping is a janitor's job
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
// otherwise time.Duration.String() with trailing zero units trimmed
// ("24h", not "24h0m0s") — the human form every doc uses. Only this
// schema display is trimmed; a ref's stored TTL still round-trips in the
// full canonical form (see the README's TTLs section).
func ttlDefaultDisplay(defaultTTL time.Duration) string {
	if defaultTTL <= 0 {
		return "none"
	}
	s := defaultTTL.String()
	s = strings.TrimSuffix(s, "0s")
	s = strings.TrimSuffix(s, "0m")
	if s == "" { // defensive: a sub-second default trims to nothing
		return defaultTTL.String()
	}
	return s
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

// forkTTLFieldsFromRef derives the raw ttl and expires_at values fork's
// structuredContent and prose both report, from a single already-read ref
// (or read error) — the one GetRef call the caller made — rather than each
// re-reading the store independently. That single-read discipline matters:
// two independent re-reads (one for the prose, one for the structured
// fields) could observe two different ref states across a concurrent touch
// or reap, making the response's own prose and structuredContent disagree
// with each other. ttl<=0 means no TTL was applied to this fork (both
// return ""); readErr non-nil means the post-fork re-read failed (ttlStr
// falls back to the requested ttl's own String(), expiresAt stays "" since
// there's no ref to compute a deadline from); otherwise ttlStr is the ref's
// own TTL and expiresAt is the RFC3339 deadline when ops.ReapDeadline can
// compute one, else "".
func forkTTLFieldsFromRef(ref store.Ref, readErr error, ttl time.Duration) (ttlStr, expiresAt string) {
	if ttl <= 0 {
		return "", ""
	}
	if readErr != nil {
		return ttl.String(), ""
	}
	deadline, ok := ops.ReapDeadline(ref)
	if !ok {
		return ref.TTL, ""
	}
	return ref.TTL, deadline.Format(time.RFC3339)
}

// forkTTLSummaryFromFields renders forkTTLFieldsFromRef's output into the
// prose clause fork's response appends after "forked ... at txid N". Kept
// separate from forkTTLFieldsFromRef so fork can read the ref once and
// derive both the prose and the structured fields from that one read,
// rather than each needing its own re-read of the store.
func forkTTLSummaryFromFields(ttlStr, expiresAt string) string {
	if ttlStr == "" {
		return "ttl=none (never expires)"
	}
	if expiresAt == "" {
		return fmt.Sprintf("ttl=%s; %s", ttlStr, forkTTLJanitorNote)
	}
	return fmt.Sprintf("ttl=%s expires_at=%s; %s", ttlStr, expiresAt, forkTTLJanitorNote)
}

// fork creates new_branch from branch's head (or checkpoint `at`). If a
// daemon is up (whether or not it has a session open on the source — the
// daemon's own "fork" op handles both), the fork is routed through it, so
// an open source session's unflushed writes are flushed first and always
// land in the new branch. No daemon routes to the plain at-rest fork, as
// before. Either way, the response's TTL clause and its structured `ttl`/
// `expires_at` fields both derive from a single post-fork ref read, via
// forkTTLFieldsFromRef, so the prose and structuredContent never disagree.
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
		req := daemon.Request{Op: "fork", DB: a.Database, Branch: branch, Name: a.NewBranch, From: a.At, Meta: a.Meta}
		if ttl > 0 {
			req.TTL = ttl.String()
		}
		resp, err := daemon.Call(t.socket, req)
		if err != nil {
			return ErrorResult("%v", err), nil
		}
		txid = resp.TXID
	} else {
		txid, err = t.ws.Fork(a.Database, branch, a.NewBranch, a.At, ttl, a.Meta)
		if err != nil {
			return ErrorResult("%v", err), nil
		}
	}
	msg := fmt.Sprintf("forked %s@%s to %s@%s at txid %d", a.Database, branch, a.Database, a.NewBranch, txid)
	// One read (only when there's a TTL to report on), shared by the prose
	// and the structured fields — see forkTTLFieldsFromRef's doc comment for
	// why two independent re-reads here would be a race.
	var ttlStr, expiresAt string
	if ttl > 0 {
		ref, _, refErr := t.ws.Store.GetRef(a.Database, a.NewBranch)
		ttlStr, expiresAt = forkTTLFieldsFromRef(ref, refErr, ttl)
	}
	msg += "; " + forkTTLSummaryFromFields(ttlStr, expiresAt)
	return StructuredResult(map[string]any{
		"database": a.Database, "branch": branch, "new_branch": a.NewBranch, "txid": txid,
		"ttl": ttlStr, "expires_at": expiresAt,
	}, "%s", msg), nil
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
// The branch's previous head is always kept first as a safety fork (see
// promote's identical always-on backup and t.defaultTTL comment there), at
// a TTL floored to ops.DefaultPromoteBackupTTL (24h) regardless of a shorter
// -default-ttl — see the same floor in promote.
//
// Two guards sit in front of that safety fork specifically when branch is
// protected and this server does not allow force: (1) if branch already has
// a `<branch>-pre-rollback` from an earlier rollback, this refuses outright
// rather than silently replacing an undo point the human may still need
// (only a protected branch gets this treatment — an unprotected branch's
// own safety fork is the agent's to manage); (2) if the daemon has a
// session open on that safety-fork name, this refuses the same way
// opRollback's daemon-side guard does (refuseIfSessionOpen), since
// replacing it would otherwise surface as an ops lease error instead of a
// clear MCP refusal. Whether branch is protected also decides how the
// result phrases its own undo instructions: see the res.Backup clause below.
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

	// protected reflects branch's OWN ref, read once and reused both for the
	// already-has-a-safety-fork guard below and for the undo wording after a
	// successful rollback — a GetRef failure here (e.g. no such branch) just
	// leaves protected false; RollbackWith's own GetRef reports the real
	// error a moment later.
	ref, _, refErr := t.ws.Store.GetRef(a.Database, branch)
	protected := refErr == nil && ref.Protected

	backupName := branch + ops.RollbackBackupSuffix
	if !t.allowForce && protected {
		bref, _, err := t.ws.Store.GetRef(a.Database, backupName)
		switch {
		case err == nil:
			if bref.Meta[ops.RollbackBackupMetaKey] == branch {
				return ErrorResult("%s@%s already has a safety fork %s@%s from an earlier rollback; "+
					"rolling back again would replace it — ask the human to promote or destroy it first",
					a.Database, branch, a.Database, backupName), nil
			}
		case errors.Is(err, store.ErrNotFound):
			// No existing safety fork at that name: nothing to protect yet.
		default:
			// Fail closed, same reasoning as guardProtectedSafetyFork: a
			// transient error here must not silently let this rollback
			// replace a safety fork that, for all we know, actually exists.
			return ErrorResult("cannot verify whether %s@%s already has a safety fork (%v); "+
				"refusing to roll back without -allow-force", a.Database, backupName, err), nil
		}
	}
	if r, refused := t.refuseIfSessionOpen(a.Database, backupName, "replacing its safety fork"); refused {
		return r, nil
	}

	// The safety fork's TTL follows the configured fork default, floored at
	// ops.DefaultPromoteBackupTTL (24h) so a short -default-ttl can't shrink
	// the undo window — see promote's identical floor.
	backupTTL := max(t.defaultTTL, ops.DefaultPromoteBackupTTL)
	res, err := t.ws.RollbackWith(a.Database, branch, a.To, ops.RollbackOptions{BackupTTL: backupTTL})
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	sc := map[string]any{
		"database": a.Database, "branch": branch, "to": a.To, "path": res.Path, "backup": res.Backup,
	}
	if res.Backup == "" {
		return StructuredResult(sc, "rolled back %s@%s to checkpoint %q; checkout at %s", a.Database, branch, a.To, res.Path), nil
	}
	// A protected branch (main, typically) can't actually be undone through
	// MCP unless this server allows force — offshoot_promote onto it would
	// itself be refused (refuseForceOnProtected) — so the undo clause must
	// not claim an agent can do it; it names the human's CLI promote
	// instead. See item 1 of the guardrails final-review fix wave.
	undo := fmt.Sprintf("undo: offshoot_promote it back onto %s", branch)
	if protected && !t.allowForce {
		undo = fmt.Sprintf("undo: ask the human to run `offshoot promote %s@%s --onto %s --force`",
			a.Database, res.Backup, branch)
	}
	// The backup clause is inserted BEFORE "checkout at %s" (rather than
	// appended after it) so the checkout path stays the message's trailing
	// token — callers/tests that pull "the last path-shaped word" out of the
	// prose (see lastPath in tools_test.go) still find it.
	return StructuredResult(sc, "rolled back %s@%s to checkpoint %q; the previous head is kept as %s@%s (%s); checkout at %s",
		a.Database, branch, a.To, a.Database, res.Backup, undo, res.Path), nil
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
//
// Before any of that, if TARGET is itself the safety fork of some OTHER
// protected branch (e.g. `target: "main-pre-rollback"` while `main` is
// protected), guardProtectedSafetyFork refuses without -allow-force:
// promoting onto a safety fork repoints — i.e. destroys — the very undo
// point the fork exists to preserve, just as surely as an actual destroy,
// and refuseForceOnProtected below only checks whether TARGET ITSELF is
// protected, which a safety fork never is.
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
	if !t.allowForce {
		if r, refused := t.guardProtectedSafetyFork(a.Database, a.Target); refused {
			return r, nil
		}
	}
	if r, refused := t.refuseIfSessionOpen(a.Database, a.Target, "promoting onto it"); refused {
		return r, nil
	}
	r, refused, eff := t.refuseForceOnProtected(a.Database, a.Target, a.Force)
	if refused {
		return r, nil
	}
	origForce := a.Force
	a.Force = eff
	// The safety fork's TTL follows the configured fork default when one is
	// set and it's at least ops.DefaultPromoteBackupTTL (24h); a shorter
	// -default-ttl (or none) still floors at 24h — an operator tuning fork
	// TTLs down for throwaway attempts must not, as a side effect, shrink
	// this safety fork's own undo window below a day.
	res, err := t.ws.PromoteWith(a.Database, a.Source, a.Target,
		ops.PromoteOptions{Force: a.Force, BackupTTL: max(t.defaultTTL, ops.DefaultPromoteBackupTTL)})
	if err != nil {
		return ErrorResult("%v", wrapForceRefusal(err, origForce, t.allowForce)), nil
	}
	sc := map[string]any{
		"database": a.Database, "source": a.Source, "target": a.Target, "txid": res.TXID, "backup": res.Backup,
	}
	if res.Backup == "" {
		return StructuredResult(sc, "promoted %s@%s onto %s@%s at txid %d", a.Database, a.Source, a.Database, a.Target, res.TXID), nil
	}
	return StructuredResult(sc, "promoted %s@%s onto %s@%s at txid %d; the previous %s@%s head is kept as %s@%s (undo: promote it back onto %s)",
		a.Database, a.Source, a.Database, a.Target, res.TXID, a.Database, a.Target, a.Database, res.Backup, a.Target), nil
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
// session the daemon still believes owns it. Before any of that, if branch
// is itself the safety fork of some OTHER protected branch (e.g.
// `main-pre-rollback` while `main` is protected), guardProtectedSafetyFork
// refuses without -allow-force — refuseForceOnProtected below only checks
// whether BRANCH ITSELF is protected, which a safety fork never is.
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
	if !t.allowForce {
		if r, refused := t.guardProtectedSafetyFork(a.Database, a.Branch); refused {
			return r, nil
		}
	}
	if r, refused := t.refuseIfSessionOpen(a.Database, a.Branch, "destroying it"); refused {
		return r, nil
	}
	r, refused, eff := t.refuseForceOnProtected(a.Database, a.Branch, a.Force)
	if refused {
		return r, nil
	}
	origForce := a.Force
	a.Force = eff
	if err := t.ws.Destroy(a.Database, a.Branch, a.Force); err != nil {
		return ErrorResult("%v", wrapForceRefusal(err, origForce, t.allowForce)), nil
	}
	return StructuredResult(map[string]any{
		"database": a.Database, "branch": a.Branch,
	}, "destroyed %s@%s", a.Database, a.Branch), nil
}

type touchArgs struct {
	Database string `json:"database"`
	Branch   string `json:"branch"`
	// TTL: "" keeps the current TTL, "none" clears it, a Go duration sets it.
	TTL string `json:"ttl"`
}

// touch resets db@branch's activity clock (ops.Touch: CAS-retried, refuses
// a branch a reaper has already claimed) and optionally sets/clears its TTL.
// Safe alongside an open daemon session: the ref CAS races only lease
// renewals, and ops.Touch retries. Changing the TTL (a.TTL != "") of a
// branch that is itself the safety fork of some OTHER protected branch is
// guarded the same way destroy() is (guardProtectedSafetyFork): a human who
// protected `main` gets to decide how long `main-pre-rollback` lives, too. A
// plain touch (a.TTL == "", extending life only) is never guarded — it can
// only push the deadline further out, never pull it in or clear it.
func (t *OffshootTools) touch(args json.RawMessage) (ToolResult, error) {
	var a touchArgs
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
	if a.TTL != "" && !t.allowForce {
		if r, refused := t.guardProtectedSafetyFork(a.Database, branch); refused {
			return r, nil
		}
	}
	var ttl *time.Duration
	switch a.TTL {
	case "":
	case "none":
		var zero time.Duration
		ttl = &zero
	default:
		d, err := time.ParseDuration(a.TTL)
		if err != nil || d <= 0 {
			return ErrorResult("ttl must be a positive Go duration (e.g. \"2h\") or \"none\", got %q", a.TTL), nil
		}
		ttl = &d
	}
	ref, err := t.ws.Touch(a.Database, branch, ttl, time.Now())
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	shown := ref.TTL
	if shown == "" {
		shown = "none"
	}
	return StructuredResult(
		map[string]any{"database": a.Database, "branch": branch, "ttl": ref.TTL, "touched_at": ref.TouchedAt},
		"touched %s@%s ttl=%s touched_at=%s", a.Database, branch, shown, ref.TouchedAt), nil
}

type diffArgs struct {
	Database string `json:"database"`
	Left     string `json:"left"`
	Right    string `json:"right"`
	Table    string `json:"table"`
	Full     bool   `json:"full"`
	MaxBytes int    `json:"max_bytes"`
}

const (
	diffDefaultMaxBytes = 32 << 10
	diffMaxBytesCeiling = 256 << 10
)

// splitSide parses "branch" or "branch@checkpoint" (never a database: MCP
// diff is scoped to one database) and validates both names.
func splitSide(arg, side string) (branch, checkpoint string, r ToolResult, bad bool) {
	parts := strings.Split(arg, "@")
	switch len(parts) {
	case 1:
		branch = parts[0]
	case 2:
		branch, checkpoint = parts[0], parts[1]
		if checkpoint == "" {
			// "branch@" — an empty checkpoint after the "@" is a malformed
			// shape, not a way to spell "head" (that's plain "branch", no
			// "@" at all).
			return "", "", ErrorResult("%s must be branch or branch@checkpoint, got %q", side, arg), true
		}
	default:
		return "", "", ErrorResult("%s must be branch or branch@checkpoint, got %q", side, arg), true
	}
	if branch == "" {
		return "", "", ErrorResult("%s needs a branch name", side), true
	}
	named := []named{namedArg(side+" branch", branch)}
	if checkpoint != "" {
		named = append(named, namedArg(side+" checkpoint", checkpoint))
	}
	if r, bad := validateNames(named...); bad {
		return "", "", r, true
	}
	return branch, checkpoint, ToolResult{}, false
}

// diff is read-only: both sides materialize through MaterializeForDiff (the
// ro-cache for a checkpoint, a private fresh export for a head) and are
// closed before returning. Summary needs no sqldiff; full shells out to it
// with a byte cap sized for a model's context, not a file.
func (t *OffshootTools) diff(args json.RawMessage) (ToolResult, error) {
	var a diffArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return ErrorResult("invalid arguments: %v", err), nil
	}
	if a.Database == "" || a.Left == "" || a.Right == "" {
		return ErrorResult("database, left, and right are required"), nil
	}
	if r, bad := validateNames(namedArg("database", a.Database)); bad {
		return r, nil
	}
	lbr, lcp, r, bad := splitSide(a.Left, "left")
	if bad {
		return r, nil
	}
	rbr, rcp, r, bad := splitSide(a.Right, "right")
	if bad {
		return r, nil
	}
	if a.MaxBytes < 0 || a.MaxBytes > diffMaxBytesCeiling {
		return ErrorResult("max_bytes must be 0..%d", diffMaxBytesCeiling), nil
	}
	maxBytes := a.MaxBytes
	if maxBytes == 0 {
		maxBytes = diffDefaultMaxBytes
	}
	left, err := t.ws.MaterializeForDiff(a.Database, lbr, lcp)
	if err != nil {
		return ErrorResult("left: %v", err), nil
	}
	defer left.Close()
	right, err := t.ws.MaterializeForDiff(a.Database, rbr, rcp)
	if err != nil {
		return ErrorResult("right: %v", err), nil
	}
	defer right.Close()

	tables, err := ops.DiffSummary(left.Path, right.Path)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	if a.Table != "" {
		var only []ops.TableDiff
		for _, d := range tables {
			if d.Table == a.Table {
				only = append(only, d)
			}
		}
		if len(only) == 0 {
			return ErrorResult("no table %q on either side", a.Table), nil
		}
		tables = only
	}
	rep := ops.DiffReportOf(tables)
	leftLabel := a.Database + "@" + a.Left
	rightLabel := a.Database + "@" + a.Right
	var b strings.Builder
	fmt.Fprintf(&b, "left:  %s right: %s\n", leftLabel, rightLabel)
	if err := ops.FormatDiffSummary(&b, rep, leftLabel, rightLabel); err != nil {
		return ErrorResult("%v", err), nil
	}
	rows := make([]map[string]any, 0, len(rep.Tables))
	for _, d := range rep.Tables {
		rows = append(rows, map[string]any{
			"table": d.Table, "left_exists": d.LeftExists, "right_exists": d.RightExists,
			"left_rows": d.Left, "right_rows": d.Right, "comparable": d.Comparable,
			"added": d.Added, "removed": d.Removed, "changed": d.Changed,
			"schema_changed": d.SchemaChanged, "key": d.Key, "status": d.Status,
		})
	}
	sc := map[string]any{
		"database": a.Database, "left": a.Left, "right": a.Right, "tables": rows,
		// totals is over the post-table-filter list (rep.Tables), so a
		// narrowed `table` call reports counts for just that one table, not
		// the whole diff.
		"totals": map[string]any{"same": rep.Totals.Same, "changed": rep.Totals.Changed, "added": rep.Totals.Added, "removed": rep.Totals.Removed},
	}
	if a.Full {
		text, truncated, err := ops.SqldiffCapped(left.Path, right.Path, a.Table, maxBytes)
		if err != nil {
			if errors.Is(err, ops.ErrSqldiffMissing) {
				return ErrorResult("full diff needs the sqldiff binary on this host (it is not installed); the summary above needs nothing — call again without full"), nil
			}
			return ErrorResult("%v", err), nil
		}
		if truncated {
			fmt.Fprintf(&b, "\n-- sqldiff (truncated at %d bytes; narrow with table or raise max_bytes)\n", maxBytes)
		} else {
			b.WriteString("\n-- sqldiff\n")
		}
		b.WriteString(text)
		sc["full"], sc["truncated"] = text, truncated
	}
	return StructuredResult(sc, "%s", b.String()), nil
}

// reapOnce runs a single at-rest reap pass, or reports that it deferred to
// a running daemon instead. skipped is true whenever t.daemonStatus()
// reports a daemon reachable at all (any answer, not just a healthy
// session — see daemonStatus's own ok contract): that daemon's own janitor
// (see internal/daemon/janitor.go's StartJanitor/janitorTick) already reaps
// this exact store on its own cadence, and a second reaper racing it here
// would be a second writer against the same store with no coordination
// between them — the CAS in ops.Workspace.Reap/reapOne makes a race between
// the two safe from corruption, but not from surprising log lines and
// duplicate "reaped" work attributed to the wrong process. skipped=true
// leaves the store completely untouched: neither Reap nor
// ClearStaleDeleteClaims is called at all.
//
// When no daemon is reachable, this mirrors janitorTick's own order and
// error handling for the two calls it shares with the janitor (Reap, then
// ClearStaleDeleteClaims — no GC; see StartReaper's doc comment for why):
// both calls run regardless of whether the first failed (each keeps doing
// everything it safely can and reports a partial result alongside its own
// error, per their doc comments), and the first error either one hits wins
// (mirroring ops.Workspace.Reap's own firstErr pattern), but reaped is
// still whatever Reap actually destroyed even when ClearStaleDeleteClaims
// (or Reap itself) errors afterward.
func (t *OffshootTools) reapOnce(now time.Time) (reaped []string, skipped bool, err error) {
	if _, up := t.daemonStatus(); up {
		return nil, true, nil
	}
	var firstErr error
	reaped, err = t.ws.Reap(now)
	if err != nil {
		firstErr = err
	}
	if _, cerr := t.ws.ClearStaleDeleteClaims(now); cerr != nil {
		if firstErr == nil {
			firstErr = cerr
		} else {
			// Both steps failed this pass: firstErr (Reap's) is what this
			// call returns, per this function's own doc comment, but that
			// would otherwise silently drop ClearStaleDeleteClaims' error
			// on the floor. janitorTick logs each step's error on its own
			// line regardless of whether an earlier step also failed (see
			// janitor.go) — mirrored here so an operator watching stderr
			// still learns about this failure too, not just the first one.
			fmt.Fprintf(os.Stderr, "offshoot mcp: clear stale delete claims: %v\n", cerr)
		}
	}
	return reaped, false, firstErr
}

// StartReaper runs reapOnce on a ticker, every `every`, until ctx is done —
// the `offshoot mcp -reap-every` fallback for a store with no `offshoot
// serve` daemon running, so a TTL set via offshoot_fork's `ttl` argument
// (or -default-ttl) is eventually enforced even when nothing else is
// reaping this store. every <= 0 disables the reaper entirely (no
// goroutine started), matching StartJanitor's own contract for the same
// shape of flag (see cmd/offshoot/main.go's -reap-every for `offshoot mcp`
// and `offshoot serve`).
//
// Deliberately does NOT run GC: unlike the daemon's janitor, this reaper
// has no operator-supplied grace period to run GC safely against (GC needs
// a grace long enough to exceed the longest plausible in-flight fork, and
// -reap-every here is about reap cadence, not that), and reclaiming
// tombstoned storage is not time-sensitive the way an expired TTL is —
// `offshoot gc` (manual) or a running `offshoot serve` daemon remains the
// only way to reclaim disk from this store.
//
// Every tick's outcome is logged to stderr: one "offshoot mcp: reaped
// <db@branch>" line per branch actually destroyed, any error from
// reapOnce, and — the first time (and only the first time) a tick skips
// because a daemon came up — a single "offshoot mcp: reaper skipped:
// daemon is running" line, so an operator watching stderr learns once that
// this reaper has backed off rather than seeing that line repeat forever
// on every tick a daemon happens to be up.
//
// Never panics: reapOnce's own errors are returned values, not panics, and
// this loop only ever logs them.
//
// Returns done, closed the instant the reaper's goroutine actually exits —
// already closed before this call returns when every <= 0 disabled it
// outright (no goroutine was ever started), or closed once the running
// goroutine observes ctx.Done() otherwise. This is a smaller-scoped sibling
// of StartJanitor/Shutdown's janitorWG synchronization (internal/daemon):
// that one lets Shutdown block until every daemon goroutine has stopped;
// this lets a caller (a test, chiefly — see
// TestStartReaperTicksAndStopsOnCancel in internal/mcp) prove this one
// goroutine specifically has stopped after cancelling ctx, rather than only
// ever being able to poll side effects and infer it. cmd/offshoot/main.go's
// `mcp` case doesn't need to wait on it (its own process teardown after
// srv.Serve returns is enough), so it discards the return value.
func (t *OffshootTools) StartReaper(ctx context.Context, every time.Duration) (done <-chan struct{}) {
	doneCh := make(chan struct{})
	if every <= 0 {
		close(doneCh)
		return doneCh
	}
	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		loggedSkip := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reaped, skipped, err := t.reapOnce(time.Now())
				if skipped {
					if !loggedSkip {
						fmt.Fprintln(os.Stderr, "offshoot mcp: reaper skipped: daemon is running")
						loggedSkip = true
					}
					continue
				}
				if err != nil {
					fmt.Fprintf(os.Stderr, "offshoot mcp: reap: %v\n", err)
				}
				for _, k := range reaped {
					fmt.Fprintf(os.Stderr, "offshoot mcp: reaped %s\n", k)
				}
			}
		}
	}()
	return doneCh
}
