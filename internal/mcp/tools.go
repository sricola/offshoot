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
	ws   *ops.Workspace
	spec string
	// defaultTTL is applied to a fork whose call omits `ttl` entirely;
	// zero means forks have no TTL unless one is given explicitly. An
	// explicit `ttl:"none"` on the call always overrides this, and an
	// explicit `ttl:"<duration>"` always wins over it too — see
	// resolveForkTTL.
	defaultTTL time.Duration
}

// NewOffshootTools binds a tool set to a workspace and store spec. The spec
// is used to detect a running daemon; offshoot_checkout reports whether one
// is running for this store. defaultTTL is the TTL offshoot_fork applies
// when a call omits `ttl`; zero means agent-created forks are immortal
// unless a call passes `ttl` itself (the CLI's `offshoot mcp -default-ttl`
// flag is how an operator sets this — see cmd/offshoot/main.go).
func NewOffshootTools(ws *ops.Workspace, spec string, defaultTTL time.Duration) *OffshootTools {
	return &OffshootTools{ws: ws, spec: spec, defaultTTL: defaultTTL}
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
				"directly with a SQL client. If a daemon is running for this store, the " +
				"result says so — checkouts like this one are how you work with this " +
				"branch from here. Call offshoot_checkpoint to name the current state so " +
				"it can be rolled back to or forked from later. `branch` defaults to " +
				"\"main\" if omitted.",
			InputSchema: schema(reqStr("database"), optStrDefault("branch", "main")),
		},
		{
			Name: "offshoot_checkpoint",
			Description: "Name the current state of a branch's checkout so it can be " +
				"returned to later. Call this after a batch of changes you might want " +
				"to keep or roll back to individually — e.g. after a migration step " +
				"succeeds, or before starting a riskier change on the same branch. " +
				"Cheap: only the diff since the last checkpoint is stored. `branch` " +
				"defaults to \"main\" if omitted.",
			InputSchema: schema(reqStr("database"), reqStr("name"), optStrDefault("branch", "main")),
		},
		{
			Name: "offshoot_fork",
			Description: "Create an isolated copy of a database branch before attempting " +
				"risky or destructive work (schema migrations, bulk deletes, " +
				"experiments). Forking is instant and costs nothing until you write. " +
				"Prefer forking over backing up by hand. Forks from the branch's " +
				"current head by default, or from a named checkpoint via `at`. " +
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
				"the rollback. `branch` defaults to \"main\" if omitted.",
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
				"not a bug.",
			InputSchema: schema(reqStr("database"), reqStr("source"), reqStr("target"), optBool("force")),
		},
		{
			Name: "offshoot_destroy",
			Description: "Permanently discard a branch and its checkout. Call this to " +
				"clean up a failed or abandoned attempt once you're done with it. " +
				"Protected branches refuse destruction unless `force` is set — treat " +
				"that refusal as confirmation you need, not a bug.",
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
	path, err := t.ws.Checkout(a.Database, branch)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	msg := fmt.Sprintf("checked out %s@%s at %s", a.Database, branch, path)
	msg += "\nthis checkout is not yet checkpointed: nothing written here can be rolled " +
		"back to or forked from until you call offshoot_checkpoint"
	if socket, serr := daemon.DefaultSocketPath(t.spec); serr == nil && daemon.Running(socket) {
		msg += "\na daemon is running for this store, but no MCP tool currently opens a " +
			"session against it; checkouts like this one are how you work with this " +
			"branch from here"
	}
	return TextResult("%s", msg), nil
}

type checkpointArgs struct {
	Database string `json:"database"`
	Branch   string `json:"branch"`
	Name     string `json:"name"`
}

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
		// The fork itself already succeeded; a re-read failure here just
		// means the response can't include a computed expiry.
		return fmt.Sprintf("ttl=%s", ttl)
	}
	deadline, ok := ops.ReapDeadline(ref)
	if !ok {
		return fmt.Sprintf("ttl=%s", ref.TTL)
	}
	return fmt.Sprintf("ttl=%s expires_at=%s; %s", ref.TTL, deadline.Format(time.RFC3339), forkTTLJanitorNote)
}

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
	txid, err := t.ws.Fork(a.Database, branch, a.NewBranch, a.At, ttl)
	if err != nil {
		return ErrorResult("%v", err), nil
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
	if err := t.ws.Destroy(a.Database, a.Branch, a.Force); err != nil {
		return ErrorResult("%v", err), nil
	}
	return TextResult("destroyed %s@%s", a.Database, a.Branch), nil
}
