package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/offshoot-db/offshoot/internal/daemon"
	"github.com/offshoot-db/offshoot/internal/ops"
)

// OffshootTools exposes offshoot's branch lifecycle to an agent over MCP:
// fork before risky work, checkpoint on success, roll back on failure,
// promote a winning attempt, destroy a losing one.
type OffshootTools struct {
	ws   *ops.Workspace
	spec string
}

// NewOffshootTools binds a tool set to a workspace and store spec. The spec
// is used to find a running daemon for session tools (offshoot_checkout
// reports whether one is running, so the agent knows to prefer a session
// once Plan 8's SDKs add them).
func NewOffshootTools(ws *ops.Workspace, spec string) *OffshootTools {
	return &OffshootTools{ws: ws, spec: spec}
}

// schema is a shorthand for a JSON Schema object describing a tool's
// arguments: required string properties plus optional string properties.
func schema(required []string, optional ...string) map[string]any {
	props := map[string]any{}
	for _, name := range required {
		props[name] = map[string]any{"type": "string"}
	}
	for _, name := range optional {
		props[name] = map[string]any{"type": "string"}
	}
	s := map[string]any{
		"type":       "object",
		"properties": props,
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
			InputSchema: schema(nil),
		},
		{
			Name: "offshoot_checkout",
			Description: "Materialize a database branch to a local SQLite file and return " +
				"the path to open. Call this before reading or writing a branch's data " +
				"directly with a SQL client. If a daemon is running for this store, the " +
				"result says so — prefer a session-based connection over repeated " +
				"one-shot checkouts when one is available, since it holds a lease and " +
				"streams writes incrementally instead of full-snapshotting.",
			InputSchema: schema([]string{"database"}, "branch"),
		},
		{
			Name: "offshoot_checkpoint",
			Description: "Name the current state of a branch's checkout so it can be " +
				"returned to later. Call this after a batch of changes you might want " +
				"to keep or roll back to individually — e.g. after a migration step " +
				"succeeds, or before starting a riskier change on the same branch. " +
				"Cheap: only the diff since the last checkpoint is stored.",
			InputSchema: schema([]string{"database", "name"}, "branch"),
		},
		{
			Name: "offshoot_fork",
			Description: "Create an isolated copy of a database branch before attempting " +
				"risky or destructive work (schema migrations, bulk deletes, " +
				"experiments). Forking is instant and costs nothing until you write. " +
				"Prefer forking over backing up by hand. Forks from the branch's " +
				"current head by default, or from a named checkpoint via `at`.",
			InputSchema: schema([]string{"database", "new_branch"}, "branch", "at"),
		},
		{
			Name: "offshoot_rollback",
			Description: "Return a branch to a previously named checkpoint, discarding " +
				"everything written since. Call this when an attempt on a branch has " +
				"gone wrong and you want to restore known-good state rather than " +
				"manually undoing changes. Reports the checkout path to reopen after " +
				"the rollback.",
			InputSchema: schema([]string{"database", "to"}, "branch"),
		},
		{
			Name: "offshoot_promote",
			Description: "Ship a winning attempt: repoint the target branch (often `main`) " +
				"at the source branch's current head. Call this once you've validated " +
				"a forked attempt and are ready to make it the branch of record. " +
				"Protected branches (main is protected by default) refuse promotion " +
				"unless `force` is set — treat that refusal as confirmation you need, " +
				"not a bug.",
			InputSchema: schema([]string{"database", "source", "target"}, "force"),
		},
		{
			Name: "offshoot_destroy",
			Description: "Permanently discard a branch and its checkout. Call this to " +
				"clean up a failed or abandoned attempt once you're done with it. " +
				"Protected branches refuse destruction unless `force` is set — treat " +
				"that refusal as confirmation you need, not a bug.",
			InputSchema: schema([]string{"database", "branch"}, "force"),
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
	path, err := t.ws.Checkout(a.Database, branch)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	msg := fmt.Sprintf("checked out %s@%s at %s", a.Database, branch, path)
	if socket, serr := daemon.DefaultSocketPath(t.spec); serr == nil && daemon.Running(socket) {
		msg += "\na daemon is running for this store; prefer a session-based connection " +
			"over repeated checkouts if your client supports one"
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
	txid, err := t.ws.Fork(a.Database, branch, a.NewBranch, a.At)
	if err != nil {
		return ErrorResult("%v", err), nil
	}
	return TextResult("forked %s@%s to %s@%s at txid %d", a.Database, branch, a.Database, a.NewBranch, txid), nil
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
	if err := t.ws.Destroy(a.Database, a.Branch, a.Force); err != nil {
		return ErrorResult("%v", err), nil
	}
	return TextResult("destroyed %s@%s", a.Database, a.Branch), nil
}
