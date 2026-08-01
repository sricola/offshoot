# offshoot Plan 6: MCP Server and Launch Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `offshoot mcp` — an MCP server that puts branching in the agent's own hands, so a coding agent forks before risky work, checkpoints when it succeeds, and rolls back when it doesn't, without a human driving the CLI.

**Architecture:** A stdio JSON-RPC 2.0 server implementing the MCP handshake and tool surface, dispatching to the existing `ops.Workspace` for at-rest lifecycle operations and to Plan 5's daemon client for live-session operations when a daemon is running. Destructive tools are gated by the protected-branch rules the store already enforces, so an agent can experiment freely but cannot vaporize `main` by accident.

**Tech Stack:** Go 1.24+, stdlib `encoding/json` and `bufio` (no MCP SDK dependency — the protocol surface we need is small and a hand-rolled server keeps the single-binary story), existing `internal/ops`, `internal/daemon`.

**Spec:** `docs/superpowers/specs/2026-07-29-offshoot-design.md` § Integration surface ("MCP server, day one — wraps the lifecycle ops so agents fork before risky work on their own initiative"), § Launch plan. **Plan sequence:** Plans 1-5 merged (capture spike GO; local lifecycle; S3 backends; leases and fencing; daemon with live capture) → **this plan** → Plan 7 (incremental LTX segments, TTL reaping) → Plan 8 (Python/TS SDKs, LangGraph adapter).

## Global Constraints

- Module `github.com/offshoot-db/offshoot`; Go 1.24+; cgo (mattn); Linux/macOS only
- **No new module dependencies** — the MCP server is stdlib-only over the existing internal packages
- MCP transport is stdio, one JSON-RPC 2.0 message per line, requests and responses framed as newline-delimited JSON; the server MUST NOT write anything but protocol messages to stdout (diagnostics go to stderr — a stray print corrupts the session)
- **Dual protocol support (decided 2026-08-01):** the server speaks BOTH the legacy `initialize`/`tools/list` handshake (2025-11-25) and the current 2026-07-28 revision (per-request `_meta` versioning, `server/discover`); a client on either is served correctly
- **Protocol details must be verified, not assumed:** the implementer checks the current MCP specification for exact method names, the `initialize` result shape, and the `tools/list`/`tools/call` payloads rather than trusting the plan's sketch; deviations are recorded in the report
- Destructive tools (`destroy`, `promote`) honor the protected-branch rules already in `ops` — no tool may bypass a `--force` gate the CLI enforces
- Every tool result reports what actually happened, including the durability state where relevant; never claim a write is durable when it is only committed to SQLite
- Every Plan-1..5 test must keep passing unmodified
- Tests must not depend on wall-clock sleeps for correctness — poll with deadlines
- Commit messages: conventional commits, ending with the repo's session trailers

## File Structure

```
internal/mcp/protocol.go     JSON-RPC 2.0 + MCP message types
internal/mcp/server.go       Server loop: read, dispatch, respond; initialize + tools/list
internal/mcp/server_test.go
internal/mcp/tools.go        Tool definitions and their handlers over ops/daemon
internal/mcp/tools_test.go
cmd/offshoot/main.go         (modify) `offshoot mcp` command + usage
examples/parallel-attempts/  The launch demo: script, README, expected output
README.md                    (modify) MCP setup and the agent workflow
```

---

### Task 1: JSON-RPC transport and the MCP handshake

**Files:**
- Create: `internal/mcp/protocol.go`, `internal/mcp/server.go`, `internal/mcp/server_test.go`

**Interfaces:**
- Consumes: stdlib only
- Produces:

```go
package mcp

// Request and Response are JSON-RPC 2.0 messages, one per line.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"` // absent for notifications
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Standard JSON-RPC error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// Server reads JSON-RPC messages from r and writes responses to w.
type Server struct { /* unexported */ }

// NewServer returns a server whose tools are provided by ts.
func NewServer(r io.Reader, w io.Writer, ts ToolSet) *Server

// Serve reads until EOF or ctx cancellation. It returns nil on clean EOF.
func (s *Server) Serve(ctx context.Context) error

// ToolSet is what the server exposes; Task 2 implements it over offshoot.
type ToolSet interface {
	Tools() []Tool
	Call(ctx context.Context, name string, args json.RawMessage) (ToolResult, error)
}

type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema any    `json:"inputSchema"`
}

// ToolResult is an MCP tool response: content blocks plus an error flag.
type ToolResult struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError,omitempty"`
}

type Content struct {
	Type string `json:"type"` // "text"
	Text string `json:"text"`
}

// TextResult is a convenience for a single text block.
func TextResult(format string, args ...any) ToolResult
// ErrorResult is a tool-level failure (IsError set), distinct from an RPC error.
func ErrorResult(format string, args ...any) ToolResult
```

This task implements `initialize`, `notifications/initialized` (a notification — no response), `tools/list`, and `ping` if the spec has one; `tools/call` is Task 2's. A request with an unknown method gets `CodeMethodNotFound`. Notifications (no `id`) never get a response.

**Protocol-verification authorization (mandatory first step):** the field names and the `initialize` result shape below are the plan's best understanding. Before writing code, check the current MCP specification (e.g. `WebFetch` the spec at modelcontextprotocol.io, or inspect a known-good server's traffic) for: the exact `initialize` params and result (protocol version string, `capabilities`, `serverInfo`), whether the initialized notification is `notifications/initialized`, and the exact `tools/list`/`tools/call` shapes. Implement what the spec actually says and record every deviation from this plan in your report.

- [ ] **Step 1: Verify the protocol**

Fetch the current MCP spec and note: protocol version string to advertise, `initialize` result shape, notification method name, `tools/list` result key, `tools/call` params key. Write these into your report before coding.

- [ ] **Step 2: Write the failing test**

`internal/mcp/server_test.go`:

```go
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// fakeTools is a minimal ToolSet for transport tests.
type fakeTools struct{ called string }

func (f *fakeTools) Tools() []Tool {
	return []Tool{{
		Name:        "ping_tool",
		Description: "does nothing",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
	}}
}

func (f *fakeTools) Call(ctx context.Context, name string, args json.RawMessage) (ToolResult, error) {
	f.called = name
	return TextResult("called %s", name), nil
}

// run feeds lines to a server and returns the response lines it wrote.
func run(t *testing.T, lines ...string) []map[string]any {
	t.Helper()
	in := strings.NewReader(strings.Join(lines, "\n") + "\n")
	var out bytes.Buffer
	s := NewServer(in, &out, &fakeTools{})
	if err := s.Serve(context.Background()); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	var got []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("response is not JSON: %q: %v", line, err)
		}
		got = append(got, m)
	}
	return got
}

func TestInitializeHandshake(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %d: %v", len(got), got)
	}
	r := got[0]
	if r["jsonrpc"] != "2.0" {
		t.Errorf("jsonrpc = %v", r["jsonrpc"])
	}
	if r["error"] != nil {
		t.Fatalf("initialize errored: %v", r["error"])
	}
	res, ok := r["result"].(map[string]any)
	if !ok {
		t.Fatalf("result missing: %v", r)
	}
	if res["protocolVersion"] == nil || res["capabilities"] == nil || res["serverInfo"] == nil {
		t.Errorf("initialize result incomplete: %v", res)
	}
	si, _ := res["serverInfo"].(map[string]any)
	if si["name"] != "offshoot" {
		t.Errorf("serverInfo.name = %v, want offshoot", si["name"])
	}
}

func TestNotificationGetsNoResponse(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if len(got) != 0 {
		t.Fatalf("a notification must not be answered, got %v", got)
	}
}

func TestToolsListReturnsSchemas(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if len(got) != 1 || got[0]["error"] != nil {
		t.Fatalf("tools/list: %v", got)
	}
	res := got[0]["result"].(map[string]any)
	tools, ok := res["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v", res["tools"])
	}
	tool := tools[0].(map[string]any)
	if tool["name"] != "ping_tool" || tool["inputSchema"] == nil {
		t.Fatalf("tool entry incomplete: %v", tool)
	}
}

func TestUnknownMethodIsMethodNotFound(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":3,"method":"no/such/method"}`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %v", got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("want an error response, got %v", got[0])
	}
	if int(e["code"].(float64)) != CodeMethodNotFound {
		t.Errorf("code = %v, want %d", e["code"], CodeMethodNotFound)
	}
}

func TestMalformedLineIsParseErrorAndServerContinues(t *testing.T) {
	got := run(t,
		`{not json at all`,
		`{"jsonrpc":"2.0","id":9,"method":"tools/list"}`)
	if len(got) != 2 {
		t.Fatalf("want a parse error then a normal response, got %v", got)
	}
	e := got[0]["error"].(map[string]any)
	if int(e["code"].(float64)) != CodeParseError {
		t.Errorf("first code = %v, want %d", e["code"], CodeParseError)
	}
	if got[1]["error"] != nil {
		t.Errorf("server must keep serving after a bad line: %v", got[1])
	}
}

func TestNothingButProtocolOnStdout(t *testing.T) {
	// Every line the server writes must be a JSON-RPC message; run() already
	// fails on non-JSON, so this asserts the count is exactly right for a
	// two-request session with one notification mixed in.
	got := run(t,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if len(got) != 2 {
		t.Fatalf("want exactly 2 responses (notification unanswered), got %d: %v", len(got), got)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `go test ./internal/mcp -v`
Expected: FAIL — package does not exist.

- [ ] **Step 4: Implement**

Write `protocol.go` with the types above (adjusted to the verified spec) and `server.go` with:

```go
// Package mcp implements the Model Context Protocol over stdio so an agent
// can drive offshoot's branching directly: fork before risky work, checkpoint
// on success, roll back on failure.
//
// The transport is newline-delimited JSON-RPC 2.0. Nothing but protocol
// messages may be written to the output stream — diagnostics go to stderr,
// because a stray write corrupts the client's session.
package mcp
```

`NewServer` stores the reader/writer/toolset; `Serve` loops with a `bufio.Scanner` (raise its buffer — tool arguments can exceed the 64KB default), decodes each line, and dispatches. A decode failure writes a `CodeParseError` response (with a null id) and continues. A request with no `id` is a notification: handle it and write nothing. `initialize` returns the verified result shape with `serverInfo.name = "offshoot"` and the binary's version; `tools/list` returns `{"tools": ts.Tools()}`. Respect `ctx` between messages so a cancelled server stops.

- [ ] **Step 5: Run**

Run: `go test ./internal/mcp -v -race && go test ./... -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/mcp
git commit -m "feat: MCP stdio transport with initialize and tools/list"
```

---

### Task 2: The offshoot tool set

**Files:**
- Create: `internal/mcp/tools.go`, `internal/mcp/tools_test.go`
- Modify: `internal/mcp/server.go` (add `tools/call` dispatch)

**Interfaces:**
- Consumes: `ToolSet`, `Tool`, `ToolResult`, `TextResult`, `ErrorResult` (Task 1); `ops.Workspace` (Open, Create, Checkout, CheckoutPath, Checkpoint, Fork, Rollback, Promote, Destroy, Status, ParseTarget); `daemon.DefaultSocketPath`, `daemon.Running`, `daemon.Call`, `daemon.Request`
- Produces:

```go
package mcp

// OffshootTools exposes offshoot's lifecycle to an agent.
type OffshootTools struct { /* unexported */ }

// NewOffshootTools binds a tool set to a workspace and store spec. The spec
// is used to find a running daemon for session tools.
func NewOffshootTools(ws *ops.Workspace, spec string) *OffshootTools

func (t *OffshootTools) Tools() []Tool
func (t *OffshootTools) Call(ctx context.Context, name string, args json.RawMessage) (ToolResult, error)
```

Tools, each with a JSON Schema for its arguments:

| Tool | Arguments | Behavior |
|---|---|---|
| `offshoot_list` | none | every database and branch with head txid, checkpoints, protected flag |
| `offshoot_checkout` | `database`, optional `branch` | materialize and return the path the agent should open |
| `offshoot_checkpoint` | `database`, optional `branch`, `name` | name the current state |
| `offshoot_fork` | `database`, optional `branch`, `new_branch`, optional `at` | branch from head or a checkpoint |
| `offshoot_rollback` | `database`, optional `branch`, `to` | return to a checkpoint; reports the new checkout path |
| `offshoot_promote` | `database`, `source`, `target`, optional `force` | ship a winning attempt |
| `offshoot_destroy` | `database`, `branch`, optional `force` | discard an attempt |

Rules that matter: `branch` defaults to `main`; every tool validates its arguments and returns `ErrorResult` (not an RPC error) for a user-level failure like "no such checkpoint", reserving RPC errors for protocol faults; `promote` and `destroy` surface the protected-branch refusal verbatim so the agent learns it needs `force`; `checkout` reports whether a daemon is running and, if so, that the agent should prefer a session — but this plan does not add session tools (they need lifecycle beyond a single tool call; Plan 8's SDKs cover that).

Tool descriptions are the agent's only documentation — write them so a model knows *when* to reach for each one, not just what it does. For example, `offshoot_fork`: "Create an isolated copy of a database branch before attempting risky or destructive work (schema migrations, bulk deletes, experiments). Forking is instant and costs nothing until you write. Prefer forking over backing up by hand."

- [ ] **Step 1: Write the failing test**

`internal/mcp/tools_test.go`:

```go
package mcp

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/offshoot-db/offshoot/internal/ops"
)

func newTools(t *testing.T) (*OffshootTools, *ops.Workspace) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	spec := filepath.Join(t.TempDir(), "store")
	w, err := ops.Init(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	return NewOffshootTools(w, spec), w
}

func call(t *testing.T, ts *OffshootTools, name string, args map[string]any) ToolResult {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ts.Call(context.Background(), name, raw)
	if err != nil {
		t.Fatalf("%s returned an RPC error: %v", name, err)
	}
	return res
}

func text(r ToolResult) string {
	var b strings.Builder
	for _, c := range r.Content {
		b.WriteString(c.Text)
	}
	return b.String()
}

func TestToolsAdvertiseSchemas(t *testing.T) {
	ts, _ := newTools(t)
	tools := ts.Tools()
	if len(tools) < 7 {
		t.Fatalf("want the full lifecycle surface, got %d tools", len(tools))
	}
	seen := map[string]bool{}
	for _, tl := range tools {
		if tl.Name == "" || tl.Description == "" || tl.InputSchema == nil {
			t.Errorf("incomplete tool: %+v", tl)
		}
		if seen[tl.Name] {
			t.Errorf("duplicate tool %q", tl.Name)
		}
		seen[tl.Name] = true
	}
	for _, want := range []string{"offshoot_list", "offshoot_checkout", "offshoot_checkpoint",
		"offshoot_fork", "offshoot_rollback", "offshoot_promote", "offshoot_destroy"} {
		if !seen[want] {
			t.Errorf("missing tool %q", want)
		}
	}
}

func TestForkCheckpointRollbackThroughTools(t *testing.T) {
	ts, _ := newTools(t)

	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	if co.IsError {
		t.Fatalf("checkout: %s", text(co))
	}
	path := strings.TrimSpace(lastPath(text(co)))
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES ('original');").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "name": "v1"}); r.IsError {
		t.Fatalf("checkpoint: %s", text(r))
	}
	if r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1"}); r.IsError {
		t.Fatalf("fork: %s", text(r))
	}

	// The agent wrecks its attempt, then rolls back.
	aco := call(t, ts, "offshoot_checkout", map[string]any{
		"database": "app", "branch": "attempt-1"})
	apath := strings.TrimSpace(lastPath(text(aco)))
	if out, err := exec.Command("sqlite3", apath, "DROP TABLE t;").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if r := call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "branch": "attempt-1", "name": "broken"}); r.IsError {
		t.Fatalf("checkpoint attempt: %s", text(r))
	}
	rb := call(t, ts, "offshoot_rollback", map[string]any{
		"database": "app", "branch": "attempt-1", "to": "fork"})
	if rb.IsError {
		t.Fatalf("rollback: %s", text(rb))
	}
	rpath := strings.TrimSpace(lastPath(text(rb)))
	got, err := exec.Command("sqlite3", rpath, "SELECT v FROM t;").Output()
	if err != nil || string(got) != "original\n" {
		t.Fatalf("rollback did not restore: %q err=%v", got, err)
	}
}

func TestProtectedBranchRefusalReachesTheAgent(t *testing.T) {
	ts, _ := newTools(t)
	if r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1"}); r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	// main is protected: promote must refuse, and the message must say how.
	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "main"})
	if !r.IsError {
		t.Fatal("promoting onto protected main without force must be refused")
	}
	if !strings.Contains(strings.ToLower(text(r)), "protected") ||
		!strings.Contains(strings.ToLower(text(r)), "force") {
		t.Fatalf("refusal must tell the agent what to do: %s", text(r))
	}
	// With force it succeeds.
	if r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "main", "force": true}); r.IsError {
		t.Fatalf("forced promote: %s", text(r))
	}
}

func TestUserErrorsAreToolErrorsNotRPCErrors(t *testing.T) {
	ts, _ := newTools(t)
	r := call(t, ts, "offshoot_rollback", map[string]any{
		"database": "app", "to": "no-such-checkpoint"})
	if !r.IsError {
		t.Fatal("rolling back to a missing checkpoint must be a tool error")
	}
	if !strings.Contains(text(r), "no-such-checkpoint") {
		t.Fatalf("message should name the bad checkpoint: %s", text(r))
	}
}

func TestUnknownToolIsAnRPCError(t *testing.T) {
	ts, _ := newTools(t)
	if _, err := ts.Call(context.Background(), "offshoot_nope", json.RawMessage(`{}`)); err == nil {
		t.Fatal("an unknown tool name must be an RPC-level error")
	}
}

// lastPath returns the last whitespace-separated token that looks like a path.
func lastPath(s string) string {
	fields := strings.Fields(s)
	for i := len(fields) - 1; i >= 0; i-- {
		if strings.Contains(fields[i], "/") {
			return fields[i]
		}
	}
	return ""
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/mcp -run 'Tools|Fork|Protected|User|Unknown' -v`
Expected: FAIL — `NewOffshootTools` undefined.

- [ ] **Step 3: Implement**

Write `tools.go` with the seven tools, each: a `Tool` entry whose `InputSchema` is a JSON Schema object with `type`, `properties`, `required`; a handler that unmarshals into a typed struct, applies the `branch` default, calls the corresponding `ops` method, and formats the outcome as text. A handler returns `(ToolResult, error)` where the error is reserved for "this is not a real tool" — every operational failure becomes `ErrorResult` carrying the underlying message verbatim.

Add `tools/call` dispatch to `server.go`: unmarshal `params` into `{name string, arguments json.RawMessage}` (per the verified spec), call `ts.Call`, and marshal the `ToolResult` as the RPC result. A non-nil error from `Call` becomes an RPC error with `CodeInvalidParams` for an unknown tool.

- [ ] **Step 4: Run**

Run: `go test ./internal/mcp -v -race && go test ./... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/mcp
git commit -m "feat: offshoot MCP tool set over the lifecycle operations"
```

---

### Task 3: `offshoot mcp` and end-to-end protocol test

**Files:**
- Modify: `cmd/offshoot/main.go`, `README.md`
- Create: `cmd/offshoot/mcp_test.go`

**Interfaces:**
- Consumes: `mcp.NewServer`, `mcp.NewOffshootTools`
- Produces: `offshoot mcp` — serves MCP on stdin/stdout for the store selected by `-store`/`OFFSHOOT_STORE`

- [ ] **Step 1: Write the failing test**

`cmd/offshoot/mcp_test.go` — drives the real binary as a subprocess, which is how a client will actually use it:

```go
package main

import (
	"bufio"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMCPSubprocessHandshakeAndCall builds nothing: it runs `go run .` so the
// test exercises the same entry point a client would spawn.
func TestMCPSubprocessHandshakeAndCall(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	store := filepath.Join(t.TempDir(), "store")
	if err := run([]string{"-store", store, "init"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-store", store, "create", "app"}); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "run", ".", "-store", store, "mcp")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stdin.Close()
		cmd.Wait()
	}()

	send := func(line string) map[string]any {
		t.Helper()
		if _, err := stdin.Write([]byte(line + "\n")); err != nil {
			t.Fatalf("write: %v (stderr: %s)", err, stderr.String())
		}
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
		if !sc.Scan() {
			t.Fatalf("no response (stderr: %s)", stderr.String())
		}
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad response %q: %v (stderr: %s)", sc.Text(), err, stderr.String())
		}
		return m
	}

	init := send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`)
	if init["error"] != nil {
		t.Fatalf("initialize: %v", init["error"])
	}
	list := send(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if list["error"] != nil {
		t.Fatalf("tools/list: %v", list["error"])
	}
	res := list["result"].(map[string]any)
	if len(res["tools"].([]any)) < 7 {
		t.Fatalf("tools = %v", res["tools"])
	}
	callRes := send(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"offshoot_list","arguments":{}}}`)
	if callRes["error"] != nil {
		t.Fatalf("tools/call: %v", callRes["error"])
	}
	out := callRes["result"].(map[string]any)
	content := out["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("empty tool result: %v", out)
	}
	if !strings.Contains(content[0].(map[string]any)["text"].(string), "app") {
		t.Fatalf("offshoot_list should mention the database: %v", content)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./cmd/offshoot -run TestMCPSubprocess -v`
Expected: FAIL — unknown command "mcp".

- [ ] **Step 3: Implement the command**

Add to `run`'s switch:

```go
	case "mcp":
		if len(rest) != 0 {
			return fmt.Errorf("usage: offshoot mcp")
		}
		ts := mcp.NewOffshootTools(w, spec)
		srv := mcp.NewServer(os.Stdin, os.Stdout, ts)
		return srv.Serve(context.Background())
```

(`spec` is the store-spec variable already in scope; add the `internal/mcp` import and a usage line. The workspace `w` is already opened before the switch.)

- [ ] **Step 4: Document**

Add to `README.md` after the daemon section:

```markdown
## Agent integration (MCP)

`offshoot mcp` speaks the Model Context Protocol on stdio, so an agent can
branch on its own initiative instead of asking you to run commands:

    claude mcp add offshoot -- offshoot -store ./.offshoot mcp

The agent gets seven tools — list, checkout, checkpoint, fork, rollback,
promote, destroy — described so it knows *when* to use them: fork before a
risky migration, checkpoint when tests pass, roll back when they don't,
promote the attempt that worked.

Destructive tools respect the same protected-branch rules as the CLI: an agent
can fork and experiment freely, but promoting onto or destroying `main`
requires an explicit force, and the refusal tells the agent so.
```

- [ ] **Step 5: Run**

Run: `go test ./... -count=1 && go vet ./... && go build ./cmd/offshoot`
Expected: PASS. The subprocess test is slower (it compiles); that is acceptable for one test.

- [ ] **Step 6: Commit**

```bash
git add cmd/offshoot README.md
git commit -m "feat: offshoot mcp command serving the tool set on stdio"
```

---

### Task 4: The launch demo

**Files:**
- Create: `examples/parallel-attempts/run.sh`, `examples/parallel-attempts/README.md`
- Modify: `README.md` (link the demo)
- Test: `examples/parallel-attempts/run_test.go`

**Interfaces:**
- Consumes: the `offshoot` binary
- Produces: a runnable script that demonstrates the pitch — three parallel migration attempts against forks of one database, tests pick the winner, `promote` ships it, the losers are destroyed

The script must be honest: it prints what it is doing, uses only documented commands, and fails loudly if any step fails (`set -euo pipefail`). It builds the binary first so a reader can run it from a fresh clone. Keep it under ~60 lines — this is a demonstration, not a framework.

- [ ] **Step 1: Write the failing test**

`examples/parallel-attempts/run_test.go`:

```go
package main

import (
	"os/exec"
	"strings"
	"testing"
)

// TestDemoRunsEndToEnd runs the published demo exactly as a reader would.
// If this fails, the demo in the README is broken for everyone.
func TestDemoRunsEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH")
	}
	cmd := exec.Command("bash", "run.sh")
	cmd.Env = append(cmd.Environ(), "OFFSHOOT_DEMO_DIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("demo failed: %v\n%s", err, out)
	}
	s := string(out)
	for _, want := range []string{"attempt-1", "attempt-2", "attempt-3", "promoted", "winner"} {
		if !strings.Contains(s, want) {
			t.Errorf("demo output missing %q\n%s", want, s)
		}
	}
	if strings.Contains(strings.ToLower(s), "error") {
		t.Errorf("demo printed an error:\n%s", s)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./examples/parallel-attempts -v`
Expected: FAIL — `run.sh` does not exist.

- [ ] **Step 3: Write the demo**

`examples/parallel-attempts/run.sh`:

```bash
#!/usr/bin/env bash
# Three agents try three migrations on forks of the same database.
# Two of them wreck it. The one that works gets promoted. Nothing else does.
set -euo pipefail

DIR="${OFFSHOOT_DEMO_DIR:-$(mktemp -d)}"
export OFFSHOOT_STORE="$DIR/store"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OFFSHOOT="$DIR/offshoot"

echo "==> building offshoot"
(cd "$ROOT" && go build -o "$OFFSHOOT" ./cmd/offshoot)

echo "==> creating a database with some data"
"$OFFSHOOT" init >/dev/null
"$OFFSHOOT" create shop >/dev/null
DB=$("$OFFSHOOT" checkout shop)
sqlite3 "$DB" "CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT);
               INSERT INTO orders (total) VALUES ('10.00'), ('25.50'), ('3.99');"
"$OFFSHOOT" checkpoint shop before-migration >/dev/null
echo "    3 orders, checkpoint 'before-migration'"

echo "==> forking three attempts (instant, no copy)"
for i in 1 2 3; do "$OFFSHOOT" fork shop "attempt-$i" >/dev/null; done

migrate() { # $1 = attempt, $2 = SQL
  local path; path=$("$OFFSHOOT" checkout "shop@$1")
  if sqlite3 "$path" "$2" 2>/dev/null && \
     [ "$(sqlite3 "$path" 'SELECT count(*) FROM orders WHERE total_cents IS NOT NULL;' 2>/dev/null)" = "3" ]; then
    "$OFFSHOOT" checkpoint "shop@$1" migrated >/dev/null
    echo "    $1: PASS"
    return 0
  fi
  echo "    $1: FAIL"
  return 1
}

echo "==> running the migrations in parallel forks"
set +e
migrate attempt-1 "ALTER TABLE orders ADD COLUMN total_cents INTEGER;
                   UPDATE orders SET total_cents = total * 100;"   # loses precision
A1=$?
migrate attempt-2 "DROP TABLE orders;"                              # catastrophic
A2=$?
migrate attempt-3 "ALTER TABLE orders ADD COLUMN total_cents INTEGER;
                   UPDATE orders SET total_cents = CAST(ROUND(total * 100) AS INTEGER);"
A3=$?
set -e

WINNER=""
for i in 1 2 3; do
  eval "rc=\$A$i"
  if [ "$rc" = "0" ] && [ -z "$WINNER" ]; then WINNER="attempt-$i"; fi
done
[ -n "$WINNER" ] || { echo "no attempt passed"; exit 1; }
echo "==> winner: $WINNER"

echo "==> promoting the winner onto main"
"$OFFSHOOT" promote "shop@$WINNER" --onto main --force >/dev/null
echo "    promoted"

echo "==> discarding the losers"
for i in 1 2 3; do
  [ "attempt-$i" = "$WINNER" ] || "$OFFSHOOT" destroy "shop@attempt-$i" >/dev/null
done
"$OFFSHOOT" gc --grace 0s >/dev/null
"$OFFSHOOT" gc --grace 0s >/dev/null

echo "==> main now has the migrated data:"
MAIN=$("$OFFSHOOT" checkout shop)
sqlite3 -header "$MAIN" "SELECT id, total, total_cents FROM orders;" | sed 's/^/    /'
echo "==> and the original is still one command away:"
echo "    offshoot rollback shop --to before-migration"
```

`chmod +x run.sh`. Write `examples/parallel-attempts/README.md` explaining what the demo shows, how to run it, and what to look at in the output — including that `attempt-2` deletes the table and main is untouched because forks are storage-independent.

- [ ] **Step 4: Run**

Run: `bash examples/parallel-attempts/run.sh` then `go test ./examples/parallel-attempts -v`
Expected: the script prints the flow and the final table with `total_cents`; the test passes. If a step fails, fix the script (or the product bug it exposes) — do not weaken the test's assertions.

- [ ] **Step 5: Link it**

Add to `README.md` under the quickstart: a line pointing at `examples/parallel-attempts/` as a runnable demonstration of the parallel-attempts workflow.

- [ ] **Step 6: Commit**

```bash
git add examples README.md
git commit -m "docs: runnable parallel-attempts demo"
```

---

### Task 5: Adversarial pass — a hostile MCP client

**Files:**
- Modify: `internal/mcp/server_test.go` (append), `internal/mcp/tools_test.go` (append)
- Modify: source only if the tests expose a real gap

**Interfaces:** none new. The tests are the spec: a misbehaving or malicious client must not crash the server, corrupt the protocol stream, or reach past the store.

- [ ] **Step 1: Write the adversarial tests**

Append to `internal/mcp/server_test.go`:

```go
func TestOversizedAndWeirdInputDoesNotBreakTheStream(t *testing.T) {
	huge := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ping_tool","arguments":{"blob":"` +
		strings.Repeat("x", 200_000) + `"}}}`
	got := run(t,
		huge,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if len(got) != 2 {
		t.Fatalf("want 2 responses, got %d", len(got))
	}
	if got[1]["error"] != nil {
		t.Fatalf("server must still serve after a large message: %v", got[1])
	}
}

func TestBatchOfRequestsEachGetsExactlyOneResponse(t *testing.T) {
	var lines []string
	for i := 0; i < 50; i++ {
		lines = append(lines, `{"jsonrpc":"2.0","id":`+strconv.Itoa(i)+`,"method":"tools/list"}`)
	}
	got := run(t, lines...)
	if len(got) != 50 {
		t.Fatalf("want 50 responses, got %d", len(got))
	}
	seen := map[float64]bool{}
	for _, r := range got {
		id := r["id"].(float64)
		if seen[id] {
			t.Fatalf("duplicate response for id %v", id)
		}
		seen[id] = true
	}
}

func TestMissingParamsIsHandledNotPanicked(t *testing.T) {
	got := run(t,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ping_tool"}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`)
	if len(got) != 3 {
		t.Fatalf("want 3 responses, got %d: %v", len(got), got)
	}
	if got[2]["error"] != nil {
		t.Fatalf("server must survive malformed calls: %v", got[2])
	}
}
```

(add `"strconv"` to the imports)

Append to `internal/mcp/tools_test.go`:

```go
func TestToolsRejectPathEscapingNames(t *testing.T) {
	ts, _ := newTools(t)
	for _, bad := range []string{"../etc", "a/b", "..", ".", "UPPER", ""} {
		r := call(t, ts, "offshoot_checkout", map[string]any{"database": bad})
		if !r.IsError {
			t.Errorf("database %q must be refused", bad)
		}
	}
	for _, bad := range []string{"../x", "a/b", ".."} {
		r := call(t, ts, "offshoot_fork", map[string]any{
			"database": "app", "new_branch": bad})
		if !r.IsError {
			t.Errorf("branch %q must be refused", bad)
		}
	}
}

func TestDestroyProtectedMainRequiresForce(t *testing.T) {
	ts, _ := newTools(t)
	r := call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "main"})
	if !r.IsError {
		t.Fatal("an agent must not be able to destroy protected main without force")
	}
	if !strings.Contains(strings.ToLower(text(r)), "protected") {
		t.Fatalf("refusal should say why: %s", text(r))
	}
}

func TestToolResultNeverClaimsUndeliveredDurability(t *testing.T) {
	ts, _ := newTools(t)
	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	path := strings.TrimSpace(lastPath(text(co)))
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	// checkout's result must not tell the agent the write is saved — nothing
	// has been checkpointed yet.
	low := strings.ToLower(text(co))
	if strings.Contains(low, "durable") || strings.Contains(low, "saved") {
		t.Fatalf("checkout must not imply durability: %s", text(co))
	}
}
```

- [ ] **Step 2: Run, investigate, fix**

Run: `go test ./internal/mcp -v -race -count=2`
Expected: PASS, or a real finding. Likely candidates: the scanner's buffer limit truncating a large message into a stream desync (the fix is a bigger buffer or a `json.Decoder` over the stream rather than line scanning); a panic on absent `params`; a tool handler passing an unvalidated name to `ops`. Fix the source and document what you found.

- [ ] **Step 3: Full suite**

Run: `go test ./... -count=1 -race && go vet ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/mcp
git commit -m "test: hostile-client coverage for the MCP server and tools"
```

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage:** implements the spec's § Integration surface item 2 ("MCP server, day one — wraps the lifecycle ops so agents fork before risky work on their own initiative") and § Launch plan's demo ("an agent forks a database, attempts migrations in parallel, tests pick the winner, promote"). The demo here is CLI-driven rather than agent-driven because a scripted, deterministic demo is testable in CI and a live-agent demo is not; the MCP surface makes the agent-driven version possible for the launch post. Deferred and stated in the header: incremental LTX segments and TTL reaping (Plan 7); Python/TS SDKs and the LangGraph adapter (Plan 8). Session tools (open/flush against the daemon) are deliberately excluded — a session outlives a single tool call, so it needs the SDK's lifecycle handling.
2. **Placeholder scan:** none. Task 1 carries a mandatory protocol-verification step rather than assuming the plan's message shapes are current, and Task 5 names the specific failure modes to expect and prescribes fixing source over expectations.
3. **Type consistency:** `Tool{Name,Description,InputSchema}`, `ToolResult{Content,IsError}`, `Content{Type,Text}`, `TextResult`/`ErrorResult`, and the `ToolSet` interface are used identically in Tasks 1, 2, 3, 5; `NewServer(r, w, ts)` and `NewOffshootTools(ws, spec)` match their call sites in `cmd/offshoot/main.go`; the seven tool names in Task 2's table match the assertions in Tasks 2, 3, and 5; `run(t, lines...)` and `call(t, ts, name, args)` are the shared test helpers across the mcp test files.
