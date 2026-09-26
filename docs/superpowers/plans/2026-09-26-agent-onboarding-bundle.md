# Agent Onboarding Bundle and MCP Modernization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make offshoot's MCP server self-describing to hosts (annotations, structured results, `meta`, a `touch` tool) and ship a Claude Code plugin + Cursor install path + rules-file snippet so an agent knows when to fork, checkpoint, and promote, plus fix three provable doc drifts.

**Architecture:** The MCP server (`internal/mcp`) gains spec-standard tool `annotations`, a `structuredContent` field on results, `meta` arguments on `offshoot_fork`/`offshoot_checkpoint`, and an eighth tool `offshoot_touch`, all threaded to the existing `ops`/daemon paths (no storage changes). A Claude Code plugin lives in `plugin/` (manifest, `.mcp.json`, one skill, two advisory hooks) and is listed by a repo-root `.claude-plugin/marketplace.json`. Docs, README, and the walkthrough's real `tools/list` capture are updated to match.

**Tech Stack:** Go 1.26 (`internal/mcp`, `cmd/offshoot`), bash + python3 hook scripts, JSON manifests, Markdown docs. Tests: `go test ./internal/mcp`, `make check-plugin` (new).

**Spec:** `reports/offshoot next features roadmap.md`, Tier 1 item "Weeks 1-2: an agent-native onboarding bundle and MCP modernization (plus the three doc-drift fixes)". Verified references: MCP spec 2026-07-28 tools page (`annotations`, `structuredContent`), Claude Code plugin manifest reference (`.claude-plugin/plugin.json`, `.mcp.json`, `skills/<name>/SKILL.md`, `hooks/hooks.json`, `${CLAUDE_PLUGIN_ROOT}`), Cursor deeplink `cursor://anysphere.cursor-deeplink/mcp/install?name=$NAME&config=$BASE64`.

## Global Constraints

- Go directive is `1.26.0`; gofmt clean; `go vet ./...` clean.
- Every commit ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B`.
- Package publication is deferred indefinitely: no `pip install offshoot-db` / `npm install @offshoot-db/...` text anywhere; install paths are `go install`, Homebrew tap, prebuilt binaries, GHCR image, git-URL pip/npm.
- MCP tool names stay `offshoot_*`; the existing seven tools' argument names and text results must not change (the walkthrough and `TestForkDescriptionStatesItsRealCost` pin them). Adding fields is fine.
- `structuredContent` is additive; existing prose text blocks stay as the only `content` entry (deliberate, documented deviation from the spec's "SHOULD also serialize JSON into text": the prose is what the model reads, and duplicating JSON doubles context).
- Hooks are advisory only: never `permissionDecision: deny`, never exit 2. They must be silent (exit 0, no output) when `offshoot` is not on PATH or no store exists.
- All doc claims must be true of the code as committed; the walkthrough's JSON must be re-captured from a real `offshoot mcp` run, never hand-edited.

---

### Task 1: Tool annotations on all seven tools

**Files:**
- Modify: `internal/mcp/protocol.go:82-86` (Tool struct)
- Modify: `internal/mcp/tools.go:250-353` (Tools())
- Test: `internal/mcp/tools_test.go`

**Interfaces:**
- Produces: `type ToolAnnotations struct { Title string; ReadOnlyHint, DestructiveHint, IdempotentHint, OpenWorldHint *bool }` with JSON tags `title,omitempty`, `readOnlyHint,omitempty`, `destructiveHint,omitempty`, `idempotentHint,omitempty`, `openWorldHint,omitempty`; `Tool.Annotations *ToolAnnotations` (`json:"annotations,omitempty"`); helper `func annotate(title string, readOnly, destructive, idempotent bool) *ToolAnnotations` that always sets all four hints explicitly (openWorldHint always false: offshoot is a closed system) so hosts never fall back to spec defaults (destructiveHint defaults to true).

- [ ] **Step 1: Write the failing test**

Append to `internal/mcp/tools_test.go`:

```go
// TestToolAnnotationsClassifyEveryTool pins the host-facing behavior hints
// the 2026-07-28 spec lets a tool carry: hosts use readOnlyHint to skip
// confirmation and destructiveHint to require it, and the spec's default
// for destructiveHint is TRUE, so every tool must state all hints
// explicitly — an unannotated fork would be treated as destructive.
func TestToolAnnotationsClassifyEveryTool(t *testing.T) {
	ts, _ := newTools(t)
	want := map[string]struct{ readOnly, destructive, idempotent bool }{
		"offshoot_list":       {true, false, true},
		"offshoot_checkout":   {false, false, true},
		"offshoot_checkpoint": {false, false, false},
		"offshoot_fork":       {false, false, false},
		"offshoot_rollback":   {false, true, false},
		"offshoot_promote":    {false, true, false},
		"offshoot_destroy":    {false, true, false},
	}
	for _, tl := range ts.Tools() {
		w, ok := want[tl.Name]
		if !ok {
			continue // tools added by later tasks pin their own annotations
		}
		a := tl.Annotations
		if a == nil {
			t.Fatalf("%s: no annotations", tl.Name)
		}
		if a.Title == "" {
			t.Errorf("%s: annotations.title must be set", tl.Name)
		}
		for name, got := range map[string]*bool{
			"readOnlyHint": a.ReadOnlyHint, "destructiveHint": a.DestructiveHint,
			"idempotentHint": a.IdempotentHint, "openWorldHint": a.OpenWorldHint,
		} {
			if got == nil {
				t.Errorf("%s: %s must be set explicitly (spec defaults are not ours)", tl.Name, name)
			}
		}
		if a.ReadOnlyHint == nil || a.DestructiveHint == nil || a.IdempotentHint == nil || a.OpenWorldHint == nil {
			continue
		}
		if *a.ReadOnlyHint != w.readOnly || *a.DestructiveHint != w.destructive || *a.IdempotentHint != w.idempotent {
			t.Errorf("%s: hints readOnly=%v destructive=%v idempotent=%v, want %v/%v/%v",
				tl.Name, *a.ReadOnlyHint, *a.DestructiveHint, *a.IdempotentHint, w.readOnly, w.destructive, w.idempotent)
		}
		if *a.OpenWorldHint {
			t.Errorf("%s: openWorldHint must be false (offshoot touches only its own store)", tl.Name)
		}
	}
}

// TestToolAnnotationsSerializeOnTheWire: the hints must reach tools/list
// JSON under the spec's field names, with the omitted-when-nil shape.
func TestToolAnnotationsSerializeOnTheWire(t *testing.T) {
	ts, _ := newTools(t)
	raw, err := json.Marshal(ts.Tools())
	if err != nil {
		t.Fatal(err)
	}
	var back []map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	for _, tl := range back {
		ann, ok := tl["annotations"].(map[string]any)
		if !ok {
			t.Fatalf("%v: annotations missing on the wire", tl["name"])
		}
		for _, k := range []string{"title", "readOnlyHint", "destructiveHint", "idempotentHint", "openWorldHint"} {
			if _, ok := ann[k]; !ok {
				t.Errorf("%v: annotations.%s missing on the wire", tl["name"], k)
			}
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcp -run 'TestToolAnnotations' -count=1`
Expected: FAIL to compile with `tl.Annotations undefined`.

- [ ] **Step 3: Add the types and the helper**

In `internal/mcp/protocol.go`, replace the `Tool` struct with:

```go
// Tool describes one callable tool, as returned from tools/list.
type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema any    `json:"inputSchema"`
	// Annotations are the spec's behavior hints
	// (https://modelcontextprotocol.io/specification/2026-07-28/server/tools#tool):
	// hosts use them to decide which calls need a confirmation prompt. The
	// spec's default for destructiveHint is true, so every offshoot tool
	// sets all four hints explicitly — see tools.go's annotate.
	Annotations *ToolAnnotations `json:"annotations,omitempty"`
}

// ToolAnnotations mirrors the spec's ToolAnnotations object. Pointers, so a
// nil hint is omitted rather than serialized as a misleading false.
type ToolAnnotations struct {
	Title           string `json:"title,omitempty"`
	ReadOnlyHint    *bool  `json:"readOnlyHint,omitempty"`
	DestructiveHint *bool  `json:"destructiveHint,omitempty"`
	IdempotentHint  *bool  `json:"idempotentHint,omitempty"`
	OpenWorldHint   *bool  `json:"openWorldHint,omitempty"`
}
```

In `internal/mcp/tools.go`, above `func (t *OffshootTools) Tools()`, add:

```go
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
```

Then add an `Annotations:` field to each tool literal in `Tools()`:

| Tool | call |
|---|---|
| `offshoot_list` | `annotate("List databases and branches", true, false, true)` |
| `offshoot_checkout` | `annotate("Materialize a branch to a SQLite file", false, false, true)` |
| `offshoot_checkpoint` | `annotate("Checkpoint a branch", false, false, false)` |
| `offshoot_fork` | `annotate("Fork a branch", false, false, false)` |
| `offshoot_rollback` | `annotate("Roll a branch back to a checkpoint", false, true, false)` |
| `offshoot_promote` | `annotate("Promote a branch onto a target", false, true, false)` |
| `offshoot_destroy` | `annotate("Destroy a branch", false, true, false)` |

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcp -count=1`
Expected: PASS (all existing tests, plus the two new ones).

- [ ] **Step 5: Commit**

```bash
git add internal/mcp/protocol.go internal/mcp/tools.go internal/mcp/tools_test.go
git commit -m "mcp: annotate every tool with explicit behavior hints

The 2026-07-28 spec defaults destructiveHint to true, so an unannotated
fork reads as destructive to a host that honors the hints. Every tool now
states title/readOnly/destructive/idempotent/openWorld explicitly: list is
read-only, checkout/fork/checkpoint are non-destructive, rollback/promote/
destroy are destructive, and openWorldHint is always false.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 2: `structuredContent` on every successful tool result

**Files:**
- Modify: `internal/mcp/protocol.go:95-110` (ToolResult, TextResult)
- Modify: `internal/mcp/tools.go` (each success return in list/checkout/checkpoint/fork/rollback/promote/destroy)
- Test: `internal/mcp/tools_test.go`

**Interfaces:**
- Produces: `ToolResult.StructuredContent any` (`json:"structuredContent,omitempty"`); helper `func StructuredResult(data any, format string, args ...any) ToolResult` returning the prose text block plus `StructuredContent: data`. Data shapes (all `map[string]any`, keys snake_case):
  - list: `{"branches": [{"database","branch","head_txid","checkpoints":[...],"protected","checked_out"}]}`
  - checkout: `{"database","branch","path","live": bool}`
  - checkpoint: `{"database","branch","name","txid","live": bool}`
  - fork: `{"database","branch","new_branch","txid","ttl","expires_at"}` (`ttl` "" and `expires_at` "" when none)
  - rollback: `{"database","branch","to","path"}`
  - promote: `{"database","source","target","txid","backup"}`
  - destroy: `{"database","branch"}`

- [ ] **Step 1: Write the failing test**

Append to `internal/mcp/tools_test.go`:

```go
// TestStructuredContentAccompaniesProse: every successful tool result also
// carries a machine-readable structuredContent (2026-07-28 spec), so a
// harness can read txids/paths/backup names without parsing prose. The
// prose stays the only content block on purpose — see the plan's Global
// Constraints — so this checks structuredContent, not a JSON text block.
func TestStructuredContentAccompaniesProse(t *testing.T) {
	ts, w := newTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	sc := func(r ToolResult) map[string]any {
		t.Helper()
		if r.IsError {
			t.Fatalf("unexpected error: %s", text(r))
		}
		m, ok := r.StructuredContent.(map[string]any)
		if !ok {
			t.Fatalf("structuredContent is %T, want map[string]any", r.StructuredContent)
		}
		return m
	}
	fork := sc(call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "attempt-2"}))
	if fork["new_branch"] != "attempt-2" || fork["txid"] == nil || fork["ttl"] == nil {
		t.Fatalf("fork structuredContent = %v", fork)
	}
	cp := sc(call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "branch": "attempt-1", "name": "v1"}))
	if cp["name"] != "v1" || cp["txid"] == nil || cp["live"] != false {
		t.Fatalf("checkpoint structuredContent = %v", cp)
	}
	co := sc(call(t, ts, "offshoot_checkout", map[string]any{"database": "app", "branch": "attempt-1"}))
	if co["path"] == "" || co["path"] == nil {
		t.Fatalf("checkout structuredContent = %v", co)
	}
	rb := sc(call(t, ts, "offshoot_rollback", map[string]any{"database": "app", "branch": "attempt-1", "to": "v1"}))
	if rb["to"] != "v1" || rb["path"] == nil {
		t.Fatalf("rollback structuredContent = %v", rb)
	}
	pr := sc(call(t, ts, "offshoot_promote", map[string]any{"database": "app", "source": "attempt-1", "target": "main", "force": true}))
	if pr["backup"] != "main"+ops.PromoteBackupSuffix || pr["txid"] == nil {
		t.Fatalf("promote structuredContent = %v", pr)
	}
	ls := sc(call(t, ts, "offshoot_list", map[string]any{}))
	branches, _ := ls["branches"].([]any)
	if len(branches) < 3 {
		t.Fatalf("list structuredContent branches = %v", ls["branches"])
	}
	ds := sc(call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "attempt-2"}))
	if ds["branch"] != "attempt-2" {
		t.Fatalf("destroy structuredContent = %v", ds)
	}
	// Errors carry no structuredContent.
	bad := call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "nope"})
	if !bad.IsError || bad.StructuredContent != nil {
		t.Fatalf("error results must not carry structuredContent: %+v", bad)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcp -run TestStructuredContentAccompaniesProse -count=1`
Expected: FAIL to compile with `r.StructuredContent undefined`.

- [ ] **Step 3: Add the field and helper**

In `internal/mcp/protocol.go`, replace `ToolResult` with:

```go
// ToolResult is an MCP tool response: content blocks plus an error flag,
// and — on success — a machine-readable mirror of the prose in
// StructuredContent (2026-07-28 spec). The prose block stays the single
// content entry: it is what the model reads, and the spec's "SHOULD also
// serialize the JSON into a text block" would double the context cost for
// no reader; harnesses read StructuredContent directly.
type ToolResult struct {
	Content           []Content `json:"content"`
	IsError           bool      `json:"isError,omitempty"`
	StructuredContent any       `json:"structuredContent,omitempty"`
}
```

Below `TextResult`, add:

```go
// StructuredResult is TextResult plus a StructuredContent payload. data
// should be a map[string]any with snake_case keys (the shapes are pinned by
// TestStructuredContentAccompaniesProse).
func StructuredResult(data any, format string, args ...any) ToolResult {
	r := TextResult(format, args...)
	r.StructuredContent = data
	return r
}
```

- [ ] **Step 4: Switch each success return to StructuredResult**

In `internal/mcp/tools.go`, for each tool's success path, replace `TextResult(...)` with `StructuredResult(data, ...)` using the shapes from the Interfaces block. Concretely:

`list` (in the loop, also collect):
```go
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
```

`checkout`: both the live-session and at-rest returns become `StructuredResult(map[string]any{"database": a.Database, "branch": branch, "path": path, "live": <true|false>}, <existing format and args>)`.

`checkpoint`: live path `StructuredResult(map[string]any{"database": a.Database, "branch": branch, "name": a.Name, "txid": resp.TXID, "live": true}, ...)`; at-rest path same with `txid` and `"live": false`.

`fork`: the existing result already echoes ttl/expiry text; build `map[string]any{"database": a.Database, "branch": branch, "new_branch": a.NewBranch, "txid": txid, "ttl": <ttl string or "">, "expires_at": <RFC3339 expiry or "">}` from the same variables the prose uses (read the fork handler around `internal/mcp/tools.go:660-760` for the variable names; use exactly what the prose prints).

`rollback`: `map[string]any{"database": a.Database, "branch": branch, "to": a.To, "path": path}`.

`promote`: `map[string]any{"database": a.Database, "source": a.Source, "target": a.Target, "txid": res.TXID, "backup": res.Backup}` on both return branches.

`destroy`: `map[string]any{"database": a.Database, "branch": a.Branch}`.

Error paths keep `ErrorResult` (no structuredContent).

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/mcp -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/mcp/protocol.go internal/mcp/tools.go internal/mcp/tools_test.go
git commit -m "mcp: mirror every successful result in structuredContent

Harnesses get txids, paths, TTLs, and the promote backup name as JSON
(2026-07-28 spec) without parsing prose. The prose stays the single
content block on purpose: it is what the model reads.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 3: `meta` argument on `offshoot_fork` and `offshoot_checkpoint`

**Files:**
- Modify: `internal/mcp/tools.go` (schema helper, `checkpointArgs`, `forkArgs`, both handlers, both descriptions)
- Modify: `internal/mcp/tools_test.go:257-270` (`jsonSchemaTypeForKind`)
- Test: `internal/mcp/tools_test.go`

**Interfaces:**
- Consumes: `ops.Workspace.Checkpoint(db, branch, name string, meta map[string]string)`, `ops.Workspace.Fork(db, src, new, at string, ttl time.Duration, meta map[string]string)`, `daemon.Request.Meta map[string]string` (honored by daemon ops `fork` and `flush`-with-Name).
- Produces: schema helper `func optMeta(name string) prop` producing `{"type":"object","additionalProperties":{"type":"string"}}`; `checkpointArgs.Meta` and `forkArgs.Meta` (`json:"meta"`, type `map[string]string`).

- [ ] **Step 1: Write the failing test**

Append to `internal/mcp/tools_test.go`:

```go
// TestForkAndCheckpointCarryMeta closes status.md's "MCP tool metadata
// exposure" deferral: a caller-supplied meta map lands on the new branch's
// ref (fork) and on the named checkpoint (checkpoint), both at rest, with
// ops.ValidateMeta's caps enforced as a tool error, not a crash.
func TestForkAndCheckpointCarryMeta(t *testing.T) {
	ts, w := newTools(t)
	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1",
		"meta": map[string]any{"run_id": "eval-42", "agent": "claude"}})
	if r.IsError {
		t.Fatalf("fork with meta: %s", text(r))
	}
	ref, _, err := w.Store.GetRef("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Meta["run_id"] != "eval-42" || ref.Meta["agent"] != "claude" {
		t.Fatalf("fork meta not stored: %v", ref.Meta)
	}
	if _, err := w.Checkout("app", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	r = call(t, ts, "offshoot_checkpoint", map[string]any{
		"database": "app", "branch": "attempt-1", "name": "v1",
		"meta": map[string]any{"git_sha": "abc123"}})
	if r.IsError {
		t.Fatalf("checkpoint with meta: %s", text(r))
	}
	ref, _, err = w.Store.GetRef("app", "attempt-1")
	if err != nil {
		t.Fatal(err)
	}
	var cp store.Checkpoint
	if err := json.Unmarshal(ref.Checkpoints["v1"], &cp); err != nil {
		t.Fatal(err)
	}
	if cp.Meta["git_sha"] != "abc123" {
		t.Fatalf("checkpoint meta not stored: %v", cp.Meta)
	}
	// Over the cap is a tool error the model can act on.
	big := map[string]any{}
	for i := 0; i < ops.MaxMetaKeys+1; i++ {
		big[fmt.Sprintf("k%d", i)] = "v"
	}
	r = call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "attempt-2", "meta": big})
	if !r.IsError || !strings.Contains(text(r), "exceeds") {
		t.Fatalf("oversized meta must be a tool error, got %+v", r)
	}
	// Schema advertises meta as an object of strings.
	for _, tl := range ts.Tools() {
		if tl.Name != "offshoot_fork" && tl.Name != "offshoot_checkpoint" {
			continue
		}
		props := tl.InputSchema.(map[string]any)["properties"].(map[string]any)
		meta, ok := props["meta"].(map[string]any)
		if !ok || meta["type"] != "object" {
			t.Fatalf("%s: meta must be advertised as an object, got %v", tl.Name, props["meta"])
		}
	}
}
```

Check imports: `store` is `github.com/sricola/offshoot/internal/store` (add if the test file lacks it), `fmt`, `strings`, `json` are already imported.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcp -run TestForkAndCheckpointCarryMeta -count=1`
Expected: FAIL: fork meta not stored (meta silently ignored today) or schema assertion fails.

- [ ] **Step 3: Implement**

In `internal/mcp/tools.go`, after `optBool`, add:

```go
// optMeta builds the optional `meta` property: a small string->string map
// stored on the new branch (fork) or the named checkpoint (checkpoint),
// capped by ops.ValidateMeta. Advertised as an object of strings so a
// model does not send a JSON-encoded string.
func optMeta(name string) prop {
	return prop{name: name, jsonType: "object", extra: map[string]any{"additionalProperties": map[string]any{"type": "string"}}}
}
```

Extend `prop` with `extra map[string]any` and, in `schema()`, after setting `def["type"]`, merge: `for k, v := range p.extra { def[k] = v }`.

Add `Meta map[string]string \`json:"meta"\`` to both `checkpointArgs` and `forkArgs`. Add `optMeta("meta")` to both tools' `schema(...)` calls, and append to both descriptions: `" Optional `meta` (string->string, at most 32 keys) tags the result with your run id, git SHA, or agent name for later lookup."`

In the checkpoint handler: pass `a.Meta` where `nil` is passed today (`t.ws.Checkpoint(a.Database, branch, a.Name, a.Meta)`), and set `Meta: a.Meta` on the daemon `flush` request. Delete the stale "meta is nil ... deliberately out of scope" comment.

In the fork handler: pass `a.Meta` to `t.ws.Fork(...)` and set `Meta: a.Meta` on the daemon `fork` request.

In `internal/mcp/tools_test.go`'s `jsonSchemaTypeForKind`, add before `default`:

```go
	case reflect.Map:
		return "object"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcp -count=1`
Expected: PASS, including `TestToolSchemaTypesMatchArgStructs`.

- [ ] **Step 5: Commit**

```bash
git add internal/mcp/tools.go internal/mcp/tools_test.go
git commit -m "mcp: accept meta on fork and checkpoint

Closes status.md's 'MCP tool metadata exposure' deferral: a caller's
string->string map lands on the forked branch's ref or the named
checkpoint, at rest and through the daemon, capped by ops.ValidateMeta
and refused as a tool error over the cap.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 4: `offshoot_touch` tool

**Files:**
- Modify: `internal/mcp/tools.go` (Tools(), Call switch, new handler)
- Modify: `internal/mcp/tools_test.go` (`toolArgStructs`, `TestToolAnnotationsClassifyEveryTool` want map)
- Test: `internal/mcp/tools_test.go`

**Interfaces:**
- Consumes: `ops.Workspace.Touch(db, branch string, ttl *time.Duration, now time.Time) (store.Ref, error)`; `resolveForkTTL`-style parsing is NOT reused: touch semantics are `""` keep, `"none"` clear, else Go duration.
- Produces: tool `offshoot_touch` with args `type touchArgs struct { Database string \`json:"database"\`; Branch string \`json:"branch"\`; TTL string \`json:"ttl"\` }`, annotated `annotate("Extend a branch's life", false, false, true)`, structured result `{"database","branch","ttl","touched_at"}`.

- [ ] **Step 1: Write the failing test**

Append to `internal/mcp/tools_test.go`:

```go
// TestTouchExtendsALeasedAttempt: an agent mid-task on a TTL'd fork can
// reset its activity clock (and optionally change or clear the TTL) so the
// janitor does not reap the branch under it. "" keeps the TTL, "none"
// clears it, a duration sets it — the daemon touch op's exact contract.
func TestTouchExtendsALeasedAttempt(t *testing.T) {
	ts, w := newTools(t)
	if _, err := w.Fork("app", "main", "attempt-1", "", 2*time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	before, _, _ := w.Store.GetRef("app", "attempt-1")
	time.Sleep(5 * time.Millisecond)
	r := call(t, ts, "offshoot_touch", map[string]any{"database": "app", "branch": "attempt-1"})
	if r.IsError {
		t.Fatalf("touch: %s", text(r))
	}
	after, _, _ := w.Store.GetRef("app", "attempt-1")
	if after.TTL != "2h0m0s" || !(after.TouchedAt > before.TouchedAt) {
		t.Fatalf("touch must keep the TTL and advance touched_at: before=%+v after=%+v", before, after)
	}
	sc := r.StructuredContent.(map[string]any)
	if sc["ttl"] != "2h0m0s" || sc["branch"] != "attempt-1" {
		t.Fatalf("touch structuredContent = %v", sc)
	}
	if r := call(t, ts, "offshoot_touch", map[string]any{"database": "app", "branch": "attempt-1", "ttl": "30m"}); r.IsError {
		t.Fatalf("touch with ttl: %s", text(r))
	}
	after, _, _ = w.Store.GetRef("app", "attempt-1")
	if after.TTL != "30m0s" {
		t.Fatalf("ttl not applied: %q", after.TTL)
	}
	if r := call(t, ts, "offshoot_touch", map[string]any{"database": "app", "branch": "attempt-1", "ttl": "none"}); r.IsError {
		t.Fatalf("touch ttl none: %s", text(r))
	}
	after, _, _ = w.Store.GetRef("app", "attempt-1")
	if after.TTL != "" {
		t.Fatalf("ttl not cleared: %q", after.TTL)
	}
	if r := call(t, ts, "offshoot_touch", map[string]any{"database": "app", "branch": "attempt-1", "ttl": "soon"}); !r.IsError {
		t.Fatal("garbage ttl must be a tool error")
	}
	if r := call(t, ts, "offshoot_touch", map[string]any{"database": "app", "branch": "nope"}); !r.IsError {
		t.Fatal("unknown branch must be a tool error")
	}
}
```

Also: add `"offshoot_touch": touchArgs{},` to `toolArgStructs`, and `"offshoot_touch": {false, false, true},` to `TestToolAnnotationsClassifyEveryTool`'s want map.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcp -run 'TestTouch|TestToolSchemaTypes' -count=1`
Expected: FAIL to compile (`touchArgs` undefined).

- [ ] **Step 3: Implement**

In `internal/mcp/tools.go`, add to `Tools()` after `offshoot_destroy`:

```go
		{
			Name: "offshoot_touch",
			Description: "Reset a branch's activity clock so its TTL does not expire mid-task, and " +
				"optionally change the TTL. Call this when an attempt on a TTL'd fork is taking " +
				"longer than expected, or before handing a fork to a long-running step. `ttl` " +
				"omitted keeps the current TTL; a Go duration like \"2h\" sets it; \"none\" clears " +
				"it so the branch never expires (prefer a longer duration over \"none\" — " +
				"branches without a TTL are only removed by an explicit destroy). A TTL alone " +
				"reaps nothing: the janitor (`offshoot serve`) or `offshoot gc` does.",
			InputSchema: schema(reqStr("database"), optStrDefault("branch", "main"), optStr("ttl")),
			Annotations: annotate("Extend a branch's life", false, false, true),
		},
```

Add `case "offshoot_touch": return t.touch(args)` to the `Call` switch, and the handler:

```go
type touchArgs struct {
	Database string `json:"database"`
	Branch   string `json:"branch"`
	// TTL: "" keeps the current TTL, "none" clears it, a Go duration sets it.
	TTL string `json:"ttl"`
}

// touch resets db@branch's activity clock (ops.Touch: CAS-retried, refuses
// a branch a reaper has already claimed) and optionally sets/clears its TTL.
// Safe alongside an open daemon session: the ref CAS races only lease
// renewals, and ops.Touch retries.
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
```

Update the `Tools()` doc comment from "seven lifecycle tools" to "eight lifecycle tools". Fix `TestToolsAdvertiseSchemas`'s `< 7` to `< 8`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcp -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/mcp/tools.go internal/mcp/tools_test.go
git commit -m "mcp: add offshoot_touch so an agent can keep a TTL'd fork alive

Eighth tool: reset the activity clock, optionally set or clear the TTL
(\"\" keep, duration set, \"none\" clear — the daemon touch op's contract).
Non-destructive, idempotent; the description says a TTL alone reaps
nothing.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 5: Claude Code plugin in `plugin/`, marketplace at repo root, `make check-plugin`

**Files:**
- Create: `.claude-plugin/marketplace.json`
- Create: `plugin/.claude-plugin/plugin.json`
- Create: `plugin/.mcp.json`
- Create: `plugin/skills/offshoot/SKILL.md`
- Create: `plugin/hooks/hooks.json`
- Create: `plugin/hooks/session-start.sh` (executable)
- Create: `plugin/hooks/pre-bash-sql-guard.sh` (executable)
- Create: `scripts/check-plugin.sh` (executable)
- Modify: `Makefile:1-3` (.PHONY list), add target `check-plugin`
- Modify: `.github/workflows/ci.yml` (metrics-lint job: append a `make check-plugin` step)

**Interfaces:**
- Produces: marketplace named `offshoot`, plugin named `offshoot`; install is `claude plugin marketplace add sricola/offshoot` then `claude plugin install offshoot@offshoot`. Hook scripts read JSON on stdin, print either nothing or a `hookSpecificOutput` JSON, always exit 0.

- [ ] **Step 1: Write the failing check**

Create `scripts/check-plugin.sh`:

```bash
#!/usr/bin/env bash
# Validates the Claude Code plugin bundle: every JSON manifest parses and
# has its required fields, the skill has frontmatter, hook scripts are
# executable and exit 0 with no output when offshoot is absent (advisory
# hooks must never break a session). Run by `make check-plugin` and CI.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
fail() { echo "check-plugin: $*" >&2; exit 1; }

python3 - <<'EOF' || fail "manifest validation failed"
import json, sys
mk = json.load(open(".claude-plugin/marketplace.json"))
assert mk["name"] == "offshoot", "marketplace name"
assert mk["plugins"][0]["name"] == "offshoot" and mk["plugins"][0]["source"] == "./plugin", "marketplace plugin entry"
pl = json.load(open("plugin/.claude-plugin/plugin.json"))
for k in ("name", "version", "description", "author", "homepage", "repository", "license"):
    assert k in pl, f"plugin.json missing {k}"
assert pl["name"] == "offshoot"
mcp = json.load(open("plugin/.mcp.json"))
assert mcp["offshoot"]["command"] == "offshoot" and mcp["offshoot"]["args"] == ["mcp"], ".mcp.json server"
hooks = json.load(open("plugin/hooks/hooks.json"))
events = set(hooks)
assert events == {"SessionStart", "PreToolUse"}, f"hook events {events}"
for h in hooks["PreToolUse"]:
    assert h["matcher"] == "Bash", "PreToolUse must match Bash only"
EOF

head -1 plugin/skills/offshoot/SKILL.md | grep -qx -- '---' || fail "SKILL.md must start with frontmatter"
grep -q '^description:' plugin/skills/offshoot/SKILL.md || fail "SKILL.md frontmatter needs description"

for s in plugin/hooks/session-start.sh plugin/hooks/pre-bash-sql-guard.sh; do
  [ -x "$s" ] || fail "$s must be executable"
  bash -n "$s" || fail "$s has a syntax error"
  # With offshoot absent from PATH and no store, hooks must be silent and succeed.
  out=$(echo '{"hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"sqlite3 app.db \"DROP TABLE t\""},"cwd":"/nonexistent"}' \
    | env -i PATH=/usr/bin:/bin HOME=/tmp bash "$s") || fail "$s exited non-zero without offshoot"
  [ -z "$out" ] || fail "$s must print nothing when offshoot is not installed, got: $out"
done
echo "check-plugin: ok"
```

`chmod +x scripts/check-plugin.sh`. Add to `Makefile`: `check-plugin` in the `.PHONY` list on line 1-3 and a target:

```make
check-plugin:
	./scripts/check-plugin.sh
```

- [ ] **Step 2: Run the check to verify it fails**

Run: `make check-plugin`
Expected: FAIL (`No such file` for the marketplace manifest).

- [ ] **Step 3: Create the manifests**

`.claude-plugin/marketplace.json`:

```json
{
  "name": "offshoot",
  "owner": { "name": "Srivatsa Ray", "url": "https://github.com/sricola" },
  "metadata": {
    "description": "Branch SQLite like git: fork-per-attempt databases for AI agents, as Claude Code tools plus a skill that says when to use them."
  },
  "plugins": [
    {
      "name": "offshoot",
      "source": "./plugin",
      "description": "offshoot MCP tools (fork, checkpoint, rollback, promote, destroy, touch, list, checkout) with a skill for the fork-before-risky-work loop and advisory hooks.",
      "version": "0.1.0",
      "category": "database",
      "keywords": ["sqlite", "database", "branching", "mcp", "agents", "eval"]
    }
  ]
}
```

`plugin/.claude-plugin/plugin.json`:

```json
{
  "name": "offshoot",
  "version": "0.1.0",
  "description": "Give each attempt its own real SQLite database: fork before risky work, checkpoint when tests pass, roll back when they fail, promote the winner. Requires the offshoot binary on PATH.",
  "author": { "name": "Srivatsa Ray", "url": "https://github.com/sricola" },
  "homepage": "https://sricola.github.io/offshoot/",
  "repository": "https://github.com/sricola/offshoot",
  "license": "Apache-2.0",
  "keywords": ["sqlite", "database", "branching", "fork", "checkpoint", "rollback", "mcp"]
}
```

`plugin/.mcp.json` (the store resolves from `OFFSHOOT_STORE` or `./.offshoot` in the session's cwd, exactly as the CLI does):

```json
{
  "offshoot": {
    "command": "offshoot",
    "args": ["mcp"]
  }
}
```

`plugin/hooks/hooks.json`:

```json
{
  "SessionStart": [
    {
      "hooks": [
        { "type": "command", "command": "${CLAUDE_PLUGIN_ROOT}/hooks/session-start.sh", "timeout": 10 }
      ]
    }
  ],
  "PreToolUse": [
    {
      "matcher": "Bash",
      "hooks": [
        { "type": "command", "command": "${CLAUDE_PLUGIN_ROOT}/hooks/pre-bash-sql-guard.sh", "timeout": 5 }
      ]
    }
  ]
}
```

- [ ] **Step 4: Create the hook scripts**

`plugin/hooks/session-start.sh`:

```bash
#!/usr/bin/env bash
# SessionStart: if offshoot is installed and a store exists, tell Claude what
# databases and branches are there so it forks before risky work instead of
# discovering offshoot mid-task. Silent (exit 0, no output) otherwise —
# an advisory hook must never break a session.
set -u
command -v offshoot >/dev/null 2>&1 || exit 0
input=$(cat 2>/dev/null || true)
cwd=$(printf '%s' "$input" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("cwd",""))' 2>/dev/null || true)
store="${OFFSHOOT_STORE:-${cwd:-.}/.offshoot}"
case "$store" in
  s3://*|file://*) ;;
  *) [ -d "$store" ] || exit 0 ;;
esac
status=$(OFFSHOOT_STORE="$store" offshoot status 2>/dev/null | head -40) || exit 0
[ -n "$status" ] || exit 0
python3 - "$store" "$status" <<'EOF'
import json, sys
store, status = sys.argv[1], sys.argv[2]
ctx = (
    "offshoot (SQLite branching) is available as MCP tools for the store at "
    f"{store}. Current branches:\n{status}\n"
    "Before a schema migration, bulk delete, or any experiment on a database: "
    "offshoot_fork first and work in the fork; offshoot_checkpoint when tests pass; "
    "offshoot_rollback when they fail; offshoot_promote the fork that worked. "
    "/rewind restores Claude's file edits only, not changes a Bash command made to "
    "a database — use offshoot_rollback for those."
)
print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": ctx}}))
EOF
exit 0
```

`plugin/hooks/pre-bash-sql-guard.sh`:

```bash
#!/usr/bin/env bash
# PreToolUse(Bash): when a command looks like destructive SQL against a
# SQLite file (sqlite3 ... DROP/DELETE/UPDATE/ALTER/TRUNCATE), remind Claude
# to checkpoint or work in a fork. Advisory only: never denies, never blocks,
# and stays silent when offshoot is not installed.
set -u
command -v offshoot >/dev/null 2>&1 || exit 0
input=$(cat 2>/dev/null || true)
python3 - "$input" <<'EOF'
import json, re, sys
try:
    data = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)
cmd = (data.get("tool_input") or {}).get("command") or ""
if not re.search(r"sqlite3|\.db\b|\.sqlite\b", cmd):
    sys.exit(0)
if not re.search(r"\b(DROP|DELETE|TRUNCATE|ALTER|UPDATE)\b", cmd, re.I):
    sys.exit(0)
ctx = (
    "This Bash command runs destructive SQL against a SQLite file. If that file is an "
    "offshoot checkout, take offshoot_checkpoint first (or run the command in an "
    "offshoot_fork) so the change can be undone with offshoot_rollback — /rewind "
    "does not undo Bash-made database changes."
)
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": ctx}}))
EOF
exit 0
```

`chmod +x plugin/hooks/*.sh`.

- [ ] **Step 5: Create the skill**

`plugin/skills/offshoot/SKILL.md`:

```markdown
---
name: offshoot
description: Use when working on a project that has an offshoot store (a .offshoot directory or OFFSHOOT_STORE) and you are about to change a SQLite database — schema migrations, bulk deletes, data experiments, or running several attempts at a fix. Teaches the fork-before-risky-work loop with the offshoot_* MCP tools.
---

# offshoot: branch the database before you change it

offshoot gives every attempt its own real SQLite database — a copy-on-write
fork, not a mock and not a re-seed. Every checkout is a stock `.db` file any
SQLite client opens. You have eight tools; the loop is four calls.

## The loop

1. **Orient:** `offshoot_list` — see databases, branches, checkpoints, and
   which branches are protected (`main` is, by default).
2. **Fork before risk:** `offshoot_fork {database, new_branch, branch?, at?, ttl?, meta?}`
   — instant, two small metadata objects, no data copy. Forks expire after
   the server's default TTL (24h) unless you pass `ttl`. Then
   `offshoot_checkout {database, branch: new_branch}` and point your SQL at
   the returned path.
3. **Checkpoint when tests pass:** `offshoot_checkpoint {database, branch, name, meta?}`.
   Name checkpoints for what they mean (`before-migration`, `migrated`).
4. **Roll back when they fail:** `offshoot_rollback {database, branch, to}`
   returns the branch to a checkpoint, discarding everything since.
5. **Promote the winner:** `offshoot_promote {database, source, target, force?}`
   repoints `target` (often `main`) at `source`'s head. Protected targets
   refuse without `force`; that refusal is confirmation you need, not a bug.
   The target's previous head is kept as `<target>-pre-promote` — the result
   names it — so a promote is undone by promoting that fork back.
6. **Clean up:** `offshoot_destroy` a failed attempt, or let its TTL expire.
   `offshoot_touch {database, branch, ttl?}` keeps a fork alive if a task
   runs long.

## Rules

- Never run a migration, `DROP`, `DELETE`, or `UPDATE` without a checkpoint
  or a fork to return to. `/rewind` restores Claude's file edits only; it does
  not undo what a Bash `sqlite3` command did to a database.
- Prefer forking to backing up by hand. Prefer a longer `ttl` to `ttl: "none"`.
- Read results: `structuredContent` carries `txid`, `path`, `ttl`, `backup`
  for you to reuse; the text is the same information for the human.
- If a tool says a daemon session is open on a branch, close it first
  (`offshoot session close`) rather than forcing.
- Work in the fork's checkout path, not `main`'s, while experimenting.

## Rules-file snippet for the project's AGENTS.md or CLAUDE.md

```
Database changes: this project uses offshoot. Before any schema migration,
bulk delete, or data experiment, fork the database (offshoot_fork) and work
in the fork's checkout; checkpoint when tests pass; roll back when they
fail; promote the fork that worked. Never run destructive SQL on main's
checkout directly.
```

Docs: https://sricola.github.io/offshoot/docs/agents/ · machine-readable index:
https://sricola.github.io/offshoot/llms.txt
```

- [ ] **Step 6: Wire CI and run the check**

In `.github/workflows/ci.yml`, in the `metrics-lint` job, append after its last `run:` step:

```yaml
      - name: make check-plugin
        run: make check-plugin
```

Run: `make check-plugin`
Expected: `check-plugin: ok`.

Manual smoke (record the output in the commit message body if it works): `claude plugin marketplace add /Users/sray/gits/offshoot && claude plugin install offshoot@offshoot` — if the `claude` CLI is unavailable in the executing environment, skip and say so.

- [ ] **Step 7: Commit**

```bash
git add .claude-plugin plugin scripts/check-plugin.sh Makefile .github/workflows/ci.yml
git commit -m "plugin: Claude Code plugin with skill, advisory hooks, and marketplace

plugin/ bundles the offshoot MCP server, a skill that teaches the
fork-before-risky-work loop and ships a rules-file snippet, a SessionStart
hook that injects the store's branches, and a PreToolUse(Bash) hook that
reminds the model to checkpoint before destructive SQL. Both hooks are
advisory (never deny, silent without offshoot). The repo-root marketplace
makes install two commands: claude plugin marketplace add sricola/offshoot
&& claude plugin install offshoot@offshoot. make check-plugin validates the
bundle in CI.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 6: Docs, README, and the re-captured walkthrough

**Files:**
- Modify: `README.md` (MCP section around line 455; "seven tools")
- Modify: `docs/agents.md` (lines 18, 24, tool table, new sections)
- Modify: `docs/reference.md:1129` and the MCP section following it
- Modify: `docs/demo/mcp-walkthrough.md` (tools/list block lines ~120-175; line 36 "seven")
- Modify: `docs/status.md:123` (MCP meta row) and the Integration Surface rows
- Modify: `CHANGELOG.md` (Unreleased)

**Interfaces:**
- Consumes: Tasks 1-5 as committed. The Cursor deeplink is `cursor://anysphere.cursor-deeplink/mcp/install?name=offshoot&config=eyJjb21tYW5kIjoib2Zmc2hvb3QiLCJhcmdzIjpbIm1jcCJdfQ==` (base64 of `{"command":"offshoot","args":["mcp"]}`); badge images `https://cursor.com/deeplink/mcp-install-dark.svg` / `-light.svg` (both verified 200).

- [ ] **Step 1: Re-capture the real tools/list**

```bash
cd /Users/sray/gits/offshoot && D=$(mktemp -d) && go build -o "$D/offshoot" ./cmd/offshoot && OFFSHOOT_STORE="$D/store" "$D/offshoot" init >/dev/null && printf '%s\n' \
'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"claude-code","version":"1"}}}' \
'{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | OFFSHOOT_STORE="$D/store" "$D/offshoot" mcp 2>/dev/null | grep '"id":2' > "$D/tools.json" && python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps(d, indent=2, ensure_ascii=False))' "$D/tools.json" > "$D/tools.pretty.json" && echo "$D/tools.pretty.json"
```

Replace the walkthrough's ` ```json ` block that begins with the `tools/list` response (the block containing `"name": "offshoot_list"` through the closing of the `tools` array, roughly lines 120-175) with the pretty-printed real capture, keeping the doc's existing truncation convention: descriptions may be shortened with `...` ONLY where the doc already quotes them in full elsewhere, and the italic note under the block must stay true. Every `annotations` object, the `meta` properties, and the `offshoot_touch` entry must appear exactly as captured. Change line 36's "seven" to "eight".

- [ ] **Step 2: Update docs/agents.md**

- Line 18: "The agent gets eight tools"; line 24 heading "## The eight tools".
- Table: add `meta?` to `offshoot_checkpoint` and `offshoot_fork` argument lists; add a row `| \`offshoot_touch\` | \`database\`, \`branch?\`, \`ttl?\` | Reset a fork's activity clock so its TTL does not expire mid-task; \`ttl\` sets or (\`"none"\`) clears it |`.
- Add a section after the table:

```markdown
## What the host sees: annotations and structured results

Every tool carries the MCP spec's behavior hints, set explicitly:
`offshoot_list` is read-only; `checkout`, `fork`, `checkpoint`, and `touch`
are non-destructive; `rollback`, `promote`, and `destroy` are destructive,
so a host that honors `destructiveHint` prompts before them. Every
successful result also returns `structuredContent` (snake_case JSON:
`txid`, `path`, `ttl`, `expires_at`, `backup`, …) next to the prose, so a
harness reads the fields instead of parsing sentences. `fork` and
`checkpoint` accept `meta` (string→string, at most 32 keys) to tag a
branch or checkpoint with a run id, git SHA, or agent name.
```

- Replace the "One command to wire up Claude Code" section body with three install paths:

```markdown
## Wire it into your agent

**Claude Code plugin** (MCP server + a skill that teaches the loop + advisory hooks):

```
claude plugin marketplace add sricola/offshoot
claude plugin install offshoot@offshoot
```

**Claude Code, MCP only:**

```
claude mcp add offshoot -- offshoot -store ./.offshoot mcp
```

**Cursor:** [![Install in Cursor](https://cursor.com/deeplink/mcp-install-dark.svg)](cursor://anysphere.cursor-deeplink/mcp/install?name=offshoot&config=eyJjb21tYW5kIjoib2Zmc2hvb3QiLCJhcmdzIjpbIm1jcCJdfQ==)
(the link installs `offshoot mcp` as a stdio server; the store resolves from `OFFSHOOT_STORE` or `./.offshoot`).

`offshoot mcp` speaks the Model Context Protocol on stdio — no daemon
required for the baseline. The plugin's skill is the same text as the
["rules-file snippet"](#rules-file-snippet) below; its hooks only add
context (they never block a command), and they stay silent when `offshoot`
is not installed.
```

- Add a section `## Rules-file snippet` containing the same snippet as the skill (the fenced block starting "Database changes: this project uses offshoot."), and a sentence pointing at `https://sricola.github.io/offshoot/llms.txt` for agents that index docs.

- [ ] **Step 3: Update README, reference, status, CHANGELOG**

- `README.md` MCP section: "eight tools — list, checkout, checkpoint, fork, rollback, promote, destroy, touch"; add the two-command plugin install and the Cursor badge (same markdown as above) directly under the existing `claude mcp add` line.
- `docs/reference.md:1129`: "eight tools (`offshoot_list`, … , `offshoot_touch`)"; in the MCP section, add one paragraph each for annotations, `structuredContent`, `meta`, and `offshoot_touch` (semantics as in agents.md), and note the plugin install path.
- `docs/status.md:123`: change the MCP metadata row's status from **deliberately deferred** to **shipped-and-tested**, notes: "`offshoot_fork`/`offshoot_checkpoint` take `meta` (`TestForkAndCheckpointCarryMeta`); v0.2.11". Add rows: "MCP tool annotations (explicit read-only/destructive/idempotent hints, openWorld false) | shipped-and-tested | `TestToolAnnotationsClassifyEveryTool`", "MCP structuredContent on every success | shipped-and-tested | `TestStructuredContentAccompaniesProse`", "`offshoot_touch` | shipped-and-tested | `TestTouchExtendsALeasedAttempt`", "Claude Code plugin + marketplace (`plugin/`, `.claude-plugin/marketplace.json`) | shipped | `make check-plugin` in CI; hooks advisory only".
- `CHANGELOG.md` under `## [Unreleased]`, add `### Added` bullets: annotations; structuredContent; `meta` on fork/checkpoint; `offshoot_touch`; the Claude Code plugin and Cursor install link; and a `### Changed` bullet for the docs (eight tools, install paths).

- [ ] **Step 4: Verify**

Run: `go test ./internal/mcp -count=1 && make check-plugin && grep -rn -i 'seven tools\|seven lifecycle\|the seven' README.md docs internal/mcp | grep -v CHANGELOG`
Expected: tests pass, check ok, grep prints nothing.

Also run the repo's link check if one exists (`grep -n 'link' Makefile`); otherwise skip and say so.

- [ ] **Step 5: Commit**

```bash
git add README.md docs CHANGELOG.md
git commit -m "docs: eight MCP tools, annotations, structured results, plugin and Cursor install

Walkthrough tools/list re-captured from a real offshoot mcp run.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

### Task 7: Three doc-drift fixes

**Files:**
- Modify: `docs/limitations.md:126-127`
- Modify: `docs/recipes/kubernetes.md:155` and `:269`
- Modify: `ROADMAP.md:120-121`

- [ ] **Step 1: Confirm the drift**

Run: `grep -n -i 'minio' docs/limitations.md; grep -n -i 'container image\|does not (yet)\|build your own' docs/recipes/kubernetes.md; grep -n 'v0\.4' ROADMAP.md`
Expected: the lines listed in Files.

- [ ] **Step 2: Fix**

- `docs/limitations.md:126-127`: replace "MinIO (conformance suite runs against real MinIO in CI on every PR)" with "RustFS (conformance suite runs against real RustFS in CI on every PR; MinIO was verified through v0.2.9 and is no longer re-verified since its images were withdrawn)".
- `docs/recipes/kubernetes.md:155`: replace the "offshoot does not (yet) publish a container image" clause with "the release workflow publishes `ghcr.io/sricola/offshoot:<tag>` (linux/amd64 and linux/arm64) on every tagged release; pin a tag". Line 269: replace "No official offshoot container image yet — build your own from this" with "Official image: `ghcr.io/sricola/offshoot:v0.2.10` (multi-arch). To build your own from this".
- `ROADMAP.md:120`: "(Full metrics endpoint lands in v0.4.)" → "(The full metrics endpoint shipped in Milestone 4.)"; line 121: "No budgets yet (v0.4)" → "Budgets shipped in Milestone 4 (ro-cache disk budget; FD budget still deferred)".

- [ ] **Step 3: Verify and commit**

Run: `grep -n -i 'real MinIO in CI' docs/limitations.md; grep -n 'does not (yet)' docs/recipes/kubernetes.md; grep -n 'v0\.4' ROADMAP.md`
Expected: no output.

```bash
git add docs/limitations.md docs/recipes/kubernetes.md ROADMAP.md
git commit -m "docs: fix three stale claims (RustFS in CI, GHCR image exists, M4 shipped)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_015DLArbhDMc9xJ2TjFw6d5B"
```

---

## Self-review notes

- Spec coverage: annotations (T1), structuredContent (T2), `meta` + `offshoot_touch` cheap deferreds (T3, T4), SKILL/AGENTS-snippet/plugin/hooks/Cursor (T5, T6), llms.txt linked (T5, T6; already emitted by the site), doc drift (T7). Not in scope, by decision: MCP registry submission (package publication deferred).
- Type consistency: `ToolAnnotations` pointer bools everywhere; `StructuredResult(data, format, args...)`; `optMeta` returns `prop` with `extra`; `touchArgs` fields `database/branch/ttl`.
- The walkthrough's `tools/list` must come from a real run (T6 step 1), never typed.
