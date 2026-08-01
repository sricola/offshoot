package mcp

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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

// --- Critical 1: agent-supplied names must be validated before ops ---

// TestToolsRejectEscapingNames pins the fix for the path-traversal finding:
// every MCP tool handler that takes a name-shaped argument (database,
// branch, new_branch, source, target, checkpoint names) must reject a
// malformed one — most importantly one that escapes the workspace via ".."
// — as a clean tool ErrorResult naming the offending argument, rather than
// letting it flow into ops (and from there into Workspace.CheckoutPath's
// bare filepath.Join).
func TestToolsRejectEscapingNames(t *testing.T) {
	const escaping = "../../../../tmp/offshoot-mcp-escape-poc"

	cases := []struct {
		tool string
		args map[string]any
	}{
		{"offshoot_checkout", map[string]any{"database": "app", "branch": escaping}},
		{"offshoot_checkout", map[string]any{"database": escaping}},
		{"offshoot_checkpoint", map[string]any{"database": "app", "branch": escaping, "name": "v1"}},
		{"offshoot_checkpoint", map[string]any{"database": "app", "name": escaping}},
		{"offshoot_fork", map[string]any{"database": "app", "new_branch": escaping}},
		{"offshoot_fork", map[string]any{"database": "app", "new_branch": "attempt-1", "branch": escaping}},
		{"offshoot_rollback", map[string]any{"database": "app", "to": escaping}},
		{"offshoot_rollback", map[string]any{"database": "app", "branch": escaping, "to": "init"}},
		{"offshoot_promote", map[string]any{"database": escaping, "source": "main", "target": "main"}},
		{"offshoot_promote", map[string]any{"database": "app", "source": escaping, "target": "main"}},
		{"offshoot_promote", map[string]any{"database": "app", "source": "main", "target": escaping}},
		{"offshoot_destroy", map[string]any{"database": "app", "branch": escaping}},
		{"offshoot_destroy", map[string]any{"database": escaping, "branch": "main"}},
	}

	for _, tc := range cases {
		t.Run(tc.tool+"/"+strings.ReplaceAll(escaping, "/", "_"), func(t *testing.T) {
			ts, _ := newTools(t)
			r := call(t, ts, tc.tool, tc.args)
			if !r.IsError {
				t.Fatalf("%s with args %v: want a tool error rejecting the escaping name, got success: %s",
					tc.tool, tc.args, text(r))
			}
			// The victim path must never be materialized outside the
			// workspace: confirm no file landed at what the traversal target
			// resolves to.
			victim := filepath.Join(os.TempDir(), "offshoot-mcp-escape-poc.db")
			if fi, statErr := os.Stat(victim); statErr == nil {
				t.Fatalf("%s: traversal file materialized outside the workspace: %s (%v)", tc.tool, victim, fi)
			}
		})
	}
}

// --- Critical 2: the schema helper must advertise each property's real type ---

// toolArgStructs maps every tool name to the Go struct its handler unmarshals
// `arguments` into (or nil for a tool that takes none), so
// TestToolSchemaTypesMatchArgStructs can compare declared JSON Schema types
// against what the handler actually accepts without hardcoding per-field
// checks.
var toolArgStructs = map[string]any{
	"offshoot_list":       nil,
	"offshoot_checkout":   checkoutArgs{},
	"offshoot_checkpoint": checkpointArgs{},
	"offshoot_fork":       forkArgs{},
	"offshoot_rollback":   rollbackArgs{},
	"offshoot_promote":    promoteArgs{},
	"offshoot_destroy":    destroyArgs{},
}

// jsonSchemaTypeForKind maps a Go reflect.Kind to the JSON Schema "type"
// value a field of that kind should be advertised as.
func jsonSchemaTypeForKind(k reflect.Kind) string {
	switch k {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	default:
		return "unsupported:" + k.String()
	}
}

// jsonFieldName extracts the field name from a `json:"name,omitempty"` tag.
func jsonFieldName(tag string) string {
	if i := strings.IndexByte(tag, ','); i >= 0 {
		return tag[:i]
	}
	return tag
}

// TestToolSchemaTypesMatchArgStructs pins the fix for the `force` schema-type
// finding: every tool's advertised InputSchema property type must match the
// Go type its handler's argument struct actually accepts, checked
// generically by reflection over every JSON-tagged field of every tool's arg
// struct (not a hardcoded check of `force` alone) — so a model that follows
// the schema literally (e.g. `"force":"true"` if the schema wrongly said
// "string") never hits a confusing unmarshal error, and the next
// schema/struct mismatch (of any field, any tool) gets caught here too.
func TestToolSchemaTypesMatchArgStructs(t *testing.T) {
	ts, _ := newTools(t)
	for _, tl := range ts.Tools() {
		argStruct, known := toolArgStructs[tl.Name]
		if !known {
			t.Fatalf("test does not know the argument struct for tool %q; add it to toolArgStructs", tl.Name)
		}

		schemaMap, ok := tl.InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("%s: InputSchema is not a map[string]any: %T", tl.Name, tl.InputSchema)
		}
		props, _ := schemaMap["properties"].(map[string]any)

		if argStruct == nil {
			if len(props) != 0 {
				t.Errorf("%s: takes no arguments but schema advertises properties: %v", tl.Name, props)
			}
			continue
		}

		typ := reflect.TypeOf(argStruct)
		fieldKindByJSONName := map[string]reflect.Kind{}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			name := jsonFieldName(f.Tag.Get("json"))
			if name == "" || name == "-" {
				continue
			}
			fieldKindByJSONName[name] = f.Type.Kind()
		}

		for propName, propSchemaAny := range props {
			propSchema, ok := propSchemaAny.(map[string]any)
			if !ok {
				t.Errorf("%s: property %q schema is not a map[string]any: %T", tl.Name, propName, propSchemaAny)
				continue
			}
			declaredType, _ := propSchema["type"].(string)
			kind, ok := fieldKindByJSONName[propName]
			if !ok {
				t.Errorf("%s: schema advertises property %q, but %s has no matching json-tagged field",
					tl.Name, propName, typ)
				continue
			}
			wantType := jsonSchemaTypeForKind(kind)
			if declaredType != wantType {
				t.Errorf("%s: property %q is declared %q in the schema but the handler's %s field is %s (want type %q)",
					tl.Name, propName, declaredType, typ, kind, wantType)
			}
		}

		// The inverse direction: every JSON-tagged struct field the handler
		// reads should be advertised in the schema too, so an agent following
		// the schema alone can discover every argument the handler accepts.
		for jsonName := range fieldKindByJSONName {
			if _, ok := props[jsonName]; !ok {
				t.Errorf("%s: %s field %q has no matching schema property", tl.Name, typ, jsonName)
			}
		}
	}
}

// --- Task 5: hostile-MCP-client adversarial pass ---

// TestToolsRejectPathEscapingNames is the brief's compact version of
// TestToolsRejectEscapingNames above: a broader sweep of malformed names
// (traversal, embedded slash, bare "." / "..", uppercase, and empty)
// across offshoot_checkout's database and offshoot_fork's new_branch.
// Every case except the empty string is caught by store.ValidateName inside
// validateNames (the empty database is caught one line earlier, by
// checkout's own `a.Database == ""` guard) — see the note in the report
// about that one incidental pass, and
// TestToolsRejectPathEscapingNamesViaHandlerNotPreCheck below for the
// strengthened version that removes it.
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

// TestToolsRejectPathEscapingNamesViaHandlerNotPreCheck strengthens
// TestToolsRejectPathEscapingNames above for the one case that would
// otherwise pass vacuously: an empty database string is refused by
// checkout's own `a.Database == ""` guard before validateNames (and
// store.ValidateName) ever runs, so that sub-case alone doesn't prove the
// handler's name-validation gate works. Every value used here is non-empty
// and reaches validateNames; each is rejected solely by store.ValidateName's
// rules (charset, "..", bare "."), which is the actual gate a hostile name
// must pass to reach ops.
func TestToolsRejectPathEscapingNamesViaHandlerNotPreCheck(t *testing.T) {
	ts, _ := newTools(t)
	for _, bad := range []string{"../etc", "a/b", "..", ".", "UPPER", "sp ace", "trailing/"} {
		r := call(t, ts, "offshoot_checkout", map[string]any{"database": bad})
		if !r.IsError {
			t.Errorf("database %q must be refused by validateNames", bad)
		}
	}
	for _, bad := range []string{"../x", "a/b", "..", "UPPER"} {
		r := call(t, ts, "offshoot_fork", map[string]any{
			"database": "app", "new_branch": bad})
		if !r.IsError {
			t.Errorf("new_branch %q must be refused by validateNames", bad)
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
