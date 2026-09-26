package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
	"github.com/sricola/offshoot/internal/testutil"
)

// newTools builds a tool set with no default fork TTL, unless a single
// defaultTTL duration is passed (variadic so every existing call site is
// unaffected).
func newTools(t *testing.T, defaultTTL ...time.Duration) (*OffshootTools, *ops.Workspace) {
	t.Helper()
	testutil.RequireSQLite3(t)
	spec := filepath.Join(t.TempDir(), "store")
	w, err := ops.Init(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Create("app"); err != nil {
		t.Fatal(err)
	}
	var ttl time.Duration
	if len(defaultTTL) > 0 {
		ttl = defaultTTL[0]
	}
	// No daemon listens here: every test built on this harness exercises
	// the at-rest fallback path (see daemon_test.go's
	// TestNoDaemonBehavesExactlyAtRest, which spot-checks that a bad socket
	// produces identical behavior to the pre-daemon-integration at-rest
	// tools below).
	sock := filepath.Join(t.TempDir(), "absent.sock")
	return NewOffshootTools(w, spec, ttl, sock), w
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
	if len(tools) < 9 {
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
		{"offshoot_diff", map[string]any{"database": "app", "left": escaping, "right": "main"}},
		{"offshoot_diff", map[string]any{"database": "app", "left": "main", "right": escaping}},
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
	"offshoot_touch":      touchArgs{},
	"offshoot_diff":       diffArgs{},
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
	case reflect.Map:
		return "object"
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
//
// r.IsError alone doesn't prove validateNames is what rejected the name:
// ops and store.ValidateName independently reject the same traversal (see
// validateNames's own doc comment — "defense in depth"), so even with
// validateNames deleted from checkout()/fork() entirely, every case here
// would still come back as a tool error, just via ops's generic
// `store: invalid name %q ...` message instead of the handler's own
// `invalid database %q: ...` / `invalid new_branch %q: ...` message. That
// handler-added wording — the argument's label, spelled out — is the one
// thing only validateNames contributes; ops has no idea whether the name it
// rejected was a "database" or a "branch". So beyond r.IsError, each
// non-empty case here also asserts the message contains that handler-level
// wording, which is what actually pins validateNames (not just "some layer
// somewhere") as the thing doing the rejecting.
//
// The empty-database case can't make that assertion: it's caught by
// checkout's own `a.Database == ""` guard before validateNames ever runs
// (see TestToolsRejectPathEscapingNamesViaHandlerNotPreCheck below, which
// sidesteps that guard by using only non-empty bad names).
func TestToolsRejectPathEscapingNames(t *testing.T) {
	ts, _ := newTools(t)
	for _, bad := range []string{"../etc", "a/b", "..", ".", "UPPER", ""} {
		r := call(t, ts, "offshoot_checkout", map[string]any{"database": bad})
		if !r.IsError {
			t.Errorf("database %q must be refused", bad)
		}
		if bad == "" {
			continue // caught by checkout's pre-validateNames empty-string guard.
		}
		want := fmt.Sprintf("invalid database %q", bad)
		if !strings.Contains(text(r), want) {
			t.Errorf("database %q: message = %q, want it to contain the handler's own %q "+
				"(a bare ops/store error here would mean validateNames, not just some layer, stopped catching it)",
				bad, text(r), want)
		}
	}
	for _, bad := range []string{"../x", "a/b", ".."} {
		r := call(t, ts, "offshoot_fork", map[string]any{
			"database": "app", "new_branch": bad})
		if !r.IsError {
			t.Errorf("branch %q must be refused", bad)
		}
		want := fmt.Sprintf("invalid new_branch %q", bad)
		if !strings.Contains(text(r), want) {
			t.Errorf("new_branch %q: message = %q, want it to contain the handler's own %q "+
				"(a bare ops/store error here would mean validateNames, not just some layer, stopped catching it)",
				bad, text(r), want)
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

// TestToolResultNeverClaimsUndeliveredDurability originally only checked
// that checkout's result avoided the literal words "durable" and "saved".
// That's keyword-only: a message reading "permanently committed and will
// never be lost" contains neither word and would have sailed through while
// making exactly the false claim this test exists to catch. Absence of two
// words isn't evidence of anything; an agent needs to be told what to do,
// not just spared a lie. So this asserts the positive: the result must
// affirmatively tell the agent the checkout isn't checkpointed yet and name
// the tool (offshoot_checkpoint) that makes it so — see the checkout()
// handler in tools.go, which was changed to say exactly that.
func TestToolResultNeverClaimsUndeliveredDurability(t *testing.T) {
	ts, _ := newTools(t)
	co := call(t, ts, "offshoot_checkout", map[string]any{"database": "app"})
	path := strings.TrimSpace(lastPath(text(co)))
	if out, err := exec.Command("sqlite3", path,
		"CREATE TABLE t (v); INSERT INTO t VALUES (1);").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	msg := text(co)
	low := strings.ToLower(msg)
	// checkout's result must not tell the agent the write is saved — nothing
	// has been checkpointed yet.
	if strings.Contains(low, "durable") || strings.Contains(low, "saved") {
		t.Fatalf("checkout must not imply durability: %s", msg)
	}
	// And it must do more than avoid a false claim: it must affirmatively
	// guide the agent toward the action (checkpointing) that this state
	// needs before it's safe from being lost.
	if !strings.Contains(low, "checkpoint") {
		t.Fatalf("checkout result must tell the agent a checkpoint is needed before this state is durable: %s", msg)
	}
	if !strings.Contains(msg, "offshoot_checkpoint") {
		t.Fatalf("checkout result must name the tool (offshoot_checkpoint) the agent needs to call: %s", msg)
	}
}

// --- Task 3: MCP forks carry TTLs ---

// refTTL reads back new_branch's TTL directly from the store, the same way
// `offshoot status` does, so these tests assert on the ref that was actually
// written rather than trusting the tool's own response text.
func refTTL(t *testing.T, w *ops.Workspace, db, branch string) string {
	t.Helper()
	ref, _, err := w.Store.GetRef(db, branch)
	if err != nil {
		t.Fatalf("GetRef(%s, %s): %v", db, branch, err)
	}
	return ref.TTL
}

// TestForkExplicitTTLIsApplied covers brief case (a): an explicit `ttl` on
// the fork call lands on the child ref, rendered through Go's canonical
// time.Duration.String() ("2h" -> "2h0m0s"), matching how every other TTL
// surface (CLI, Status) round-trips it.
func TestForkExplicitTTLIsApplied(t *testing.T) {
	ts, w := newTools(t)
	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1", "ttl": "2h"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	if got := refTTL(t, w, "app", "attempt-1"); got != "2h0m0s" {
		t.Fatalf("ref TTL = %q, want canonical \"2h0m0s\"", got)
	}
}

// TestForkFallsBackToDefaultTTL covers brief case (b): a fork that omits
// `ttl` entirely picks up NewOffshootTools' configured DefaultTTL.
func TestForkFallsBackToDefaultTTL(t *testing.T) {
	ts, w := newTools(t, 24*time.Hour)
	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	if got := refTTL(t, w, "app", "attempt-1"); got != "24h0m0s" {
		t.Fatalf("ref TTL = %q, want canonical \"24h0m0s\" (the configured default)", got)
	}
}

// TestForkExplicitNoneOverridesDefaultTTL covers brief case (c): `ttl:"none"`
// always yields no TTL, even when NewOffshootTools has a configured default
// — the explicit argument wins.
func TestForkExplicitNoneOverridesDefaultTTL(t *testing.T) {
	ts, w := newTools(t, 24*time.Hour)
	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1", "ttl": "none"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	if got := refTTL(t, w, "app", "attempt-1"); got != "" {
		t.Fatalf("ref TTL = %q, want \"\" (ttl:\"none\" must override the default)", got)
	}
}

// TestForkUnparseableTTLIsAToolError covers brief case (d): a TTL the
// handler can't parse comes back as a tool error (not an RPC fault) that
// names the accepted forms, so the agent can retry with a valid value.
func TestForkUnparseableTTLIsAToolError(t *testing.T) {
	ts, _ := newTools(t)
	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1", "ttl": "bananas"})
	if !r.IsError {
		t.Fatal("ttl:\"bananas\" must be a tool error")
	}
	if !strings.Contains(strings.ToLower(text(r)), "duration") {
		t.Fatalf("error should name the accepted forms (a Go duration, or \"none\"): %s", text(r))
	}
}

// TestForkNegativeOrZeroTTLIsRefused pins that fork's explicit `ttl`, unlike
// touch, has no "none" concept for a non-positive parsed duration: "0s" or
// "-1h" is refused rather than silently treated as no TTL (see
// ops.Workspace.Fork's own doc for the same rule at the ops layer).
func TestForkNegativeOrZeroTTLIsRefused(t *testing.T) {
	ts, _ := newTools(t)
	for _, bad := range []string{"0s", "-1h"} {
		r := call(t, ts, "offshoot_fork", map[string]any{
			"database": "app", "new_branch": "attempt-" + bad, "ttl": bad})
		if !r.IsError {
			t.Errorf("ttl:%q must be refused (positive only, or \"none\")", bad)
		}
	}
}

// TestForkSchemaAdvertisesTTL covers brief case (e): the fork tool's schema
// advertises `ttl` as a string property, so an agent that introspects the
// schema (rather than reading the Description) still discovers the
// argument.
func TestForkSchemaAdvertisesTTL(t *testing.T) {
	ts, _ := newTools(t)
	for _, tl := range ts.Tools() {
		if tl.Name != "offshoot_fork" {
			continue
		}
		schemaMap, ok := tl.InputSchema.(map[string]any)
		if !ok {
			t.Fatalf("offshoot_fork: InputSchema is not a map[string]any: %T", tl.InputSchema)
		}
		props, _ := schemaMap["properties"].(map[string]any)
		prop, ok := props["ttl"].(map[string]any)
		if !ok {
			t.Fatalf("offshoot_fork: schema does not advertise a \"ttl\" property: %v", props)
		}
		if prop["type"] != "string" {
			t.Errorf("offshoot_fork: ttl property type = %v, want \"string\"", prop["type"])
		}
		if !strings.Contains(strings.ToLower(tl.Description), "expire") {
			t.Errorf("offshoot_fork: Description must state the default TTL behavior: %s", tl.Description)
		}
		if !strings.Contains(strings.ToLower(tl.Description), "janitor") {
			t.Errorf("offshoot_fork: Description must note reaping requires a running janitor: %s", tl.Description)
		}
		return
	}
	t.Fatal("offshoot_fork not found in Tools()")
}

// TestForkDescriptionStatesItsRealCost pins the storage/latency boundary an
// agent sees before choosing the tool. A shared fork is tiny, not free, and
// only a named-checkpoint fork avoids the default at-head checkout hash.
func TestForkDescriptionStatesItsRealCost(t *testing.T) {
	ts, _ := newTools(t)
	for _, tl := range ts.Tools() {
		if tl.Name != "offshoot_fork" {
			continue
		}
		desc := strings.ToLower(tl.Description)
		for _, fact := range []string{"two small metadata objects", "named checkpoint", "at-head", "hashes the checkout"} {
			if !strings.Contains(desc, fact) {
				t.Errorf("offshoot_fork description must include %q: %s", fact, tl.Description)
			}
		}
		if strings.Contains(desc, "costs nothing") {
			t.Errorf("offshoot_fork description must not claim a zero-cost fork: %s", tl.Description)
		}
		return
	}
	t.Fatal("offshoot_fork not found in Tools()")
}

// --- PM amendment: the fork response echoes the applied TTL and expiry ---

// TestForkResponseEchoesTTLAndExpiry pins the PM amendment: the fork tool's
// RESPONSE text (not just the ref written to the store) must state the
// applied TTL and a computed expiry timestamp, so both land in the agent's
// transcript and it can reason about them later without a separate
// offshoot_list/offshoot_checkout round trip. It must also carry the same
// "requires a running janitor" caveat as the Description, since a
// daemonless MCP setup only reaps on `offshoot gc`.
func TestForkResponseEchoesTTLAndExpiry(t *testing.T) {
	ts, _ := newTools(t)
	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1", "ttl": "2h"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	msg := text(r)
	if !strings.Contains(msg, "2h0m0s") {
		t.Errorf("response must echo the applied TTL (canonical \"2h0m0s\"): %s", msg)
	}
	// Anchor on the "expires_at=" label forkTTLSummaryFromFields actually
	// emits, not a bare "20" substring — the latter is nearly vacuous: it
	// happens to pass today only because the current year starts with "20",
	// so it would keep passing even if the degraded path (see
	// forkTTLFieldsFromRef's doc comment) dropped expires_at while some
	// OTHER "20"-containing text (a txid, a future year, an unrelated
	// number) remained in the message.
	if !strings.Contains(msg, "expires_at=") {
		t.Errorf("response must echo a computed expiry timestamp (expires_at=...): %s", msg)
	}
	if !strings.Contains(strings.ToLower(msg), "janitor") {
		t.Errorf("response must note that reaping requires a running janitor: %s", msg)
	}
}

// TestForkResponseNoTTLSaysSo covers the no-TTL case: the response should
// clearly say the fork never expires rather than silently omitting any TTL
// mention (which would be ambiguous with a bug that dropped the TTL text).
func TestForkResponseNoTTLSaysSo(t *testing.T) {
	ts, _ := newTools(t)
	r := call(t, ts, "offshoot_fork", map[string]any{
		"database": "app", "new_branch": "attempt-1"})
	if r.IsError {
		t.Fatalf("fork: %s", text(r))
	}
	msg := strings.ToLower(text(r))
	if !strings.Contains(msg, "none") && !strings.Contains(msg, "never") && !strings.Contains(msg, "no ttl") {
		t.Errorf("response should say the fork has no TTL: %s", text(r))
	}
}

// TestForkTTLSummaryKeepsJanitorNoteWhenReReadFails is the regression test
// for the whole-branch review finding that the fork TTL summary's degraded
// path dropped the janitor caveat along with the computed expiry when the
// post-fork GetRef re-read fails — even though the caveat is a static fact
// about how reaping works, not something derived from that re-read, so it
// should never have depended on it succeeding. Exercised directly against
// forkTTLFieldsFromRef + forkTTLSummaryFromFields (rather than through the
// fork tool end to end) by asking for a branch that was never forked, so
// GetRef fails exactly the way a real race (the ref vanishing between fork
// and re-read) would.
func TestForkTTLSummaryKeepsJanitorNoteWhenReReadFails(t *testing.T) {
	_, ws := newTools(t)
	ref, _, err := ws.Store.GetRef("app", "never-forked")
	ttlStr, expiresAt := forkTTLFieldsFromRef(ref, err, time.Hour)
	msg := forkTTLSummaryFromFields(ttlStr, expiresAt)
	if strings.Contains(msg, "expires_at=") {
		t.Fatalf("expected the degraded (no re-read) path, but got an expiry anyway: %s", msg)
	}
	if !strings.Contains(strings.ToLower(msg), "janitor") {
		t.Errorf("degraded path must still carry the janitor caveat — it doesn't depend on the re-read: %s", msg)
	}
	if !strings.Contains(msg, "1h0m0s") {
		t.Errorf("degraded path must still echo the applied TTL: %s", msg)
	}
}

// TestPromoteKeepsSafetyForkAndSaysSo: offshoot_promote always keeps the
// target's previous head as <target>-pre-promote (agents get no opt-out —
// this is the safe-by-default path), its TTL follows the configured fork
// default, and the result text names the fork so the agent knows its undo
// handle. The description states the mechanism so the model can plan on it.
func TestPromoteKeepsSafetyForkAndSaysSo(t *testing.T) {
	ts, w := newTools(t, 2*time.Hour)
	if _, err := w.Fork("app", "main", "attempt-1", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	r := call(t, ts, "offshoot_promote", map[string]any{
		"database": "app", "source": "attempt-1", "target": "main", "force": true})
	if r.IsError {
		t.Fatalf("promote: %s", text(r))
	}
	backup := "main" + ops.PromoteBackupSuffix
	if !strings.Contains(text(r), "app@"+backup) {
		t.Fatalf("result must name the safety fork %s: %s", backup, text(r))
	}
	ref, _, err := w.Store.GetRef("app", backup)
	if err != nil {
		t.Fatalf("safety fork must exist: %v", err)
	}
	if ref.TTL != "2h0m0s" {
		t.Fatalf("safety fork TTL must follow the configured fork default, got %q", ref.TTL)
	}
	for _, tl := range ts.Tools() {
		if tl.Name == "offshoot_promote" && !strings.Contains(tl.Description, "-pre-promote") {
			t.Fatalf("offshoot_promote description must state the safety fork: %s", tl.Description)
		}
	}
}

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
		"offshoot_touch":      {false, false, true},
		"offshoot_diff":       {true, false, true},
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
	// sc round-trips the whole ToolResult through JSON — the actual wire
	// encoding an MCP client receives — instead of reading
	// r.StructuredContent as the in-process Go value the handler happened to
	// construct (same "prove it survives the wire" reason
	// TestToolAnnotationsSerializeOnTheWire marshals/unmarshals tools/list).
	// The decoder uses UseNumber() so a later exact numeric comparison isn't
	// silently lossy: json.Unmarshal's default float64 can't represent every
	// uint64 exactly, and a txid is a uint64.
	sc := func(r ToolResult) map[string]any {
		t.Helper()
		if r.IsError {
			t.Fatalf("unexpected error: %s", text(r))
		}
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal ToolResult: %v", err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var whole map[string]any
		if err := dec.Decode(&whole); err != nil {
			t.Fatalf("unmarshal ToolResult: %v", err)
		}
		m, ok := whole["structuredContent"].(map[string]any)
		if !ok {
			t.Fatalf("structuredContent is %T, want map[string]any (wire: %s)", whole["structuredContent"], raw)
		}
		return m
	}
	// txid asserts m[key] round-tripped as an exact uint64 (via json.Number,
	// not json.Unmarshal's default lossy-for-large-values float64) and that
	// it matches db@branch's actual HeadTXID freshly read from the store —
	// structuredContent must carry the real value, not merely be present.
	txid := func(m map[string]any, key, db, branch string) {
		t.Helper()
		n, ok := m[key].(json.Number)
		if !ok {
			t.Fatalf("%s is %T, want json.Number", key, m[key])
		}
		got, err := strconv.ParseUint(n.String(), 10, 64)
		if err != nil {
			t.Fatalf("%s = %q: %v", key, n, err)
		}
		ref, _, err := w.Store.GetRef(db, branch)
		if err != nil {
			t.Fatalf("GetRef(%s, %s): %v", db, branch, err)
		}
		if got != ref.HeadTXID {
			t.Fatalf("%s = %d, want %s@%s's actual head txid %d", key, got, db, branch, ref.HeadTXID)
		}
	}

	fork := sc(call(t, ts, "offshoot_fork", map[string]any{"database": "app", "new_branch": "attempt-2"}))
	if fork["new_branch"] != "attempt-2" || fork["ttl"] == nil {
		t.Fatalf("fork structuredContent = %v", fork)
	}
	txid(fork, "txid", "app", "attempt-2")

	// offshoot_checkpoint is at-rest here (no daemon session), and the ops
	// layer's Checkpoint requires an existing checkout to snapshot from
	// (see ops.Workspace.Checkpoint's os.Stat guard) — so checkout must run
	// before checkpoint, matching real CLI/agent usage (checkout, then
	// checkpoint), unlike the brief's checkpoint-then-checkout ordering.
	co := sc(call(t, ts, "offshoot_checkout", map[string]any{"database": "app", "branch": "attempt-1"}))
	if co["path"] == "" || co["path"] == nil {
		t.Fatalf("checkout structuredContent = %v", co)
	}
	cp := sc(call(t, ts, "offshoot_checkpoint", map[string]any{"database": "app", "branch": "attempt-1", "name": "v1"}))
	if cp["name"] != "v1" || cp["live"] != false {
		t.Fatalf("checkpoint structuredContent = %v", cp)
	}
	txid(cp, "txid", "app", "attempt-1")

	rb := sc(call(t, ts, "offshoot_rollback", map[string]any{"database": "app", "branch": "attempt-1", "to": "v1"}))
	if rb["to"] != "v1" || rb["path"] == nil {
		t.Fatalf("rollback structuredContent = %v", rb)
	}
	pr := sc(call(t, ts, "offshoot_promote", map[string]any{"database": "app", "source": "attempt-1", "target": "main", "force": true}))
	if pr["backup"] != "main"+ops.PromoteBackupSuffix {
		t.Fatalf("promote structuredContent = %v", pr)
	}
	txid(pr, "txid", "app", "main")

	ls := sc(call(t, ts, "offshoot_list", map[string]any{}))
	branches, ok := ls["branches"].([]any)
	if !ok || len(branches) < 3 {
		t.Fatalf("list structuredContent branches = %v", ls["branches"])
	}
	ds := sc(call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "attempt-2"}))
	if ds["branch"] != "attempt-2" {
		t.Fatalf("destroy structuredContent = %v", ds)
	}
	// Errors carry no structuredContent, checked both on the in-process
	// value and on the actual wire encoding — the omitempty tag must drop
	// the key entirely rather than emit a JSON null.
	bad := call(t, ts, "offshoot_destroy", map[string]any{"database": "app", "branch": "nope"})
	if !bad.IsError || bad.StructuredContent != nil {
		t.Fatalf("error results must not carry structuredContent: %+v", bad)
	}
	raw, err := json.Marshal(bad)
	if err != nil {
		t.Fatalf("marshal error ToolResult: %v", err)
	}
	if bytes.Contains(raw, []byte("structuredContent")) {
		t.Fatalf("error result must omit structuredContent on the wire: %s", raw)
	}
}

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
	var cp store.Checkpoint = ref.Checkpoints["v1"]
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

// TestDiffToolComparesAttemptsContentAware: the ninth tool answers "what
// changed between attempt A and B" for an agent, without sqldiff, and its
// structuredContent carries the per-table counts a harness needs.
func TestDiffToolComparesAttemptsContentAware(t *testing.T) {
	ts, w := newTools(t)
	p, err := w.Checkout("app", "main")
	if err != nil {
		t.Fatal(err)
	}
	sqlite := func(path, q string) {
		t.Helper()
		if out, err := exec.Command("sqlite3", path, q).CombinedOutput(); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
	}
	sqlite(p, "CREATE TABLE results (id INTEGER PRIMARY KEY, passed INT); INSERT INTO results VALUES (1,1),(2,1),(3,1);")
	if _, err := w.Checkpoint("app", "main", "seed", nil); err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{"attempt-1", "attempt-2"} {
		if _, err := w.Fork("app", "main", b, "seed", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	p2, _ := w.Checkout("app", "attempt-2")
	sqlite(p2, "UPDATE results SET passed=0 WHERE id=2; INSERT INTO results VALUES (4,1);")
	if _, err := w.Checkpoint("app", "attempt-2", "done", nil); err != nil {
		t.Fatal(err)
	}

	r := call(t, ts, "offshoot_diff", map[string]any{"database": "app", "left": "attempt-1@fork", "right": "attempt-2@done"})
	if r.IsError {
		t.Fatalf("diff: %s", text(r))
	}
	if !strings.Contains(text(r), "left:  app@attempt-1@fork right: app@attempt-2@done") || !strings.Contains(text(r), "1 tables: 0 same, 1 changed, 0 added, 0 removed") {
		t.Fatalf("diff text:\n%s", text(r))
	}
	sc := r.StructuredContent.(map[string]any)
	tables := sc["tables"].([]map[string]any)
	if len(tables) != 1 || tables[0]["changed"] != 1 || tables[0]["added"] != 1 || tables[0]["status"] != "changed" {
		t.Fatalf("structured tables = %v", tables)
	}
	if _, has := sc["full"]; has {
		t.Fatalf("full must be omitted when not requested: %v", sc)
	}
	// head-side left (no checkpoint) is allowed: attempt-1's head equals its fork point.
	if r := call(t, ts, "offshoot_diff", map[string]any{"database": "app", "left": "attempt-1", "right": "attempt-2@done"}); r.IsError {
		t.Fatalf("head-side diff: %s", text(r))
	}
	// bad shapes are tool errors
	for _, bad := range []map[string]any{
		{"database": "app", "left": "a@b@c", "right": "main"},
		{"database": "app", "left": "../x", "right": "main"},
		{"database": "app", "left": "main", "right": "main", "max_bytes": 1 << 20},
		{"database": "app", "left": "main", "right": "main", "table": "nope"},
	} {
		if r := call(t, ts, "offshoot_diff", bad); !r.IsError {
			t.Fatalf("args %v must be a tool error", bad)
		}
	}
	// full: sqldiff-dependent
	r = call(t, ts, "offshoot_diff", map[string]any{"database": "app", "left": "attempt-1@fork", "right": "attempt-2@done", "full": true, "table": "results"})
	if _, err := exec.LookPath("sqldiff"); err != nil {
		if !r.IsError || !strings.Contains(text(r), "sqldiff") {
			t.Fatalf("without sqldiff, full must say so: %s", text(r))
		}
	} else {
		if r.IsError || !strings.Contains(text(r), "UPDATE results") {
			t.Fatalf("full diff: %s", text(r))
		}
		if _, has := r.StructuredContent.(map[string]any)["full"]; !has {
			t.Fatal("full must be present in structuredContent when requested")
		}
	}
}
