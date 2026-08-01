package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
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
	if name == "unknown_tool" {
		return ToolResult{}, fmt.Errorf("mcp: unknown tool %q", name)
	}
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

// --- Dual protocol revision support ---

// TestLegacyClientFullFlow pins the pre-existing 2025-11-25 handshake end to
// end and additionally asserts the legacy tools/list envelope carries none
// of the modern (2026-07-28) envelope fields — the two eras must stay
// visibly distinct on the wire.
func TestLegacyClientFullFlow(t *testing.T) {
	got := run(t,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"legacy-client","version":"1"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
	)
	if len(got) != 2 {
		t.Fatalf("want 2 responses (notification unanswered), got %d: %v", len(got), got)
	}

	initRes := got[0]["result"].(map[string]any)
	if initRes["protocolVersion"] != "2025-11-25" {
		t.Errorf("initialize protocolVersion = %v, want 2025-11-25", initRes["protocolVersion"])
	}

	listRes := got[1]["result"].(map[string]any)
	if _, ok := listRes["resultType"]; ok {
		t.Errorf("legacy tools/list result must not carry the modern resultType envelope: %v", listRes)
	}
	if _, ok := listRes["ttlMs"]; ok {
		t.Errorf("legacy tools/list result must not carry modern caching fields: %v", listRes)
	}
	tools, ok := listRes["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v", listRes["tools"])
	}
}

// TestModernClientFullFlow drives the 2026-07-28 flow a dual-era-aware
// client would use on stdio: probe with server/discover, then call
// tools/list with the required per-request _meta. Both responses must
// carry the modern envelope (resultType, caching fields, serverInfo).
func TestModernClientFullFlow(t *testing.T) {
	discover := `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	list := `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	got := run(t, discover, list)
	if len(got) != 2 {
		t.Fatalf("want 2 responses, got %d: %v", len(got), got)
	}

	dres := got[0]["result"].(map[string]any)
	if dres["resultType"] != "complete" {
		t.Errorf("discover resultType = %v, want complete", dres["resultType"])
	}
	supported, ok := dres["supportedVersions"].([]any)
	if !ok || len(supported) != 2 {
		t.Fatalf("discover supportedVersions = %v", dres["supportedVersions"])
	}
	if dres["cacheScope"] != "public" {
		t.Errorf("discover cacheScope = %v, want public", dres["cacheScope"])
	}
	if ttl, ok := dres["ttlMs"].(float64); !ok || ttl <= 0 {
		t.Errorf("discover ttlMs = %v, want a positive freshness hint", dres["ttlMs"])
	}
	dmeta, ok := dres["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("discover result missing _meta: %v", dres)
	}
	if si, ok := dmeta["io.modelcontextprotocol/serverInfo"].(map[string]any); !ok || si["name"] != "offshoot" {
		t.Errorf("discover serverInfo = %v", dmeta["io.modelcontextprotocol/serverInfo"])
	}

	lres := got[1]["result"].(map[string]any)
	if lres["resultType"] != "complete" {
		t.Errorf("tools/list resultType = %v, want complete", lres["resultType"])
	}
	if lres["cacheScope"] != "public" {
		t.Errorf("tools/list cacheScope = %v, want public", lres["cacheScope"])
	}
	if ttl, ok := lres["ttlMs"].(float64); !ok || ttl <= 0 {
		t.Errorf("tools/list ttlMs = %v, want a positive freshness hint", lres["ttlMs"])
	}
	tools, ok := lres["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v", lres["tools"])
	}
}

// TestLegacyToolsCallDispatch pins tools/call under the legacy (2025-11-25)
// era: the result is the bare ToolResult envelope (content/isError), with
// none of the modern era's resultType/_meta wrapper.
func TestLegacyToolsCallDispatch(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ping_tool","arguments":{}}}`)
	if len(got) != 1 || got[0]["error"] != nil {
		t.Fatalf("tools/call: %v", got)
	}
	res := got[0]["result"].(map[string]any)
	if _, ok := res["resultType"]; ok {
		t.Errorf("legacy tools/call result must not carry the modern resultType envelope: %v", res)
	}
	content, ok := res["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %v", res["content"])
	}
	block := content[0].(map[string]any)
	if block["text"] != "called ping_tool" {
		t.Errorf("content text = %v", block["text"])
	}
}

// TestModernToolsCallDispatch covers tools/call under the modern
// (2026-07-28) era: the result must carry resultType and serverInfo _meta,
// same as every other modern result, but — unlike tools/list — no
// ttlMs/cacheScope: a tool call is an action, not a cacheable value.
func TestModernToolsCallDispatch(t *testing.T) {
	req := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ping_tool","arguments":{},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`
	got := run(t, req)
	if len(got) != 1 || got[0]["error"] != nil {
		t.Fatalf("tools/call: %v", got)
	}
	res := got[0]["result"].(map[string]any)
	if res["resultType"] != "complete" {
		t.Errorf("tools/call resultType = %v, want complete", res["resultType"])
	}
	if _, ok := res["ttlMs"]; ok {
		t.Errorf("tools/call is not cacheable; must not carry ttlMs: %v", res)
	}
	if _, ok := res["cacheScope"]; ok {
		t.Errorf("tools/call is not cacheable; must not carry cacheScope: %v", res)
	}
	meta, ok := res["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("tools/call result missing _meta: %v", res)
	}
	if si, ok := meta["io.modelcontextprotocol/serverInfo"].(map[string]any); !ok || si["name"] != "offshoot" {
		t.Errorf("serverInfo = %v", meta["io.modelcontextprotocol/serverInfo"])
	}
	content, ok := res["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %v", res["content"])
	}
}

// TestUnknownToolIsInvalidParamsRPCError covers the boundary the brief
// draws: a user-level tool failure (bad args, no such checkpoint) comes
// back as an ErrorResult, but an unknown tool name is not a real tool at
// all — that's a protocol fault, surfaced as an RPC error (CodeInvalidParams).
func TestUnknownToolIsInvalidParamsRPCError(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"unknown_tool","arguments":{}}}`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %v", got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("want an error response, got %v", got[0])
	}
	if int(e["code"].(float64)) != CodeInvalidParams {
		t.Errorf("code = %v, want %d", e["code"], CodeInvalidParams)
	}
}

// TestUnsupportedProtocolVersionErrors covers a modern client requesting a
// revision this server doesn't implement: it must get
// UnsupportedProtocolVersionError (-32022) naming what it does support,
// per https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning.
func TestUnsupportedProtocolVersionErrors(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"1900-01-01","io.modelcontextprotocol/clientCapabilities":{}}}}`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %v", got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("want an error response, got %v", got[0])
	}
	if int(e["code"].(float64)) != CodeUnsupportedProtocolVersion {
		t.Errorf("code = %v, want %d", e["code"], CodeUnsupportedProtocolVersion)
	}
	data, ok := e["data"].(map[string]any)
	if !ok {
		t.Fatalf("error missing data: %v", e)
	}
	if data["requested"] != "1900-01-01" {
		t.Errorf("data.requested = %v", data["requested"])
	}
	supported, ok := data["supported"].([]any)
	if !ok || len(supported) != 2 {
		t.Fatalf("data.supported = %v", data["supported"])
	}
}

// TestModernRequestMissingClientCapabilitiesIsInvalidParams covers the
// spec's "a request missing any required _meta field is malformed" rule:
// clientCapabilities is required on every modern request.
func TestModernRequestMissingClientCapabilitiesIsInvalidParams(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %v", got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("want an error response, got %v", got[0])
	}
	if int(e["code"].(float64)) != CodeInvalidParams {
		t.Errorf("code = %v, want %d", e["code"], CodeInvalidParams)
	}
}

// --- Review fixes ---

// TestParseErrorResponseHasNullID pins the JSON-RPC 2.0 requirement that a
// response whose request id could not be determined still carries the id
// key, as JSON null — not an absent key. Response.ID used to be tagged
// `json:"id,omitempty"` on a json.RawMessage (a slice), so omitempty
// silently dropped the key whenever the id was nil.
func TestParseErrorResponseHasNullID(t *testing.T) {
	got := run(t, `{not json at all`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %v", got)
	}
	idVal, ok := got[0]["id"]
	if !ok {
		t.Fatalf("id key missing entirely from parse-error response, want present and null: %v", got[0])
	}
	if idVal != nil {
		t.Errorf("id = %v, want JSON null", idVal)
	}
}

// TestOversizedLineGetsErrorAndServerContinues covers a line over
// maxLineSize: it must produce a JSON-RPC error response, the same as a
// malformed line does, rather than silently killing the session.
func TestOversizedLineGetsErrorAndServerContinues(t *testing.T) {
	huge := strings.Repeat("x", maxLineSize+10)
	got := run(t, huge, `{"jsonrpc":"2.0","id":5,"method":"tools/list"}`)
	if len(got) != 2 {
		t.Fatalf("want an oversized-line error then a normal response, got %d: %v", len(got), got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("want an error response for the oversized line, got %v", got[0])
	}
	if int(e["code"].(float64)) != CodeInvalidRequest {
		t.Errorf("code = %v, want %d", e["code"], CodeInvalidRequest)
	}
	if got[1]["error"] != nil {
		t.Errorf("server must keep serving after an oversized line: %v", got[1])
	}
}

// TestServeCancelsPromptlyOnBlockedRead covers the doc comment's claim that
// Serve returns as soon as ctx is cancelled, even mid-read: it feeds Serve
// a reader that blocks forever (nothing is ever written to the pipe) and
// asserts Serve still returns promptly once ctx is cancelled, rather than
// hanging until the read itself unblocks (which here would be never).
func TestServeCancelsPromptlyOnBlockedRead(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewServer(pr, io.Discard, &fakeTools{})
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Serve error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return promptly after ctx was cancelled while a read was blocked")
	}
}

// --- Task 5: hostile-MCP-client adversarial pass ---

// TestOversizedAndWeirdInputDoesNotBreakTheStream covers a large-but-under-
// the-hard-cap tools/call argument (200KB, well past bufio.Scanner's old
// 64KB default but nowhere near maxLineSize): it must be parsed and served
// normally, and the connection must remain usable afterward. The over-cap
// case (a line past maxLineSize) is covered separately by
// TestOversizedLineGetsErrorAndServerContinues.
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

// TestOversizedAndWeirdInputDoesNotBreakTheStreamModernEra is the brief's
// large-argument case replayed on the 2026-07-28 per-request _meta path.
// The brief's own test only exercises the legacy handshake-based era;
// requestEra's classification and the modern envelope construction are
// separate code paths that a large-but-legal message could just as easily
// trip up (e.g. by mis-sizing a buffer before the _meta envelope is even
// unwrapped), so the same input is replayed here under modern framing.
func TestOversizedAndWeirdInputDoesNotBreakTheStreamModernEra(t *testing.T) {
	meta := `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}`
	huge := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ping_tool","arguments":{"blob":"` +
		strings.Repeat("x", 200_000) + `"},` + meta + `}}`
	list := `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{` + meta + `}}`
	got := run(t, huge, list)
	if len(got) != 2 {
		t.Fatalf("want 2 responses, got %d", len(got))
	}
	if got[0]["error"] != nil {
		t.Fatalf("modern tools/call must handle a large argument: %v", got[0])
	}
	if res, ok := got[0]["result"].(map[string]any); !ok || res["resultType"] != "complete" {
		t.Fatalf("modern tools/call result malformed after a large argument: %v", got[0])
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

// TestBatchMixingErasEachGetsExactlyOneCorrectlyShapedResponse hardens the
// brief's batch test across both protocol eras: since requestEra classifies
// each request independently (the server is stateless per request, not per
// connection — see server.go's package doc and requestEra), a hostile or
// merely confused client can interleave legacy and modern requests on one
// connection. Each response must still be unique and shaped for its own
// request's era, with no leakage from the previous line's classification.
func TestBatchMixingErasEachGetsExactlyOneCorrectlyShapedResponse(t *testing.T) {
	meta := `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}`
	var lines []string
	wantModern := map[int]bool{}
	for i := 0; i < 50; i++ {
		id := strconv.Itoa(i)
		if i%2 == 0 {
			lines = append(lines, `{"jsonrpc":"2.0","id":`+id+`,"method":"tools/list"}`)
			wantModern[i] = false
		} else {
			lines = append(lines, `{"jsonrpc":"2.0","id":`+id+`,"method":"tools/list","params":{`+meta+`}}`)
			wantModern[i] = true
		}
	}
	got := run(t, lines...)
	if len(got) != 50 {
		t.Fatalf("want 50 responses, got %d", len(got))
	}
	seen := map[float64]bool{}
	for _, r := range got {
		if r["error"] != nil {
			t.Fatalf("unexpected error in mixed-era batch: %v", r)
		}
		id := r["id"].(float64)
		if seen[id] {
			t.Fatalf("duplicate response for id %v", id)
		}
		seen[id] = true
		res, ok := r["result"].(map[string]any)
		if !ok {
			t.Fatalf("id %v: missing result: %v", id, r)
		}
		_, isModern := res["resultType"]
		if want := wantModern[int(id)]; isModern != want {
			t.Fatalf("id %v: resultType present=%v, want era-appropriate=%v: %v", id, isModern, want, res)
		}
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

// TestMissingParamsIsHandledNotPanickedModernEra replays the brief's
// missing-params case under the modern era: a tools/call whose _meta is
// present and valid (so it takes the modern branch through dispatch) but
// whose name is absent must not panic, and the connection must remain
// usable for a subsequent legitimate modern request.
func TestMissingParamsIsHandledNotPanickedModernEra(t *testing.T) {
	meta := `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}`
	got := run(t,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{`+meta+`}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ping_tool",`+meta+`}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{`+meta+`}}`)
	if len(got) != 3 {
		t.Fatalf("want 3 responses, got %d: %v", len(got), got)
	}
	if got[0]["error"] == nil {
		t.Fatalf("modern tools/call with no name must be a params error, not silently accepted: %v", got[0])
	}
	if got[2]["error"] != nil {
		t.Fatalf("server must survive malformed modern calls: %v", got[2])
	}
}

// TestUnknownMethodWithBadProtocolVersionPrefersVersionError covers a
// hostile client that names both a garbage method and an unsupported modern
// protocol version in the same request. dispatch's default-case comment
// documents that the version mismatch should win over "method not found"
// (it's the more actionable diagnostic), but until this test that branch
// was documented, not verified.
func TestUnknownMethodWithBadProtocolVersionPrefersVersionError(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":1,"method":"no/such/method","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"1900-01-01","io.modelcontextprotocol/clientCapabilities":{}}}}`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %v", got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("want an error response, got %v", got[0])
	}
	if int(e["code"].(float64)) != CodeUnsupportedProtocolVersion {
		t.Errorf("code = %v, want %d (version mismatch should win over method-not-found)",
			e["code"], CodeUnsupportedProtocolVersion)
	}
}
