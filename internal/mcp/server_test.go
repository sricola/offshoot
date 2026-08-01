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

// TestOversizedMultiFragmentLineResyncs exercises readBoundedLine's drain
// loop across many fragments, not just the one where tooLong first flips to
// true. TestOversizedLineGetsErrorAndServerContinues above only pushes the
// line maxLineSize+10 bytes over the cap; with the 64KB buffered reader this
// server uses, that overage lands entirely within bufio.Reader's very last
// fragment for the line (isPrefix already false there), so the loop's
// break-on-!isPrefix path fires on the very same iteration tooLong is set —
// the custom drain logic that justifies readBoundedLine over bufio.Scanner
// never actually iterates past that point. A line multiple megabytes over
// the cap forces many more ReadLine fragments after tooLong flips true, so
// the drain loop has to keep discarding fragments (not just skip appending)
// across several iterations before it finds the line's real end and
// resyncs. This asserts both that the oversized line still produces exactly
// one error response, and that the very next request lands as its own
// correctly answered response — not a corrupted read of leftover line
// bytes.
func TestOversizedMultiFragmentLineResyncs(t *testing.T) {
	huge := strings.Repeat("x", maxLineSize+2_000_000)
	got := run(t, huge, `{"jsonrpc":"2.0","id":42,"method":"tools/list"}`)
	if len(got) != 2 {
		t.Fatalf("want an oversized-line error then exactly one normal response, got %d: %v", len(got), got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("want an error response for the oversized line, got %v", got[0])
	}
	if int(e["code"].(float64)) != CodeInvalidRequest {
		t.Errorf("code = %v, want %d", e["code"], CodeInvalidRequest)
	}
	if got[1]["error"] != nil {
		t.Fatalf("server must resync after a multi-fragment oversized line, not corrupt the next response: %v", got[1])
	}
	if id, ok := got[1]["id"].(float64); !ok || id != 42 {
		t.Fatalf("response after the oversized line is out of sync: got id %v, want 42: %v", got[1]["id"], got[1])
	}
	res, ok := got[1]["result"].(map[string]any)
	if !ok {
		t.Fatalf("response after the oversized line has no result: %v", got[1])
	}
	if _, ok := res["tools"]; !ok {
		t.Fatalf("response after the oversized line is not a valid tools/list result: %v", res)
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

// TestMissingParamsIsHandledNotPanicked covers dispatch's tools/call param
// handling under three kinds of malformed request: no params object at all,
// a params object with no name field, and — the case that actually exercises
// the `params.Name == ""` guard in dispatch — a params object with name
// present but empty. Without that third case, removing the guard is
// invisible here: an empty name would flow straight into ts.Call, and
// fakeTools.Call happily answers any name that isn't literally
// "unknown_tool", so the call would "succeed" instead of panicking or
// erroring, and this test would stay green.
func TestMissingParamsIsHandledNotPanicked(t *testing.T) {
	got := run(t,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ping_tool"}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":""}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/list"}`)
	if len(got) != 4 {
		t.Fatalf("want 4 responses, got %d: %v", len(got), got)
	}
	e, ok := got[2]["error"].(map[string]any)
	if !ok {
		t.Fatalf("a present-but-empty tool name must be a params error, not silently accepted: %v", got[2])
	}
	if int(e["code"].(float64)) != CodeInvalidParams {
		t.Errorf("code = %v, want %d", e["code"], CodeInvalidParams)
	}
	if got[3]["error"] != nil {
		t.Fatalf("server must survive malformed calls: %v", got[3])
	}
}

// TestMissingParamsIsHandledNotPanickedModernEra replays the brief's
// missing-params case under the modern era: a tools/call whose _meta is
// present and valid (so it takes the modern branch through dispatch) but
// whose name is absent must not panic, and the connection must remain
// usable for a subsequent legitimate modern request.
//
// The "ModernEra" in this test's name has to be earned by actually checking
// something only the modern path produces — otherwise a requestEra
// regression that stops recognizing anything as modern (so every request
// here quietly falls back to the legacy branch) leaves this test green: the
// missing-name request still errors either way (dispatch's
// `params.Name == ""` check runs before the era ever branches), and a bare
// "no error" check on the ping_tool call passes on the legacy envelope too.
// So this also pins the modern envelope itself (resultType + _meta) on the
// id-2 ping_tool call and the id-3 tools/list call, the same shape
// TestModernToolsCallDispatch and TestModernClientFullFlow check elsewhere.
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

	callRes, ok := got[1]["result"].(map[string]any)
	if !ok {
		t.Fatalf("id 2: missing result: %v", got[1])
	}
	if callRes["resultType"] != "complete" {
		t.Errorf("modern tools/call result must carry resultType=complete (era not recognized as modern?): %v", callRes)
	}
	if _, ok := callRes["_meta"].(map[string]any); !ok {
		t.Errorf("modern tools/call result missing _meta (era not recognized as modern?): %v", callRes)
	}

	listRes, ok := got[2]["result"].(map[string]any)
	if !ok {
		t.Fatalf("id 3: missing result: %v", got[2])
	}
	if listRes["resultType"] != "complete" {
		t.Errorf("modern tools/list result must carry resultType=complete (era not recognized as modern?): %v", listRes)
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

// --- ping era classification ---
//
// ping used to bypass requestEra entirely and answer `{}` unconditionally,
// unlike every other verb (tools/list, tools/call, and the default case all
// classify by era first). That meant a modern ping naming an unsupported
// protocol version got silent success instead of the version error every
// other path produces. These three tests pin the fix: a legacy ping still
// gets the bare `{}` legacy result, a modern ping gets the modern envelope,
// and a modern ping with a bad version gets CodeUnsupportedProtocolVersion
// exactly like tools/list does in TestUnsupportedProtocolVersionErrors.

// TestLegacyPing pins the pre-existing behavior: a ping with no _meta block
// gets a bare `{}` result, no envelope.
func TestLegacyPing(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":1,"method":"ping"}`)
	if len(got) != 1 || got[0]["error"] != nil {
		t.Fatalf("ping: %v", got)
	}
	res, ok := got[0]["result"].(map[string]any)
	if !ok {
		t.Fatalf("result missing: %v", got[0])
	}
	if len(res) != 0 {
		t.Errorf("legacy ping result = %v, want empty object", res)
	}
}

// TestModernPing covers a ping made under the modern (2026-07-28) era: the
// result must carry resultType and serverInfo _meta, same as every other
// modern result, but — like tools/call, and unlike tools/list — no
// ttlMs/cacheScope: ping is a liveness check, not a cacheable value.
func TestModernPing(t *testing.T) {
	meta := `"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}`
	got := run(t, `{"jsonrpc":"2.0","id":1,"method":"ping","params":{`+meta+`}}`)
	if len(got) != 1 || got[0]["error"] != nil {
		t.Fatalf("ping: %v", got)
	}
	res := got[0]["result"].(map[string]any)
	if res["resultType"] != "complete" {
		t.Errorf("ping resultType = %v, want complete", res["resultType"])
	}
	if _, ok := res["ttlMs"]; ok {
		t.Errorf("ping is not cacheable; must not carry ttlMs: %v", res)
	}
	if _, ok := res["cacheScope"]; ok {
		t.Errorf("ping is not cacheable; must not carry cacheScope: %v", res)
	}
	rmeta, ok := res["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("ping result missing _meta: %v", res)
	}
	if si, ok := rmeta["io.modelcontextprotocol/serverInfo"].(map[string]any); !ok || si["name"] != "offshoot" {
		t.Errorf("serverInfo = %v", rmeta["io.modelcontextprotocol/serverInfo"])
	}
}

// TestModernPingWithUnsupportedVersionErrors is the regression test for the
// actual bug: before this fix, ping never called requestEra at all, so a
// modern ping naming a protocol version this server doesn't implement got
// silent success (`{}`) instead of the version error every other verb
// produces for the identical _meta. This must fail exactly the way
// TestUnsupportedProtocolVersionErrors does for tools/list.
func TestModernPingWithUnsupportedVersionErrors(t *testing.T) {
	got := run(t, `{"jsonrpc":"2.0","id":1,"method":"ping","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"1900-01-01","io.modelcontextprotocol/clientCapabilities":{}}}}`)
	if len(got) != 1 {
		t.Fatalf("want 1 response, got %v", got)
	}
	e, ok := got[0]["error"].(map[string]any)
	if !ok {
		t.Fatalf("ping with an unsupported protocol version must error, got a result: %v", got[0])
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
}
