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
