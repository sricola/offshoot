package main

import (
	"bufio"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/testutil"
)

// TestMCPSubprocessHandshakeAndCall builds nothing: it runs `go run .` so the
// test exercises the same entry point a client would spawn. It drives both
// protocol eras the server supports against the real binary: the legacy
// (2025-11-25) initialize handshake, and the modern (2026-07-28) stateless
// per-request path via server/discover and a _meta-bearing tools/call.
func TestMCPSubprocessHandshakeAndCall(t *testing.T) {
	testutil.RequireSQLite3(t)
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

	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)

	send := func(line string) map[string]any {
		t.Helper()
		if _, err := stdin.Write([]byte(line + "\n")); err != nil {
			t.Fatalf("write: %v (stderr: %s)", err, stderr.String())
		}
		if !sc.Scan() {
			t.Fatalf("no response (stderr: %s)", stderr.String())
		}
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad response %q: %v (stderr: %s)", sc.Text(), err, stderr.String())
		}
		return m
	}

	// --- Legacy (2025-11-25) handshake-based flow ---

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

	// --- Modern (2026-07-28) stateless, per-request flow ---
	//
	// The modern era carries no connection state from the legacy handshake
	// above: each request identifies its own protocol version and
	// capabilities via params._meta. server/discover is the transport-level
	// probe a modern client uses before ever sending a versioned request.

	discover := send(`{"jsonrpc":"2.0","id":4,"method":"server/discover"}`)
	if discover["error"] != nil {
		t.Fatalf("server/discover: %v", discover["error"])
	}
	dres := discover["result"].(map[string]any)
	if dres["resultType"] != "complete" {
		t.Fatalf("server/discover resultType = %v", dres["resultType"])
	}
	supported := dres["supportedVersions"].([]any)
	found2026 := false
	for _, v := range supported {
		if v == "2026-07-28" {
			found2026 = true
		}
	}
	if !found2026 {
		t.Fatalf("server/discover should advertise 2026-07-28: %v", supported)
	}

	modernCall := send(`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"offshoot_list","arguments":{},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`)
	if modernCall["error"] != nil {
		t.Fatalf("modern tools/call: %v", modernCall["error"])
	}
	mres := modernCall["result"].(map[string]any)
	if mres["resultType"] != "complete" {
		t.Fatalf("modern tools/call resultType = %v", mres["resultType"])
	}
	mcontent := mres["content"].([]any)
	if len(mcontent) == 0 {
		t.Fatalf("empty modern tool result: %v", mres)
	}
	if !strings.Contains(mcontent[0].(map[string]any)["text"].(string), "app") {
		t.Fatalf("modern offshoot_list should mention the database: %v", mcontent)
	}
	meta, ok := mres["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("modern tools/call result missing _meta: %v", mres)
	}
	if meta["io.modelcontextprotocol/serverInfo"] == nil {
		t.Fatalf("modern tools/call _meta missing serverInfo: %v", meta)
	}
}
