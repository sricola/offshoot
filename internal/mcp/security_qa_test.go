package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

// documentedMCPTools is the exact set reference.md "#offshoot-mcp" lists:
// "ten tools (offshoot_list, offshoot_checkout, offshoot_checkpoint,
// offshoot_fork, offshoot_rollback, offshoot_promote, offshoot_destroy,
// offshoot_prune_checkpoints, offshoot_touch, offshoot_diff)".
var documentedMCPTools = []string{
	"offshoot_checkout",
	"offshoot_checkpoint",
	"offshoot_destroy",
	"offshoot_diff",
	"offshoot_fork",
	"offshoot_list",
	"offshoot_promote",
	"offshoot_prune_checkpoints",
	"offshoot_rollback",
	"offshoot_touch",
}

// TestMCPToolsListIsExactlyTheDocumentedNoProtect pins reference.md
// "#offshoot-mcp": the server advertises exactly the ten documented tools
// and "There is no offshoot_protect/offshoot_unprotect tool; flipping a
// branch's protected flag is CLI-only by design". Both the ToolSet's Tools()
// and the wire-level tools/list answer of a real Server built on it are
// compared against the pinned set, so adding (or dropping) a tool fails
// here, and offshoot_protect/offshoot_unprotect are also refused as
// unknown tools by Call.
func TestMCPToolsListIsExactlyTheNineDocumentedNoProtect(t *testing.T) {
	ts, _ := newTools(t)

	var names []string
	for _, tool := range ts.Tools() {
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	if got, want := strings.Join(names, ","), strings.Join(documentedMCPTools, ","); got != want {
		t.Fatalf("Tools() = %s\nwant exactly %s", got, want)
	}

	// The same set reaches the wire through a real Server.
	in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}` + "\n")
	var out bytes.Buffer
	if err := NewServer(in, &out, ts).Serve(context.Background()); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	var resp struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &resp); err != nil {
		t.Fatalf("tools/list response %q: %v", out.String(), err)
	}
	if resp.Error != nil {
		t.Fatalf("tools/list error: %v", resp.Error)
	}
	var wire []string
	for _, tool := range resp.Result.Tools {
		wire = append(wire, tool.Name)
	}
	sort.Strings(wire)
	if got, want := strings.Join(wire, ","), strings.Join(documentedMCPTools, ","); got != want {
		t.Fatalf("tools/list on the wire = %s\nwant exactly %s", got, want)
	}

	for _, name := range []string{"offshoot_protect", "offshoot_unprotect"} {
		_, err := ts.Call(context.Background(), name, json.RawMessage(`{"database":"app","branch":"main"}`))
		if err == nil || !strings.Contains(err.Error(), "unknown tool") {
			t.Fatalf("Call(%s) = %v, want an unknown-tool error", name, err)
		}
	}
}
