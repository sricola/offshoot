package daemon

import "testing"

// branchInfoByName finds one branch's BranchInfo in a "branches" response,
// failing the test if it's absent.
func branchInfoByName(t *testing.T, resp Response, name string) BranchInfo {
	t.Helper()
	if !resp.OK {
		t.Fatalf("branches = %+v", resp)
	}
	for _, b := range resp.Branches {
		if b.Branch == name {
			return b
		}
	}
	t.Fatalf("branches missing %q: %+v", name, resp.Branches)
	return BranchInfo{}
}

// TestBranchesReportSharedVsMaterialized covers the copy-on-write cost
// reporting (Task 7): BranchInfo.Shared must be true for a base-pointer
// (shared) fork and false for a materialized, self-contained branch — both
// the root branch (never shared) and a formerly-shared fork after `compact`
// cuts its cord. This is the wire-level guarantee behind the honest
// two-cost-model story: fork shares, compact (like promote/rollback)
// materializes.
func TestBranchesReportSharedVsMaterialized(t *testing.T) {
	srv, _ := newServer(t)
	sock := srv.SocketPath()

	// newServer pre-creates db "app" (a self-contained root at txid 1).
	// A fresh db's chain is one snapshot — far under the fork-time floor —
	// so this fork SHARES: it writes a base pointer, no snapshot of its own.
	if r := call(t, sock, Request{Op: "fork", DB: "app", Branch: "main", Name: "kid"}); !r.OK {
		t.Fatalf("fork = %+v", r)
	}

	br := call(t, sock, Request{Op: "branches", DB: "app"})
	if got := branchInfoByName(t, br, "main"); got.Shared {
		t.Fatalf("main must report Shared=false (self-contained root), got %+v", got)
	}
	if got := branchInfoByName(t, br, "kid"); !got.Shared {
		t.Fatalf("shared fork must report Shared=true, got %+v", got)
	}

	// Compact materializes kid into its own self-contained lineage and
	// clears the base pointer — the report must flip to materialized.
	if r := call(t, sock, Request{Op: "compact", DB: "app", Branch: "kid"}); !r.OK {
		t.Fatalf("compact = %+v", r)
	}
	br = call(t, sock, Request{Op: "branches", DB: "app"})
	if got := branchInfoByName(t, br, "kid"); got.Shared {
		t.Fatalf("compacted branch must report Shared=false, got %+v", got)
	}
}
