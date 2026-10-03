package main

import (
	"strings"
	"testing"

	"github.com/sricola/offshoot/internal/daemon"
)

// TestSessionStatusLineShowsState: session status says whether each listed
// session is still open or already closing, since a closing one stays listed
// until its close has released the lease. An older daemon sends no state,
// which means open.
func TestSessionStatusLineShowsState(t *testing.T) {
	in := daemon.SessionInfo{DB: "app", Branch: "main", Holder: "h/1", Epoch: 2, State: daemon.SessionStateClosing}
	if line := sessionStatusLine(in); !strings.HasPrefix(line, "app@main state=closing ") {
		t.Fatalf("line = %q, want it to start with app@main state=closing", line)
	}
	in.State = "" // an older daemon sends no state
	if line := sessionStatusLine(in); !strings.HasPrefix(line, "app@main state=open ") {
		t.Fatalf("line = %q, want an absent state shown as open", line)
	}
}
