// Package daemon defines the wire protocol used to talk to a running
// offshoot daemon over a unix socket.
//
// The protocol is newline-delimited JSON: each Request is written as a
// single JSON value (json.Encoder terminates it with a trailing newline),
// and the daemon writes back exactly one Response per Request, also as a
// single JSON value terminated by a newline. A single connection may be
// used to send many requests in sequence, each answered before the next is
// read. There is no pipelining — the client must wait for a Response
// before sending the next Request on the same connection.
package daemon

// Request is one client request sent to the daemon.
type Request struct {
	// Op is one of: "open" | "flush" | "status" | "close" | "shutdown" |
	// "create" | "checkout" | "fork" | "destroy" | "rollback" | "promote" |
	// "touch" | "branches".
	Op     string `json:"op"`
	DB     string `json:"db,omitempty"`
	Branch string `json:"branch,omitempty"` // also: fork/promote source branch
	// Name is overloaded by op: checkpoint name (flush, rollback),
	// new-branch name (fork), or promote target branch.
	Name string `json:"name,omitempty"`
	// From is fork's source checkpoint name ("" = source branch's head).
	From string `json:"from,omitempty"`
	// TTL is a Go duration string. For fork/touch: "" means no change (fork:
	// no TTL; touch: keep the current TTL); touch additionally accepts
	// "none" to clear the TTL.
	TTL string `json:"ttl,omitempty"`
	// Force overrides protected-branch/live-lease refusals for destroy and
	// promote (passed through to ops.Destroy/ops.Promote).
	Force bool `json:"force,omitempty"`
}

// Response is the daemon's reply to a single Request.
type Response struct {
	OK       bool          `json:"ok"`
	Error    string        `json:"error,omitempty"`
	Checkout string        `json:"checkout,omitempty"`
	TXID     uint64        `json:"txid,omitempty"`
	Sessions []SessionInfo `json:"sessions,omitempty"`
	Branches []BranchInfo  `json:"branches,omitempty"`
}

// BranchInfo describes one branch of one db, as returned by the "branches"
// op.
type BranchInfo struct {
	Branch    string `json:"branch"`
	HeadTXID  uint64 `json:"head_txid"`
	Protected bool   `json:"protected"`
	// TTL is the branch's TTL verbatim from the ref: time.Duration's
	// canonical String() re-render (e.g. a fork requested with ttl "1h"
	// reads back here as "1h0m0s"), not necessarily the literal string a
	// client last sent. Safe to echo straight back as a future fork/touch
	// request's TTL — it always round-trips through time.ParseDuration.
	TTL          string   `json:"ttl,omitempty"`
	TTLRemaining string   `json:"ttl_remaining,omitempty"`
	LeaseHolder  string   `json:"lease_holder,omitempty"`
	Checkpoints  []string `json:"checkpoints,omitempty"` // sorted names
}

// SessionInfo describes one session open in the daemon, as returned by the
// "status" op.
type SessionInfo struct {
	DB          string `json:"db"`
	Branch      string `json:"branch"`
	Checkout    string `json:"checkout"`
	Holder      string `json:"holder"`
	Epoch       uint64 `json:"epoch"`
	DurableTXID uint64 `json:"durable_txid"`
	// DurableAge is how long it has been since the most recent successful
	// flush (manual or automatic), rendered via time.Duration.String() at
	// the moment "status" was answered — "" if this session has never
	// flushed successfully. Answers "how stale is durable_txid, right now."
	DurableAge string `json:"durable_age,omitempty"`
	// LastFlushAt is the RFC3339 timestamp of the most recent successful
	// flush (manual or automatic), or "" if none has ever succeeded.
	LastFlushAt string `json:"last_flush_at,omitempty"`
	// FlushError is the most recent AUTOMATIC-flush failure (see
	// session.Session.LastFlushErr) — "" once a later flush, manual or
	// automatic, has succeeded, or if auto-flush was never enabled
	// (Options.FlushEvery == 0) or has never failed. A manual "flush" op's
	// own failure is returned directly as this Response's Error, not here.
	FlushError string `json:"flush_error,omitempty"`
	// CaptureLag is capture.Engine.Lag(): WAL bytes committed by writers but
	// not yet applied to this session's replica. Always present (no
	// omitempty) — 0 is itself meaningful ("fully caught up"), not "absent".
	CaptureLag int64  `json:"capture_lag_bytes"`
	Error      string `json:"error,omitempty"`
}
