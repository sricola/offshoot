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
	// "touch" | "branches" | "dbs" | "export" | "checkout-at".
	Op     string `json:"op"`
	DB     string `json:"db,omitempty"`
	Branch string `json:"branch,omitempty"` // also: fork/promote source branch
	// Name is overloaded by op: checkpoint name (flush, rollback), new-branch
	// name (fork), promote target branch, or the checkpoint to materialize
	// (export — "" means the branch's head; checkout-at — required, no head
	// alias).
	Name string `json:"name,omitempty"`
	// From is fork's source checkpoint name ("" = source branch's head).
	From string `json:"from,omitempty"`
	// TTL is a Go duration string. For fork/touch: "" means no change (fork:
	// no TTL; touch: keep the current TTL); touch additionally accepts
	// "none" to clear the TTL.
	TTL string `json:"ttl,omitempty"`
	// Force overrides protected-branch/live-lease refusals for destroy and
	// promote (passed through to ops.Destroy/ops.Promote); for export and
	// checkout-at it instead overrides their own overwrite/cache-hit
	// refusals (ops.Workspace.Export/CheckoutAt).
	Force bool `json:"force,omitempty"`
	// Path is export's destination file path — server-side, on the daemon's
	// own host/filesystem. Trust model: the daemon speaks only a local unix
	// socket, reachable only by a process that can already open that socket
	// file (same host, same user); Path is trusted as an ordinary
	// filesystem path this process can write, with exactly one guard
	// enforced at the RPC boundary (opExport): it must be ABSOLUTE. A
	// relative path is refused rather than resolved against the daemon's
	// own working directory, which the client cannot see or control.
	Path string `json:"path,omitempty"`
	// Meta is a small string->string map (capped by ops.ValidateMeta; see
	// its doc comment for the exact limits), used by "fork" (stored on the
	// new branch's Ref.Meta) and "flush" WHEN Name is also set (stored on
	// that named checkpoint's own Meta — see opFlush's doc comment for why
	// flush, not a separate "checkpoint" op, is where a daemon session's
	// named checkpoints are created). Omitted/absent (the old-client shape)
	// means no metadata, exactly like nil — this field is purely additive,
	// so a pre-Milestone-3 client's requests decode and behave identically
	// to before.
	Meta map[string]string `json:"meta,omitempty"`
}

// Response is the daemon's reply to a single Request.
type Response struct {
	OK       bool          `json:"ok"`
	Error    string        `json:"error,omitempty"`
	Checkout string        `json:"checkout,omitempty"`
	TXID     uint64        `json:"txid,omitempty"`
	Sessions []SessionInfo `json:"sessions,omitempty"`
	Branches []BranchInfo  `json:"branches,omitempty"`
	// Databases is every database this store has at least one ref for
	// (store.Store.ListRefs's keys, sorted), as returned by the "dbs" op.
	Databases []string `json:"databases,omitempty"`
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
	// TouchedAt is the ref's activity-clock stamp (store.Ref.TouchedAt),
	// RFC3339 UTC. Omitted for a ref that predates TTL tracking and has
	// never been touched since.
	TouchedAt string `json:"touched_at,omitempty"`
	// CheckpointsV2 carries the same checkpoints as Checkpoints (sorted by
	// name, identically), plus each one's txid and creation timestamp.
	// Added alongside Checkpoints — which stays exactly as it was — rather
	// than replacing it, so an old client reading only Checkpoints keeps
	// working unchanged (wire compat; Milestone 3 Task 1).
	CheckpointsV2 []CheckpointInfo `json:"checkpoints_v2,omitempty"`
}

// CheckpointInfo is one checkpoint's wire shape within BranchInfo.CheckpointsV2.
type CheckpointInfo struct {
	Name string `json:"name"`
	TXID uint64 `json:"txid"`
	// CreatedAt is RFC3339 UTC, omitted for a checkpoint that predates
	// per-checkpoint timestamps (see store.Checkpoint.CreatedAt).
	CreatedAt string `json:"created_at,omitempty"`
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
