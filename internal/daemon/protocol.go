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
	Op     string `json:"op"` // "open" | "flush" | "status" | "close" | "shutdown"
	DB     string `json:"db,omitempty"`
	Branch string `json:"branch,omitempty"`
	Name   string `json:"name,omitempty"` // checkpoint name for flush
}

// Response is the daemon's reply to a single Request.
type Response struct {
	OK       bool          `json:"ok"`
	Error    string        `json:"error,omitempty"`
	Checkout string        `json:"checkout,omitempty"`
	TXID     uint64        `json:"txid,omitempty"`
	Sessions []SessionInfo `json:"sessions,omitempty"`
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
	Error       string `json:"error,omitempty"`
}
