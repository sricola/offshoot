package mcp

import (
	"context"
	"encoding/json"
	"fmt"
)

// This server is dual-era: it speaks both the legacy, handshake-based
// 2025-11-25 revision and the modern, stateless 2026-07-28 revision (final
// as of 2026-07-28; see https://blog.modelcontextprotocol.io/posts/2026-07-28/
// and https://modelcontextprotocol.io/specification/2026-07-28/changelog).
// Neither is "current" in the sense of being the only supported revision —
// that is the point of dual-era support. See server.go's requestEra for how
// a given request is routed to one era or the other.
//
// legacyProtocolVersion is the revision initialize always answers with: a
// legacy client that supports only one prior revision has no fall-forward
// mechanism, so per the 2026-07-28 spec's backward-compatibility guidance a
// server names the revision it supports regardless of what the client
// requested in initialize's params.
const legacyProtocolVersion = "2025-11-25"

// modernProtocolVersion is the one 2026-07-28-family revision this server
// speaks on the stateless, per-request path (server/discover, and any
// request whose params carry a modern `_meta` block).
const modernProtocolVersion = "2026-07-28"

// serverVersion is this package's own implementation version, reported in
// initialize's serverInfo.version. The offshoot binary does not otherwise
// track a version string.
const serverVersion = "0.1.0"

// Request and Response are JSON-RPC 2.0 messages, one per line.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"` // absent for notifications
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	JSONRPC string `json:"jsonrpc"`
	// ID deliberately has no `omitempty`: json.RawMessage is a []byte slice,
	// and omitempty on a slice drops the field whenever it is nil/empty —
	// which is exactly the parse-error case where we could not read a
	// request id. JSON-RPC 2.0 requires "id":null in that situation, not an
	// absent id key. json.RawMessage's own MarshalJSON already renders a nil
	// RawMessage as the literal `null`, so leaving omitempty off is
	// sufficient: a real id round-trips unchanged, and a nil id serializes
	// as `null` instead of disappearing.
	ID     json.RawMessage `json:"id"`
	Result any             `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Standard JSON-RPC error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// CodeUnsupportedProtocolVersion is MCP-specific, not standard JSON-RPC: it
// lives in the sub-range -32020..-32099 that the 2026-07-28 spec reserves
// for the MCP specification itself (see
// https://modelcontextprotocol.io/specification/2026-07-28/basic#error-codes
// and https://modelcontextprotocol.io/specification/2026-07-28/schema#unsupportedprotocolversionerror).
// A server emits it when a request's `_meta` names a protocol version the
// server does not implement.
const CodeUnsupportedProtocolVersion = -32022

// Tool describes one callable tool, as returned from tools/list.
type Tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema any    `json:"inputSchema"`
}

// ToolSet is what the server exposes; Task 2 implements it over offshoot.
type ToolSet interface {
	Tools() []Tool
	Call(ctx context.Context, name string, args json.RawMessage) (ToolResult, error)
}

// ToolResult is an MCP tool response: content blocks plus an error flag.
type ToolResult struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError,omitempty"`
}

type Content struct {
	Type string `json:"type"` // "text"
	Text string `json:"text"`
}

// TextResult is a convenience for a single text block.
func TextResult(format string, args ...any) ToolResult {
	return ToolResult{Content: []Content{{Type: "text", Text: fmt.Sprintf(format, args...)}}}
}

// ErrorResult is a tool-level failure (IsError set), distinct from an RPC error.
func ErrorResult(format string, args ...any) ToolResult {
	return ToolResult{Content: []Content{{Type: "text", Text: fmt.Sprintf(format, args...)}}, IsError: true}
}

// implementationInfo identifies either end of the connection during
// initialize, per the MCP "Implementation" schema type (clientInfo /
// serverInfo).
type implementationInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// initializeResult is the result of an initialize request. Fields per
// https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle.
type initializeResult struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    serverCapabilities `json:"capabilities"`
	ServerInfo      implementationInfo `json:"serverInfo"`
}

// serverCapabilities declares which optional protocol features this server
// supports. Only the tools capability is relevant until Task 2 adds
// tools/call; listChanged is omitted (false) because this server's tool
// list is fixed for the lifetime of a connection.
type serverCapabilities struct {
	Tools toolsCapability `json:"tools"`
}

type toolsCapability struct{}

// toolsListResult is the result of a legacy (2025-11-25) tools/list request.
type toolsListResult struct {
	Tools []Tool `json:"tools"`
}

// --- 2026-07-28 (modern, stateless) protocol types ---
//
// The modern revision drops the initialize handshake: every request carries
// its own protocol version, capabilities, and (optionally) identity in
// params._meta, under keys reserved by the spec
// (https://modelcontextprotocol.io/specification/2026-07-28/basic#meta).
// Results, in turn, carry a required `resultType` and MAY carry a
// `_meta.io.modelcontextprotocol/serverInfo`. See server.go's requestEra for
// how an incoming request is classified as modern vs. legacy.

// Reserved `_meta` keys used for per-request protocol negotiation and
// per-response server identification in the 2026-07-28 revision.
const (
	metaKeyProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	metaKeyClientInfo         = "io.modelcontextprotocol/clientInfo"
	metaKeyClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
	metaKeyServerInfo         = "io.modelcontextprotocol/serverInfo"
)

// requestMeta is params._meta on a modern request. ProtocolVersion and
// ClientCapabilities are required by spec; a request missing either is
// malformed. ClientCapabilities is left as json.RawMessage (rather than a
// typed ClientCapabilities struct) because this server only needs to know
// the field was present, not interpret it — the tools capability the
// client offers is not currently gated on anything.
type requestMeta struct {
	ProtocolVersion    string              `json:"io.modelcontextprotocol/protocolVersion"`
	ClientInfo         *implementationInfo `json:"io.modelcontextprotocol/clientInfo,omitempty"`
	ClientCapabilities json.RawMessage     `json:"io.modelcontextprotocol/clientCapabilities"`
}

// requestParamsMeta unwraps the `_meta` envelope from a request's params
// without needing to know the rest of that method's param shape.
type requestParamsMeta struct {
	Meta *requestMeta `json:"_meta"`
}

// responseMeta is the `_meta` object modern results attach to identify this
// server, per the spec's "servers SHOULD include serverInfo in every
// result's _meta" guidance.
type responseMeta struct {
	ServerInfo *implementationInfo `json:"io.modelcontextprotocol/serverInfo,omitempty"`
}

func newResponseMeta() responseMeta {
	return responseMeta{ServerInfo: &implementationInfo{Name: "offshoot", Version: serverVersion}}
}

// unsupportedVersionData is the `error.data` payload of a modern
// UnsupportedProtocolVersionError, per
// https://modelcontextprotocol.io/specification/2026-07-28/schema#unsupportedprotocolversionerror.
type unsupportedVersionData struct {
	Supported []string `json:"supported"`
	Requested string   `json:"requested"`
}

// discoverResult is the result of a server/discover request. Per
// https://modelcontextprotocol.io/specification/2026-07-28/server/discover,
// servers MUST implement server/discover and it is era-agnostic: it always
// answers with what this server supports, regardless of what (if anything)
// the caller's own _meta requested. When resultType is "complete", the
// 2026-07-28 spec's caching page requires ttlMs and cacheScope.
type discoverResult struct {
	ResultType        string             `json:"resultType"`
	SupportedVersions []string           `json:"supportedVersions"`
	Capabilities      serverCapabilities `json:"capabilities"`
	TTLMs             int64              `json:"ttlMs"`
	CacheScope        string             `json:"cacheScope"`
	Meta              responseMeta       `json:"_meta"`
	Instructions      string             `json:"instructions,omitempty"`
}

// modernToolsListResult is the result of a tools/list request made under
// the modern revision: same tool data as the legacy toolsListResult, but
// wrapped in the modern envelope (resultType, and the ttlMs/cacheScope pair
// the 2026-07-28 CacheableResult interface requires on tools/list results).
// This server's tool list never varies per caller, so cacheScope is always
// "public".
type modernToolsListResult struct {
	ResultType string       `json:"resultType"`
	Tools      []Tool       `json:"tools"`
	TTLMs      int64        `json:"ttlMs"`
	CacheScope string       `json:"cacheScope"`
	Meta       responseMeta `json:"_meta"`
}

// toolsListTTLMs is how long a client may cache a tools/list response
// before re-fetching. This server's tool list is fixed for the process
// lifetime, so this is a generous, arbitrary freshness hint rather than a
// measured value.
const toolsListTTLMs = 5 * 60 * 1000

// toolCallParams is tools/call's params, same on both eras (per Task 1's
// verified spec reading: "tools/call's params/result shapes are otherwise
// unchanged from 2025-11-25 (arguments, content, isError)"). A modern
// request additionally carries `_meta`, unwrapped separately by requestEra
// via requestParamsMeta — Params here only needs the tool-call-specific
// fields.
type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// modernToolCallResult is the result of a tools/call request made under the
// modern revision: same content/isError data as the legacy ToolResult, but
// wrapped in the modern envelope. Unlike modernToolsListResult, this does
// NOT carry ttlMs/cacheScope: a tool call is an action with side effects,
// not a value the 2026-07-28 CacheableResult interface applies to — that
// interface (and its required caching fields) is opt-in per result type,
// and tools/call's result never opted in. resultType alone is what every
// modern result carries.
type modernToolCallResult struct {
	ResultType string       `json:"resultType"`
	Content    []Content    `json:"content"`
	IsError    bool         `json:"isError,omitempty"`
	Meta       responseMeta `json:"_meta"`
}

// modernPingResult is the result of a ping request made under the modern
// revision: just the resultType/_meta envelope. Like tools/call and unlike
// tools/list and server/discover, ping is not in the 2026-07-28 spec's
// CacheableResult interface's list of cacheable operations — a ping answer
// has no value to cache, it's a liveness check — so this does NOT carry
// ttlMs/cacheScope.
type modernPingResult struct {
	ResultType string       `json:"resultType"`
	Meta       responseMeta `json:"_meta"`
}
