package mcp

import (
	"context"
	"encoding/json"
	"fmt"
)

// protocolVersion is the MCP protocol revision this server implements, per
// https://modelcontextprotocol.io/specification/2025-11-25 (the current
// stable release as of writing; a 2026-07-28 revision existed only as a
// release candidate). initialize always answers with this version: per the
// spec's version-negotiation rules, a server that supports only one
// revision responds with that revision regardless of what the client
// requested.
const protocolVersion = "2025-11-25"

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
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
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

// toolsListResult is the result of a tools/list request.
type toolsListResult struct {
	Tools []Tool `json:"tools"`
}
