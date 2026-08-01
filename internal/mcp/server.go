// Package mcp implements the Model Context Protocol over stdio so an agent
// can drive offshoot's branching directly: fork before risky work, checkpoint
// on success, roll back on failure.
//
// The transport is newline-delimited JSON-RPC 2.0. Nothing but protocol
// messages may be written to the output stream — diagnostics go to stderr,
// because a stray write corrupts the client's session.
//
// This server is dual-era (see protocol.go): it answers legacy
// (2025-11-25) clients that open with an `initialize` handshake, and modern
// (2026-07-28) clients that carry their protocol version on every request
// via params._meta, per request rather than per connection. See
// requestEra for how a given request is classified.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// maxLineSize bounds a single JSON-RPC line. Tool call arguments (e.g. large
// SQL statements or diff payloads) can exceed bufio.Scanner's 64KB default,
// so this is sized well above the largest message we expect to see in
// practice. A line over this bound does not end the session (see
// readBoundedLine): the server answers it with a JSON-RPC error, exactly as
// it does for a malformed line, and keeps serving.
const maxLineSize = 8 << 20 // 8MB

// readBufferSize is the initial size of the buffered reader Serve uses.
// It's sized to comfortably hold a typical message in one read syscall;
// readBoundedLine grows past it transparently for larger lines, up to
// maxLineSize.
const readBufferSize = 64 * 1024

// Server reads JSON-RPC messages from r and writes responses to w.
type Server struct {
	r  io.Reader
	w  io.Writer
	ts ToolSet
}

// NewServer returns a server whose tools are provided by ts.
func NewServer(r io.Reader, w io.Writer, ts ToolSet) *Server {
	return &Server{r: r, w: w, ts: ts}
}

// readResult is one line off the wire, or the terminal error/EOF that ended
// reading.
type readResult struct {
	line    []byte
	tooLong bool
	err     error // io.EOF on clean close; non-nil non-EOF on a real read error
}

// Serve reads until EOF or ctx cancellation, dispatching each line to
// dispatch and writing back whatever response (if any) it produces. It
// returns nil on clean EOF, or ctx.Err() if ctx is cancelled.
//
// Cancellation is genuine, not just checked between messages: the actual
// read happens in a background goroutine, and Serve's main loop selects on
// ctx.Done() against that goroutine's output channel. So Serve returns as
// soon as ctx is cancelled even if a read is blocked mid-line on a slow or
// silent peer. The one caveat inherent to wrapping an arbitrary io.Reader
// this way: the background goroutine itself can only unblock once the
// underlying Read call returns (data, error, or the peer closing its end),
// so it may keep running after Serve has already returned; it exits on its
// own once that happens; it is never leaked past process exit since the
// process owns the underlying descriptor.
func (s *Server) Serve(ctx context.Context) error {
	br := bufio.NewReaderSize(s.r, readBufferSize)

	results := make(chan readResult)
	go func() {
		for {
			line, tooLong, err := readBoundedLine(br, maxLineSize)
			select {
			case results <- readResult{line: line, tooLong: tooLong, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case res := <-results:
			if res.err != nil {
				if res.err == io.EOF {
					return nil
				}
				return res.err
			}

			if res.tooLong {
				if werr := s.writeResponse(Response{
					Error: &RPCError{
						Code:    CodeInvalidRequest,
						Message: fmt.Sprintf("request exceeds maximum message size of %d bytes", maxLineSize),
					},
				}); werr != nil {
					return werr
				}
				continue
			}

			if len(bytes.TrimSpace(res.line)) == 0 {
				continue
			}

			var req Request
			if err := json.Unmarshal(res.line, &req); err != nil {
				if werr := s.writeResponse(Response{
					Error: &RPCError{Code: CodeParseError, Message: fmt.Sprintf("parse error: %v", err)},
				}); werr != nil {
					return werr
				}
				continue
			}

			resp, isNotification := s.dispatch(ctx, req)
			if isNotification {
				continue
			}
			if err := s.writeResponse(resp); err != nil {
				return err
			}
		}
	}
}

// readBoundedLine reads one newline-delimited line from br, same framing as
// the JSON-RPC stdio transport requires. Unlike bufio.Scanner (whose
// Scan permanently fails once a token exceeds its max buffer, taking the
// whole session down with it), readBoundedLine keeps the reader usable: a
// line over max is fully drained from the underlying stream — so parsing
// resyncs cleanly on the next newline — and reported back via tooLong
// instead of line data.
//
// err is io.EOF on a clean close, following bufio.Reader.ReadLine's own
// contract of returning a final unterminated line (if any) before EOF, or a
// non-nil non-EOF error on any other read failure.
func readBoundedLine(br *bufio.Reader, max int) (line []byte, tooLong bool, err error) {
	var buf []byte
	for {
		fragment, isPrefix, ferr := br.ReadLine()
		if ferr != nil {
			return nil, false, ferr
		}
		if !tooLong {
			if len(buf)+len(fragment) > max {
				tooLong = true
				buf = nil
			} else {
				buf = append(buf, fragment...)
			}
		}
		if !isPrefix {
			break
		}
	}
	if tooLong {
		return nil, true, nil
	}
	return buf, false, nil
}

// dispatch handles one decoded request. It returns (response, false) for
// requests that need an answer, or (zero, true) for notifications, which
// must never be answered.
func (s *Server) dispatch(ctx context.Context, req Request) (Response, bool) {
	isNotification := len(req.ID) == 0

	reply := func(result any, rpcErr *RPCError) (Response, bool) {
		if isNotification {
			return Response{}, true
		}
		return Response{JSONRPC: "2.0", ID: req.ID, Result: result, Error: rpcErr}, false
	}

	switch req.Method {
	case "initialize":
		// initialize always selects legacy semantics, regardless of what
		// the client requested: per the 2026-07-28 spec's backward-
		// compatibility rules, a dual-era server serves an initialize
		// request under the one legacy revision it supports.
		return reply(initializeResult{
			ProtocolVersion: legacyProtocolVersion,
			Capabilities:    serverCapabilities{Tools: toolsCapability{}},
			ServerInfo:      implementationInfo{Name: "offshoot", Version: serverVersion},
		}, nil)

	case "notifications/initialized":
		// The legacy client's readiness signal. No response, regardless of
		// whether it was sent as a notification (it always is, but be
		// tolerant).
		return Response{}, true

	case "ping":
		return reply(struct{}{}, nil)

	case "server/discover":
		// server/discover is era-agnostic: it always answers with what
		// this server supports, whether or not the caller identified a
		// preferred version. A modern client uses it as the stdio
		// backward-compatibility probe (see the spec's stdio transport
		// page); this server always answers it the same way.
		return reply(discoverResult{
			ResultType:        "complete",
			SupportedVersions: []string{modernProtocolVersion, legacyProtocolVersion},
			Capabilities:      serverCapabilities{Tools: toolsCapability{}},
			TTLMs:             toolsListTTLMs,
			CacheScope:        "public",
			Meta:              newResponseMeta(),
			Instructions:      "offshoot MCP server: fork before risky work, checkpoint on success, roll back on failure.",
		}, nil)

	case "tools/list":
		meta, rpcErr := requestEra(req.Params)
		if rpcErr != nil {
			return reply(nil, rpcErr)
		}
		if meta != nil {
			return reply(modernToolsListResult{
				ResultType: "complete",
				Tools:      s.ts.Tools(),
				TTLMs:      toolsListTTLMs,
				CacheScope: "public",
				Meta:       newResponseMeta(),
			}, nil)
		}
		return reply(toolsListResult{Tools: s.ts.Tools()}, nil)

	case "tools/call":
		meta, rpcErr := requestEra(req.Params)
		if rpcErr != nil {
			return reply(nil, rpcErr)
		}
		var params toolCallParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return reply(nil, &RPCError{
				Code:    CodeInvalidParams,
				Message: fmt.Sprintf("invalid params: %v", err),
			})
		}
		if params.Name == "" {
			return reply(nil, &RPCError{
				Code:    CodeInvalidParams,
				Message: "missing required param: name",
			})
		}
		result, err := s.ts.Call(ctx, params.Name, params.Arguments)
		if err != nil {
			// A non-nil error from Call is reserved for "this is not a real
			// tool" (or another protocol-level fault) — never a user-level
			// operational failure, which comes back as an ErrorResult
			// instead and is handled below like any other tool result.
			return reply(nil, &RPCError{
				Code:    CodeInvalidParams,
				Message: err.Error(),
			})
		}
		if meta != nil {
			return reply(modernToolCallResult{
				ResultType: "complete",
				Content:    result.Content,
				IsError:    result.IsError,
				Meta:       newResponseMeta(),
			}, nil)
		}
		return reply(result, nil)

	default:
		// Still classify unrecognized methods by era: a modern request
		// naming an unsupported protocol version gets
		// UnsupportedProtocolVersionError even if the method itself is
		// also unknown, since the version mismatch is the more actionable
		// diagnostic.
		if _, rpcErr := requestEra(req.Params); rpcErr != nil {
			return reply(nil, rpcErr)
		}
		return reply(nil, &RPCError{
			Code:    CodeMethodNotFound,
			Message: fmt.Sprintf("method not found: %s", req.Method),
		})
	}
}

// requestEra classifies a request by the presence of a modern
// params._meta block, per
// https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning:
// "A request carrying modern per-request _meta is served statelessly
// according to this revision." A request with no such block (every legacy
// request, including all six of this package's original tests) is legacy.
//
// It returns (non-nil meta, nil) for a valid modern request, (nil, nil) for
// a legacy request, and (nil, rpcErr) for a request that identifies itself
// as modern but is malformed (wrong or missing protocol version, or missing
// the required clientCapabilities field).
func requestEra(params json.RawMessage) (*requestMeta, *RPCError) {
	if len(params) == 0 {
		return nil, nil
	}

	var envelope requestParamsMeta
	if err := json.Unmarshal(params, &envelope); err != nil {
		// Params don't even parse as an object; that's not this function's
		// problem to report — the method's own param handling (or, for
		// methods with none, silent ignoring) surfaces it. Treat as
		// legacy/no-meta so dispatch proceeds down the legacy path.
		return nil, nil
	}
	meta := envelope.Meta
	if meta == nil || meta.ProtocolVersion == "" {
		return nil, nil
	}

	if meta.ProtocolVersion != modernProtocolVersion {
		return nil, &RPCError{
			Code:    CodeUnsupportedProtocolVersion,
			Message: "unsupported protocol version",
			Data: unsupportedVersionData{
				Supported: []string{modernProtocolVersion, legacyProtocolVersion},
				Requested: meta.ProtocolVersion,
			},
		}
	}
	if meta.ClientCapabilities == nil {
		return nil, &RPCError{
			Code:    CodeInvalidParams,
			Message: fmt.Sprintf("missing required _meta field: %s", metaKeyClientCapabilities),
		}
	}
	return meta, nil
}

func (s *Server) writeResponse(resp Response) error {
	resp.JSONRPC = "2.0"
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("mcp: marshal response: %w", err)
	}
	data = append(data, '\n')
	_, err = s.w.Write(data)
	return err
}
