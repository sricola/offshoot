// Package mcp implements the Model Context Protocol over stdio so an agent
// can drive offshoot's branching directly: fork before risky work, checkpoint
// on success, roll back on failure.
//
// The transport is newline-delimited JSON-RPC 2.0. Nothing but protocol
// messages may be written to the output stream — diagnostics go to stderr,
// because a stray write corrupts the client's session.
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
// practice.
const maxLineSize = 8 << 20 // 8MB

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

// Serve reads until EOF or ctx cancellation. It returns nil on clean EOF.
func (s *Server) Serve(ctx context.Context) error {
	scanner := bufio.NewScanner(s.r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}

		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			if werr := s.writeResponse(Response{
				JSONRPC: "2.0",
				Error:   &RPCError{Code: CodeParseError, Message: fmt.Sprintf("parse error: %v", err)},
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
	return scanner.Err()
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
		return reply(initializeResult{
			ProtocolVersion: protocolVersion,
			Capabilities:    serverCapabilities{Tools: toolsCapability{}},
			ServerInfo:      implementationInfo{Name: "offshoot", Version: serverVersion},
		}, nil)

	case "notifications/initialized":
		// The client's readiness signal. No response, regardless of whether
		// it was sent as a notification (it always is, but be tolerant).
		return Response{}, true

	case "ping":
		return reply(struct{}{}, nil)

	case "tools/list":
		return reply(toolsListResult{Tools: s.ts.Tools()}, nil)

	default:
		return reply(nil, &RPCError{
			Code:    CodeMethodNotFound,
			Message: fmt.Sprintf("method not found: %s", req.Method),
		})
	}
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
