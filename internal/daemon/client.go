package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultSocketPath returns the conventional socket path for a store spec.
func DefaultSocketPath(spec string) (string, error) {
	if p := os.Getenv("OFFSHOOT_SOCKET"); p != "" {
		return p, nil
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("daemon: no cache dir for the socket (set OFFSHOOT_SOCKET): %w", err)
	}
	sum := sha256.Sum256([]byte(spec))
	dir := filepath.Join(cache, "offshoot")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return filepath.Join(dir, hex.EncodeToString(sum[:8])+".sock"), nil
}

// Running reports whether a daemon is accepting connections at socketPath.
func Running(socketPath string) bool {
	c, err := net.DialTimeout("unix", socketPath, 500*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// Call sends one request to the daemon and returns its response.
func Call(socketPath string, req Request) (Response, error) {
	c, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		return Response{}, fmt.Errorf(
			"daemon: no daemon is running at %s; start one with `offshoot serve`: %w",
			socketPath, err)
	}
	defer c.Close()
	if err := json.NewEncoder(c).Encode(req); err != nil {
		return Response{}, fmt.Errorf("daemon: encoding request: %w", err)
	}
	var resp Response
	if err := json.NewDecoder(c).Decode(&resp); err != nil {
		return Response{}, fmt.Errorf("daemon: reading response: %w", err)
	}
	if !resp.OK && resp.Error != "" {
		// The server's error is already prefixed with "daemon: " (see
		// errResp in server.go); strip that before adding our own so a
		// refusal reads as "daemon: <message>", not "daemon: daemon: ...".
		// Errors that bubble up from other packages (e.g. "session: ...")
		// keep their own prefix, still distinguishable from a dial or
		// decode failure above.
		return resp, fmt.Errorf("daemon: %s", strings.TrimPrefix(resp.Error, "daemon: "))
	}
	return resp, nil
}
