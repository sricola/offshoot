package daemon

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/sricola/offshoot/internal/ops"
	"github.com/sricola/offshoot/internal/store"
)

// FuzzDecodeRequest drives decodeRequest — the wire decode both the unix
// socket loop and POST /rpc run on untrusted bytes — over arbitrary input
// read as one connection's stream, exactly as Server.handle reads it:
// request after request until the first decode error. The property: never
// panic or loop forever; every request decoded is a fixed point of the
// wire format (re-encoded the way Call encodes it and decoded again, it
// re-encodes to the same bytes), and the pure validators dispatch applies
// to its fields accept or reject it without panicking. dispatch itself is
// not called: it acts on a live store.
func FuzzDecodeRequest(f *testing.F) {
	for _, req := range []Request{
		{Op: "status"},
		{Op: "open", DB: "app", Branch: "main"},
		{Op: "flush", DB: "app", Branch: "main", Name: "cp1", Meta: map[string]string{"k": "v"}},
		{Op: "fork", DB: "app", Branch: "main", Name: "exp", From: "cp1", TTL: "1h", Meta: map[string]string{"run": "42"}},
		{Op: "promote", DB: "app", Branch: "exp", Name: "main", Force: true, NoBackup: true, BackupTTL: "24h", Materialize: true},
		{Op: "create", DB: "app", Path: "/tmp/src.sqlite"},
		{Op: "diff", Left: "app@main", Right: "app@exp@cp1", Table: "t", Full: true, MaxBytes: 1 << 20},
		{Op: "touch", DB: "app", Branch: "exp", TTL: "none"},
		{Op: "subscribe"},
	} {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(req); err != nil {
			f.Fatal(err)
		}
		f.Add(buf.Bytes())
	}
	f.Add([]byte(`{"op":"status"}` + "\n" + `{"op":"flush","db":"a","branch":"b"}` + "\n"))
	f.Add([]byte(`{"op":"fork","meta":{"a":"b","a":"c"},"max_bytes":-1}`))
	f.Add([]byte(`{"op":1}`))
	f.Add([]byte(`{"op":"status"`))
	f.Add([]byte(`[{"op":"status"}]`))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		// Each successful decode consumes at least one byte, so a stream
		// of len(data) bytes cannot yield more than len(data)+1 requests;
		// more means the decoder stopped making progress.
		for n := 0; ; n++ {
			if n > len(data)+1 {
				t.Fatalf("decoded %d requests from %d bytes: decoder is not making progress", n, len(data))
			}
			req, err := decodeRequest(dec)
			if err != nil {
				return
			}
			var first bytes.Buffer
			if err := json.NewEncoder(&first).Encode(req); err != nil {
				t.Fatalf("decoded request does not re-encode: %v", err)
			}
			again, err := decodeRequest(json.NewDecoder(bytes.NewReader(first.Bytes())))
			if err != nil {
				t.Fatalf("re-encoded request %s does not decode: %v", first.Bytes(), err)
			}
			var second bytes.Buffer
			if err := json.NewEncoder(&second).Encode(again); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(first.Bytes(), second.Bytes()) {
				t.Fatalf("request is not a fixed point of the wire format:\n%s\n%s", first.Bytes(), second.Bytes())
			}
			_ = ops.ValidateMeta(req.Meta)
			for _, s := range []string{req.DB, req.Branch, req.Name, req.From} {
				_ = store.ValidateName(s)
			}
			for _, d := range []string{req.TTL, req.BackupTTL} {
				_, _ = time.ParseDuration(d)
			}
			_ = httpForbiddenOps[req.Op]
		}
	})
}
