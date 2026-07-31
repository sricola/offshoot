package store

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrNoCAS reports a backend that does not enforce conditional writes.
var ErrNoCAS = errors.New("store: backend does not enforce conditional writes")

// ProbeCAS verifies create-only and compare-and-swap enforcement. offshoot's
// single-writer-per-ref guarantee rests entirely on these semantics, so a
// store that fails the probe is refused outright rather than used with
// silently weaker safety.
func ProbeCAS(b Backend) error {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return err
	}
	key := "probe/cas-" + hex.EncodeToString(buf)
	defer b.Delete(key)

	first := []byte("offshoot-probe-1")
	etag, err := b.PutIf(key, first, "")
	if err != nil {
		return fmt.Errorf("store: probe create-only put failed: %w", err)
	}

	// A second create-only put must be rejected AND must not modify content.
	second := []byte("offshoot-probe-2")
	if _, err := b.PutIf(key, second, ""); err == nil {
		return fmt.Errorf("%w: create-only put over an existing key was accepted", ErrNoCAS)
	} else if !errors.Is(err, ErrCAS) {
		return fmt.Errorf("store: probe create-only put returned an unexpected error: %w", err)
	}
	if err := probeContentIs(b, key, first, "after a rejected create-only put"); err != nil {
		return err
	}

	// A CAS with a bogus etag must be rejected AND must not modify content.
	if _, err := b.PutIf(key, second, "offshoot-probe-not-a-real-etag"); err == nil {
		return fmt.Errorf("%w: compare-and-swap with a stale etag was accepted", ErrNoCAS)
	} else if !errors.Is(err, ErrCAS) {
		return fmt.Errorf("store: probe stale-etag put returned an unexpected error: %w", err)
	}
	if err := probeContentIs(b, key, first, "after a rejected compare-and-swap"); err != nil {
		return err
	}

	// A CAS with the current etag must succeed.
	if _, err := b.PutIf(key, second, etag); err != nil {
		return fmt.Errorf("store: probe compare-and-swap with a valid etag failed: %w", err)
	}
	return probeContentIs(b, key, second, "after a successful compare-and-swap")
}

func probeContentIs(b Backend, key string, want []byte, when string) error {
	got, _, err := b.Get(key)
	if err != nil {
		return fmt.Errorf("store: probe read %s: %w", when, err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%w: content changed %s", ErrNoCAS, when)
	}
	return nil
}
