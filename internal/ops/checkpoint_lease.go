package ops

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// checkpointHolderPrefix marks a lease held by an at-rest checkpoint, so a
// refusal can say another checkpoint is in progress rather than send the
// user looking for a session to close.
const checkpointHolderPrefix = "checkpoint:"

// newCheckpointHolder is the lease holder for one at-rest checkpoint call:
// "checkpoint:<host>/<pid>/<8 hex>". It keeps LocalHolder's <host>/<pid>,
// which `lease list` and status print, and adds a per-call nonce:
// AcquireLease treats the same holder on a live lease as a self-renew with
// no epoch bump, so two checkpoints in one process sharing a holder would
// share an epoch, and with it an object key, and race again.
func newCheckpointHolder() string {
	var nonce [4]byte
	_, _ = rand.Read(nonce[:]) // crypto/rand.Read does not return an error since Go 1.24
	return checkpointHolderPrefix + LocalHolder() + "/" + hex.EncodeToString(nonce[:])
}

// isCheckpointHolder reports whether a lease holder is an at-rest
// checkpoint's (see newCheckpointHolder).
func isCheckpointHolder(holder string) bool {
	return strings.HasPrefix(holder, checkpointHolderPrefix)
}
