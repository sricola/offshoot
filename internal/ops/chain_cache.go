package ops

import (
	"strings"

	"github.com/sricola/offshoot/internal/store"
)

// observeChainSource, when non-nil, is told where a checkpoint or a fork at
// head got the head's chain: "cache" (the sidecar's recorded chain, see
// cachedChain) or "resolve" (store.Chain, which lists the lineage). A
// test-only seam; nil in production.
var observeChainSource func(kind string)

// cachedChain returns the head chain the checkout's sidecar recorded
// (sumRecord.Chain) when it provably is what store.Chain(ref.Lineage,
// ref.HeadTXID) resolves, and (nil, false) otherwise. An at-rest
// checkpoint records its new head's chain in the trusted stamp it writes
// after its head write: the chain it resolved (or took from here) plus the
// object it just wrote, or that object alone when it wrote a snapshot. So
// the next checkpoint, and a fork at head, skip listing the lineage, which
// on a local store grows with every epoch the lineage's checkpoints minted.
//
// It is used only when all of these hold:
//   - the sidecar's identity (Lineage, Epoch, TXID) is the ref's head
//     (Lineage, HeadEpoch, HeadTXID);
//   - the recorded chain is non-empty, and every key parses as a member
//     key (store.ParseMemberKey) under ref.Lineage's prefix;
//   - the first member is a snapshot, every later one is a segment that
//     starts right after the previous member ends (MinTXID == previous
//     MaxTXID + 1, so strictly ascending with no hole), and the last one
//     ends at HeadTXID.
//
// That is sound because nothing changes a head's chain without changing
// the head's identity: the lease fences every other writer of the lineage,
// and a daemon session's flushes advance the head without restamping the
// sidecar, so the identity stops matching; a rollback, promote or compact
// repoints the branch at another lineage; an orphan a fenced writer left
// is under a lower epoch, either above the head (which resolution to the
// head never reaches) or at a txid at or below it (which
// store.keepHighestEpoch collapses in favour of the live, higher-epoch
// member), so it never displaces a member of the live chain; and GC never
// deletes a reachable member. A
// sidecar from before this field (no chain), a distrusted stamp (which
// records none), a materialize or a session's close (which record none)
// all resolve. A chain that crosses into a base lineage is recorded but
// never used here (its keys name another lineage), so a shared lineage
// resolves until it writes its own snapshot.
func (w *Workspace) cachedChain(path string, ref store.Ref) ([]store.ChainMember, bool) {
	rec, ok := readSidecar(path)
	if !ok || rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch || rec.TXID != ref.HeadTXID || len(rec.Chain) == 0 {
		return nil, false
	}
	prefix := store.LineagePrefix(ref.Lineage)
	members := make([]store.ChainMember, 0, len(rec.Chain))
	for _, key := range rec.Chain {
		m, ok := store.ParseMemberKey(key)
		if !ok || !strings.HasPrefix(key, prefix) {
			return nil, false
		}
		members = append(members, m)
	}
	if !members[0].Snapshot || members[len(members)-1].MaxTXID != ref.HeadTXID {
		return nil, false
	}
	for i := 1; i < len(members); i++ {
		if members[i].Snapshot || members[i].MinTXID != members[i-1].MaxTXID+1 {
			return nil, false
		}
	}
	return members, true
}

// headChain is the head's chain for a checkpoint or a fork at head: the
// sidecar's recorded chain when cachedChain accepts it, else store.Chain.
func (w *Workspace) headChain(path string, ref store.Ref) ([]store.ChainMember, error) {
	if members, ok := w.cachedChain(path, ref); ok {
		if observeChainSource != nil {
			observeChainSource("cache")
		}
		return members, nil
	}
	if observeChainSource != nil {
		observeChainSource("resolve")
	}
	return w.Store.Chain(ref.Lineage, ref.HeadTXID)
}

// chainKeys is the recorded form of a chain: its members' keys, in order,
// followed by extra (the object a checkpoint just wrote on top of it).
func chainKeys(members []store.ChainMember, extra string) []string {
	keys := make([]string, 0, len(members)+1)
	for _, m := range members {
		keys = append(keys, m.Key)
	}
	return append(keys, extra)
}
