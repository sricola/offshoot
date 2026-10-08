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
// the next checkpoint, a fork at head, a promote (which always resolves
// its source's head) and a rollback to the head's own checkpoint skip
// listing the lineage, which on a local store grows with every epoch the
// lineage's checkpoints minted.
//
// It is used only when all of these hold:
//   - the sidecar's identity (Lineage, Epoch, TXID) is the ref's head
//     (Lineage, HeadEpoch, HeadTXID);
//   - the recorded chain is non-empty, and every key parses as a member
//     key (store.ParseMemberKey);
//   - the keys are a run on other lineages (possibly empty) followed by a
//     run under ref.Lineage's prefix (non-empty), never interleaved; when
//     the first run is non-empty the ref has a base pointer, the first run
//     ends at ref.Base.TXID and the second starts at ref.Base.TXID + 1 with
//     a segment;
//   - the first member is a snapshot, every later one is a segment that
//     starts right after the previous member ends (MinTXID == previous
//     MaxTXID + 1, so strictly ascending with no hole), and the last one
//     ends at HeadTXID under ref.HeadEpoch (the checkpoint key is always
//     written under the lease epoch the head records, so this holds by
//     construction; checked anyway in case a future stamp bug decouples
//     the two).
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
// deletes a reachable member. A sidecar from before this field (no chain),
// a distrusted stamp (which records none), a materialize or a session's
// close (which record none) all resolve.
//
// A shared child's chain begins with members on its base spine, below the
// fork point, and those cannot change under an unchanged child head
// either. A lineage's base pointer is immutable (written create-only) and
// ref.Base mirrors it, so the seam a record was built against is the seam
// the ref still names. No writer writes an object at a txid at or below
// the head of a lineage a ref already names (a checkpoint writes at
// HeadTXID + 1, a session flushes above the head, and a rollback, promote
// or compact writes into a new lineage before any ref names it), so no
// member below the seam is superseded after the fork. GC marks what
// every ref's head resolves to, which for the child follows the same base
// pointer to the same ancestor members, so destroying the base branch does
// not delete them while the child reaches them. A spine of several hops
// obeys the same facts per hop, so the first run may span several
// lineages and the check does not need to know which. What this does not
// prove is that the first run's keys name lineages on this child's spine:
// the sidecar's keys are trusted structurally, as they are for a single
// lineage, because the sidecar is a local file in the checkout's own
// directory, the same trust domain as the checkout itself.
func (w *Workspace) cachedChain(path string, ref store.Ref) ([]store.ChainMember, bool) {
	rec, ok := readSidecar(path)
	if !ok || rec.Lineage != ref.Lineage || rec.Epoch != ref.HeadEpoch || rec.TXID != ref.HeadTXID || len(rec.Chain) == 0 {
		return nil, false
	}
	own := store.LineagePrefix(ref.Lineage)
	members := make([]store.ChainMember, 0, len(rec.Chain))
	// seam is the index of the first key under ref.Lineage: every key
	// before it is on another lineage (the base spine), every key from it
	// on is the lineage's own.
	seam := -1
	for i, key := range rec.Chain {
		m, ok := store.ParseMemberKey(key)
		if !ok {
			return nil, false
		}
		if strings.HasPrefix(key, own) {
			if seam < 0 {
				seam = i
			}
		} else if seam >= 0 {
			return nil, false // an ancestor key after an own key is never a chain
		}
		members = append(members, m)
	}
	if seam < 0 {
		return nil, false // no own key: nothing this head wrote
	}
	if seam > 0 {
		// A shared child: the first run is the base spine's half of the
		// chain and must end exactly at the fork point the ref names, with
		// the lineage's own half starting right after it.
		if ref.Base == nil || members[seam-1].MaxTXID != ref.Base.TXID || members[seam].MinTXID != ref.Base.TXID+1 {
			return nil, false
		}
		if members[seam].Snapshot {
			return nil, false // an own snapshot never follows ancestor keys
		}
	}
	if !members[0].Snapshot || members[len(members)-1].MaxTXID != ref.HeadTXID || members[len(members)-1].Epoch != ref.HeadEpoch {
		return nil, false
	}
	for i := 1; i < len(members); i++ {
		if members[i].Snapshot || members[i].MinTXID != members[i-1].MaxTXID+1 {
			return nil, false
		}
	}
	return members, true
}

// headChain resolves the chain at a ref's head for every caller that
// resolves exactly the head: a segment checkpoint, a fork at head, a
// promote (which always resolves its source's head) and a rollback to the
// head's own checkpoint. It returns the sidecar's recorded chain when
// cachedChain accepts it, and otherwise falls back to store.Chain.
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
