package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// TestNoopStorageTouchIdempotent probes whether emitting extra storage touches
// that write the already-committed value (a no-op SSTORE the serial no-op filter
// drops) — together with the touched account's unchanged leaf — shifts the trie
// root. This is the rawViewCollapse "Bug B" question: does the extra-key set
// alone cause a wrong root, or is it idempotent noise?
func TestNoopStorageTouchIdempotent(t *testing.T) {
	seedKeys, seedUpd := fixtureBaseAccounts().Build()

	// Two independently-seeded tries with identical committed state.
	msA := NewMockState(t)
	trieA := NewHexPatriciaHashed(length.Addr, msA, DefaultTrieConfig())
	trieA.SetTraceWriter(nil)
	rootSeedA := processBatch(t, msA, trieA, seedKeys, seedUpd)

	msB := NewMockState(t)
	trieB := NewHexPatriciaHashed(length.Addr, msB, DefaultTrieConfig())
	trieB.SetTraceWriter(nil)
	rootSeedB := processBatch(t, msB, trieB, seedKeys, seedUpd)
	require.Equal(t, rootSeedA, rootSeedB, "seed roots must match")

	// Batch 2 — a genuine change: bump one account's balance.
	realKeys, realUpd := NewUpdateBuilder().
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 999999).
		Build()
	rootReal := processBatch(t, msA, trieA, realKeys, realUpd)

	// Batch 2' — the same genuine change PLUS "no-op" re-emissions:
	//  - storage slots re-written with their already-committed values
	//  - the owning accounts' leaves re-emitted with unchanged fields (Bug A path)
	noopKeys, noopUpd := NewUpdateBuilder().
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 999999).
		// no-op storage: same values as committed in fixtureBaseAccounts
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
		// unchanged account leaves (Bug A: storage touch dirties account)
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Build()
	rootNoop := processBatch(t, msB, trieB, noopKeys, noopUpd)

	require.Equal(t, rootReal, rootNoop,
		"extra no-op storage touches + unchanged account leaves must NOT shift the root")
}

// TestPhantomAccountShiftsRoot confirms the other half: emitting an account leaf
// that was ABSENT pre-block (a reverted-CREATE phantom: nonce=1, code) DOES shift
// the root, while a zero-write to an absent storage slot (DeleteUpdate of a
// non-existent key) does NOT. This pins the real rawView root-breaker as the
// phantom account, not the storage no-ops.
func TestPhantomAccountShiftsRoot(t *testing.T) {
	seedKeys, seedUpd := fixtureBaseAccounts().Build()

	msReal := NewMockState(t)
	trieReal := NewHexPatriciaHashed(length.Addr, msReal, DefaultTrieConfig())
	trieReal.SetTraceWriter(nil)
	processBatch(t, msReal, trieReal, seedKeys, seedUpd)

	msPh := NewMockState(t)
	triePh := NewHexPatriciaHashed(length.Addr, msPh, DefaultTrieConfig())
	triePh.SetTraceWriter(nil)
	processBatch(t, msPh, triePh, seedKeys, seedUpd)

	// Real batch: a genuine balance bump only.
	realKeys, realUpd := NewUpdateBuilder().
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 999999).
		Build()
	rootReal := processBatch(t, msReal, trieReal, realKeys, realUpd)

	// Phantom batch: same bump PLUS a previously-absent account emitted with
	// nonce=1 and code (a reverted CREATE the rawView path persists), PLUS a
	// zero-write to a previously-absent storage slot (should be a harmless
	// delete-of-absent).
	phKeys, phUpd := NewUpdateBuilder().
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 999999).
		Nonce("2bd541ab3b704f7d4c9dff79efadeaa85ec034f1", 1).
		CodeHash("2bd541ab3b704f7d4c9dff79efadeaa85ec034f1", "5fe6c3721132ddcc5d6545b888995e1a1d1ff30764ce2b57bf9bda0b836d2bbc").
		Storage("14c4d3bba7f5009599257d3701785d34c7f2aa27", "1111111111111111111111111111111111111111111111111111111111111111", "").
		Build()
	rootPh := processBatch(t, msPh, triePh, phKeys, phUpd)

	require.NotEqual(t, rootReal, rootPh,
		"a phantom account (absent -> nonce=1+code) MUST shift the root")
}
