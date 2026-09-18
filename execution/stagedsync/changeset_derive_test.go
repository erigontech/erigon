package stagedsync

import (
	"context"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func acctBytes(nonce uint64, bal uint64) []byte {
	a := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(bal), CodeHash: accounts.EmptyCodeHash}
	return accounts.SerialiseV3(&a)
}

func sortDiffs(d []kv.DomainEntryDiff) []kv.DomainEntryDiff {
	out := append([]kv.DomainEntryDiff(nil), d...)
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// TestChangesetDerivedEqualsAccumulated proves the framework: block N's account
// changeset, DERIVED at block-end as GetAsOf(block-boundary) over the write set,
// equals the changeset the accumulated DomainPut path produces. No production
// wiring changed — this is the equivalence proof the derivation rests on.
func TestChangesetDerivedEqualsAccumulated(t *testing.T) {
	ctx := context.Background()
	_, tx, doms := setupStepTest(t) // stepSize 16

	// Pre-block base (block N-1, step 0): accounts A,B,C,E committed to disk.
	addrs := map[string][]byte{"A": {0x11}, "B": {0x22}, "C": {0x33}, "D": {0x44}, "E": {0x55}}
	for _, n := range []string{"A", "B", "C", "E"} {
		k := make([]byte, 20)
		copy(k, addrs[n])
		require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, k, acctBytes(1, 100), 5, nil))
	}
	require.NoError(t, doms.Flush(ctx, tx))

	// Block N: txNums 17..21 (step 1). A written TWICE (dedup must keep pre-block
	// prev), B updated, D new, E deleted, C untouched.
	const blockFirstTxNum = uint64(17)
	writes := []struct {
		name  string
		txNum uint64
		val   []byte // nil = delete
	}{
		{"A", 17, acctBytes(2, 200)},
		{"B", 18, acctBytes(2, 250)},
		{"D", 19, acctBytes(1, 300)},
		{"A", 20, acctBytes(3, 400)}, // second write of A
		{"E", 21, nil},               // delete
	}

	// --- accumulated path ---
	cs := &changeset.StateChangeSet{}
	doms.SetChangesetAccumulator(cs)
	for _, w := range writes {
		k := make([]byte, 20)
		copy(k, addrs[w.name])
		prev, _, err := doms.GetLatest(kv.AccountsDomain, tx, k)
		require.NoError(t, err)
		if w.val == nil {
			require.NoError(t, doms.DomainDel(kv.AccountsDomain, tx, k, w.txNum, prev))
		} else {
			require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, k, w.val, w.txNum, prev))
		}
	}
	accumulated := sortDiffs(cs.Diffs[kv.AccountsDomain].GetDiffSet())

	// --- derived path: GetAsOf(boundary) per written key, at the write's step ---
	derived := &kv.DomainDiff{}
	for _, w := range writes {
		k := make([]byte, 20)
		copy(k, addrs[w.name])
		prev, _, err := doms.GetAsOf(kv.AccountsDomain, k, blockFirstTxNum)
		require.NoError(t, err)
		step := kv.Step(w.txNum / 16)
		derived.DomainUpdate(k, step, prev)
	}
	derivedSet := sortDiffs(derived.GetDiffSet())

	require.Equal(t, len(accumulated), len(derivedSet), "diff count must match")
	for i := range accumulated {
		require.Equal(t, []byte(accumulated[i].Key), []byte(derivedSet[i].Key), "diff key %d", i)
		require.Equal(t, accumulated[i].Value, derivedSet[i].Value, "diff prevValue %d (key %x)", i, accumulated[i].Key)
	}
}
