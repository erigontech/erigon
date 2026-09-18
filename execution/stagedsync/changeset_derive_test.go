package stagedsync

import (
	"context"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
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

// TestChangesetDerivedEqualsAccumulated_SelfDestruct proves the one storage case
// that needs more than the tx write set: a self-destruct (DomainDelPrefix) records
// a per-slot delete diff for every slot the account held; the derivation must
// enumerate those pre-block slots as-of the boundary (they are tombstoned by
// block-end) and emit the same diffs.
func TestChangesetDerivedEqualsAccumulated_SelfDestruct(t *testing.T) {
	ctx := context.Background()
	_, tx, doms := setupStepTest(t) // stepSize 16

	// Account X with 3 storage slots, committed as the pre-block base.
	acct := make([]byte, 20)
	acct[0] = 0x99
	slots := [][]byte{}
	for i := 0; i < 3; i++ {
		k := make([]byte, 52) // 20 addr + 32 loc
		copy(k, acct)
		k[51] = byte(i + 1)
		val := []byte{0xaa, byte(i + 1)}
		require.NoError(t, doms.DomainPut(kv.StorageDomain, tx, k, val, 5, nil))
		slots = append(slots, k)
	}
	require.NoError(t, doms.Flush(ctx, tx))

	const sdTxNum = uint64(17) // block N boundary txNum for the self-destruct
	const blockBoundary = uint64(17)

	// --- accumulated path: self-destruct via DomainDelPrefix ---
	cs := &changeset.StateChangeSet{}
	doms.SetChangesetAccumulator(cs)
	require.NoError(t, doms.DomainDelPrefix(kv.StorageDomain, tx, acct, sdTxNum))
	accumulated := sortDiffs(cs.Diffs[kv.StorageDomain].GetDiffSet())

	// --- derived path: enumerate pre-block slots as-of boundary, GetAsOf each ---
	derived := &kv.DomainDiff{}
	prefixEnd := make([]byte, 20)
	copy(prefixEnd, acct)
	for i := len(prefixEnd) - 1; i >= 0; i-- { // next-prefix
		prefixEnd[i]++
		if prefixEnd[i] != 0 {
			break
		}
	}
	it, err := tx.RangeAsOf(kv.StorageDomain, acct, prefixEnd, blockBoundary, order.Asc, -1)
	require.NoError(t, err)
	defer it.Close()
	step := kv.Step(sdTxNum / 16)
	got := 0
	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		if len(v) == 0 {
			continue
		}
		derived.DomainUpdate(k, step, v)
		got++
	}
	require.Equal(t, len(slots), got, "must enumerate all pre-block slots")
	derivedSet := sortDiffs(derived.GetDiffSet())

	require.Equal(t, len(accumulated), len(derivedSet), "self-destruct diff count")
	for i := range accumulated {
		require.Equal(t, []byte(accumulated[i].Key), []byte(derivedSet[i].Key), "sd diff key %d", i)
		require.Equal(t, accumulated[i].Value, derivedSet[i].Value, "sd diff prevValue %d", i)
	}
}
