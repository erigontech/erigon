package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestVersionedRead_RecordOnce_DirtyReResolveDoesNotOverwrite pins the read-side
// invariant the optimistic (Validated) path depends on: a versioned read is
// recorded on first access and is NOT re-recorded on a repeat read just because
// the account went dirty from an *unrelated* write. Overwriting the recorded
// read with the value re-resolved on the second read is the B1 consumed!=recorded
// gap — the reader acts on the first version but records the second, so seal-time
// re-validation compares (second == current) and passes, letting a diverged read
// commit stale.
//
// Reader consumes v1 first; the writer then diverges to v2; the repeat read must
// still return v1 (record-once), and the final dep check must see v1 != v2.
func TestVersionedRead_RecordOnce_DirtyReResolveDoesNotOverwrite(t *testing.T) {
	t.Parallel()
	_, tx, domains := NewTestRwTx(t)

	mvhm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))

	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	key := accounts.InternKey(common.HexToHash("0x01"))      // the slot the reader depends on
	otherKey := accounts.InternKey(common.HexToHash("0x02")) // an unrelated slot, to dirty addr
	v1 := *uint256.NewInt(0x54b6)
	v2 := *uint256.NewInt(0x5479)

	// tx0 commits key=v1 as a Done value at version (0,0).
	w := NewWithVersionMap(reader, mvhm)
	defer w.Close()
	w.txIndex = 0
	require.NoError(t, w.SetState(addr, key, v1))
	mvhm.FlushVersionedWrites(w.VersionedWrites(), true, "")

	// Reader (tx2) first read: sees the committed v1 and records the dependency.
	r := NewWithVersionMap(reader, mvhm)
	defer r.Close()
	r.txIndex = 2
	got, err := r.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, v1, got, "first read must see the committed value v1")

	// Reader writes an UNRELATED slot of the same account -> addr goes dirty.
	require.NoError(t, r.SetState(addr, otherKey, *uint256.NewInt(7)))

	// The writer re-executes and diverges: key=v2 at a new incarnation.
	mvhm.WriteStorage(addr, key, Version{TxIndex: 0, Incarnation: 1}, v2, true)

	// Repeat read of key. Block-STM read-once: it must return the first-consumed
	// value v1, not re-resolve to v2. The tx did not write `key` (only otherKey),
	// so the per-address dirty state must not bypass read-once for this cell.
	got2, err := r.GetState(addr, key)
	require.NoError(t, err)
	require.Equal(t, v1, got2,
		"repeat read must return the first-recorded value (record-once), not re-resolve to v2")

	// Final dep check: reader consumed v1, committed value is now v2 -> invalid.
	valid := mvhm.ValidateReadSet(2, r.VersionedReads(),
		func(rv, wv Version) VersionValidity {
			if rv != wv {
				return VersionInvalid
			}
			return VersionValid
		}, false, "")
	require.Equal(t, VersionInvalid, valid,
		"final check must catch recorded-v1 vs committed-v2; else a diverged read commits stale (B1)")
}
