package execmodule_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
)

// A sealed block's txnum→txhash mapping must leave its two system-transaction slots EMPTY.
//
// Block-start and block-end are executed by the executor, not stored. WriteRawTransactions writes only
// At(i) = BaseTxnID+1+i, so the ids at each end of the range hold nothing and the transactions index keys
// them off the txnum (pad32) instead of a transaction hash. A one-pass chain produces exactly that — measured
// on dev-L1: 2000 system slots across 1000 blocks, every one empty, zero duplicate keys.
//
// Multi-round construction used to break this: every round that re-hashed the header allocated a fresh id range,
// leaving earlier ranges behind with their transactions still stored, where a row could sit on a block's system
// slot. That txnum then maps to a user transaction's hash — the wrong mapping, and a duplicate key the snapshot
// index cannot build (measured: 3 blocks per 1000 on trading, 0 on dev-L1).
func TestSealedBlockLeavesNoTransactionInItsSystemSlots(t *testing.T) {
	h := newRoundHarness(t)
	ctx := h.ctx

	head, err := h.m.ExecModule.CurrentHeader(ctx)
	require.NoError(t, err)

	params, in := h.attrs(head)
	require.NoError(t, h.round(ctx, in, 0, 0), "block 1 round 1")
	require.NoError(t, h.round(ctx, in, 0, 1), "block 1 round 2")
	first := h.sealAndAdopt(params, 2)
	hash, number := first.Hash(), first.Number.Uint64()

	// The successor is what settles which allocation the block ends up owning.
	params2, in2 := h.attrs(first)
	require.NoError(t, h.round(ctx, in2, 1, 0), "block 2 round")
	h.sealAndAdopt(params2, 1)

	require.NoError(t, h.m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		bfs, rerr := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(number, hash))
		require.NoError(t, rerr)
		require.NotNil(t, bfs, "the sealed block must have a body record")

		id := make([]byte, 8)
		occupant := func(txnID uint64) []byte {
			binary.BigEndian.PutUint64(id, txnID)
			v, gerr := tx.GetOne(kv.EthTx, id)
			require.NoError(t, gerr)
			return v
		}

		require.Empty(t, occupant(bfs.BaseTxnID.U64()),
			"block %d maps its block-START txnum (%d) to a stored transaction; that txnum must key off the "+
				"txnum itself, not a transaction hash", number, bfs.BaseTxnID.U64())
		require.Empty(t, occupant(bfs.BaseTxnID.LastSystemTx(bfs.TxCount)),
			"block %d maps its block-END txnum (%d) to a stored transaction",
			number, bfs.BaseTxnID.LastSystemTx(bfs.TxCount))
		return nil
	}))
}

// A block's ids are contiguous with its successor's, and a DROPPED round cannot leave a row on the kept block's
// end system slot.
//
// Rounds write a block's body at a fixed base without advancing the txn id sequence; the seal advances it once,
// by the final count. The block overlay is shared across rounds, so a round that wrote a longer body and was then
// dropped leaves its rows behind although the body is rolled back — the first of them exactly on the kept block's
// end system slot, which must stay a nil entry. Before the fix every round that re-hashed the header allocated a
// fresh range, so block 1 below took ids 2-4 and then 5-8, abandoning the first range with a transaction still in it.
func TestPreExecBlockIdsAreContiguousAndADroppedRoundLeavesNothing(t *testing.T) {
	h := newRoundHarness(t)
	ctx := h.ctx

	head, err := h.m.ExecModule.CurrentHeader(ctx)
	require.NoError(t, err)

	params, in := h.attrs(head)
	require.NoError(t, h.round(ctx, in, 0, 0), "block 1 round 1 (kept)")
	require.NoError(t, h.round(ctx, in, 0, 1), "block 1 round 2 (kept)")
	dropped := execmodule.WithRoundCommit(ctx, func() bool { return false })
	require.ErrorIs(t, h.round(dropped, in, 1, 0), execmodule.ErrRoundAbandoned, "block 1 round 3 must be dropped")
	first := h.sealAndAdopt(params, 2)

	// The dropped round's transaction is still pending, so the successor carries it.
	params2, in2 := h.attrs(first)
	require.NoError(t, h.round(ctx, in2, 1, 0), "block 2 round")
	second := h.sealAndAdopt(params2, 1)

	// A block's body reaches the committed DB only once a later block's fork choice flushes it.
	params3, in3 := h.attrs(second)
	require.NoError(t, h.round(ctx, in3, 1, 1), "block 3 round")
	h.sealAndAdopt(params3, 1)

	require.NoError(t, h.m.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		b1, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(first.Number.Uint64(), first.Hash()))
		require.NoError(t, err)
		require.NotNil(t, b1)
		b2, err := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(second.Number.Uint64(), second.Hash()))
		require.NoError(t, err)
		require.NotNil(t, b2)

		b1End := b1.BaseTxnID.LastSystemTx(b1.TxCount)
		require.Equal(t, b1End+1, b2.BaseTxnID.U64(),
			"block 2 must start right after block 1's end system slot: block 1 ids %d..%d, block 2 base %d",
			b1.BaseTxnID.U64(), b1End, b2.BaseTxnID.U64())

		id := make([]byte, 8)
		get := func(txnID uint64) []byte {
			binary.BigEndian.PutUint64(id, txnID)
			v, gerr := tx.GetOne(kv.EthTx, id)
			require.NoError(t, gerr)
			return v
		}
		require.Empty(t, get(b1.BaseTxnID.U64()), "block 1 start system slot must be a nil entry")
		require.Empty(t, get(b1End), "block 1 end system slot holds a row: the dropped round's longer body was left behind")
		require.Empty(t, get(b2.BaseTxnID.U64()), "block 2 start system slot must be a nil entry")
		for i := 0; i < 2; i++ {
			require.NotEmpty(t, get(b1.BaseTxnID.At(i)), "block 1 transaction %d must be stored at its id", i)
		}
		return nil
	}))
}
