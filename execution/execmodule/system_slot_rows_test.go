package execmodule_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
)

// A sealed block's txnum→txhash mapping must leave its two system-transaction slots EMPTY.
//
// Block-start and block-end are executed by the executor, not stored. WriteRawTransactions writes only
// At(i) = BaseTxnID+1+i, so the ids at each end of the range hold nothing and the transactions index keys
// them off the txnum (pad32) instead of a transaction hash. A one-pass chain produces exactly that — measured
// on dev-L1: 2000 system slots across 1000 blocks, every one empty, zero duplicate keys.
//
// Multi-round construction can leave a real transaction in the start slot, because a successor takes its id
// range while its parent is still accumulating; the parent grows over it and the successor is re-allocated
// higher, stranding rows. The block-start txnum then maps to a user transaction's hash — the wrong mapping,
// and a duplicate key the index cannot build (measured: 3 blocks per 1000 on trading, 0 on dev-L1).
//
// SCOPE: this pins the INVARIANT. It does not reproduce the stranding — the harness runs its rounds through
// one generation with a monotonically advancing sequence, so no allocation ever overlaps.
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
