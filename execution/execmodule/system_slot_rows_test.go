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

// A sealed block must leave NO transaction row in either of its system-transaction slots.
//
// A body owns the txn-id range [BaseTxnID, LastSystemTx]. Real transactions only ever occupy
// At(i) = BaseTxnID+1+i, so the two ends of that range — BaseTxnID and LastSystemTx — must hold nothing.
// Every canonical read honours that: ReadBodyWithTransactions walks from First(), so a row sitting in a
// system slot is invisible to eth_getBlockByNumber, to receipts, and to re-execution. The chain looks
// perfect with one there.
//
// Exactly one consumer reads those slots: the snapshot dumper. DumpTxs calls addSystemTx(body.BaseTxnID),
// emits whatever it finds, and the transactions index then keys every emitted record on txn.Hash(). A
// leftover real transaction in the first system slot is therefore emitted TWICE — once from the slot, once
// from its real position — and recsplit cannot build an index over a duplicated key. It responds by
// retrying with another salt, forever, in an unbounded loop: measured on the demo box at 135,445 retries
// and climbing, one core pinned, 145k .tmp files, and the segment permanently unindexable.
//
// The rows get there because a block's body is written more than once. writePreExecBlock calls
// WriteRawBodyIfNotExists, whose existence check is on (number, hash) — so every round that re-hashes the
// header, and every withdrawals re-stamp, MISSES, falls through to WriteRawBody, and takes a FRESH
// BaseTxnID range from IncrementSequence(kv.EthTx, ...). rawdb.DeleteBody removes kv.BlockBody and
// kv.BlockAccessList; it does NOT remove kv.EthTx rows, so each superseded allocation's transactions stay
// behind. When a superseded range sits one id below the final one, its first real transaction lands exactly
// on the final body's BaseTxnID.
//
// Measured on two independent runs: 3 blocks per 1000 affected, ALWAYS the first system slot, never the
// last — identical counts on a local run (blocks 0..1000) and on the deployed demo (blocks 1000..2000).
//
// This is the half that TestSealedBodyKeepsItsOwnTransactionsAfterTheNextBlock and
// TestRestampedBlockKeepsItsOwnTransactionsAfterTheNextBlock cannot see: both assert body CONTENT, and the
// content is correct. The damage is entirely in the rows those blocks do not own.
func TestSealedBlockLeavesNoTransactionInItsSystemSlots(t *testing.T) {
	h := newRoundHarness(t)
	ctx := h.ctx

	head, err := h.m.ExecModule.CurrentHeader(ctx)
	require.NoError(t, err)

	// Drive the ROUND SHAPE the affected blocks actually had in production, measured on the deployed demo:
	//
	//   [TRACE-filter]  round block=1601 in=1 kept=0 stale=0 future=1
	//   [TRACE-preexec] round block=1601 roundKept=0 bodyLen=0 forkTxNum=6590
	//   [TRACE-preexec] round block=1601 roundKept=0 bodyLen=0 forkTxNum=6590   <- same txNum, kept nothing
	//   [TRACE-preexec] round block=1601 roundKept=2 bodyLen=2 forkTxNum=6592   <- admits TWO at once
	//
	// A transaction arrives whose nonce is ahead of the sender's, so the round keeps NOTHING — and a round
	// that keeps nothing still writes an (empty) body, which still takes an id range, because
	// TxCountToTxAmount(0) is 2. When the gap-filling transaction arrives, a later round admits both at once
	// and writes a body of a DIFFERENT width. Every one of those writes goes through
	// WriteRawBodyIfNotExists under a different header hash, so every one allocates afresh.
	//
	// The clean blocks around them grow one transaction per round (bodyLen 0,1,2,3) and strand nothing.
	// A round that runs IN FULL and is then DROPPED is what stranded the row. It writes the block's body
	// under its own header hash — taking a fresh id range, because WriteRawBodyIfNotExists keys its existence
	// check on (number, hash) — and is then discarded when its commit claim is refused. Discarding the
	// round drops its SharedDomains, but the transactions it wrote are in kv.EthTx, and rawdb.DeleteBody
	// (the only cleanup on this path) removes kv.BlockBody and kv.BlockAccessList, never kv.EthTx rows.
	//
	// The kept rounds around it settle on a DIFFERENT range, so the dropped round's rows are left overlapping
	// a range the block does not own — and the one that lands on the final body's BaseTxnID is the stranded
	// row the snapshot dumper then emits twice.
	params, in := h.attrs(head)
	require.NoError(t, h.round(ctx, in, 0, 0), "block 1 round 1 (kept)")
	dropped := execmodule.WithRoundCommit(ctx, func() bool { return false })
	require.ErrorIs(t, h.round(dropped, in, 1, 0), execmodule.ErrRoundAbandoned, "block 1 round 2 must be dropped")
	require.NoError(t, h.round(ctx, in, 0, 1), "block 1 round 3 (kept)")

	first := h.sealAndAdopt(params, 2)
	hash, number := first.Hash(), first.Number.Uint64()

	// The successor is what re-seeds kv.EthTx from committed state, so it is what settles which allocation
	// the block ends up owning and which are left stranded.
	// Block 1 kept key 0's nonces 0 and 1, so those are spent. The DROPPED round's transaction — key 1
	// nonce 0 — was never committed and is still pending, so it is what the next block carries.
	params2, in2 := h.attrs(first)
	require.NoError(t, h.round(ctx, in2, 1, 0), "block 2 round (the dropped round's transaction)")
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
			"block %d holds a transaction in its FIRST system slot (txnID %d): a superseded id allocation's "+
				"row was left in kv.EthTx, and the snapshot dumper will emit it as a duplicate key that no "+
				"salt can resolve", number, bfs.BaseTxnID.U64())
		require.Empty(t, occupant(bfs.BaseTxnID.LastSystemTx(bfs.TxCount)),
			"block %d holds a transaction in its LAST system slot (txnID %d)",
			number, bfs.BaseTxnID.LastSystemTx(bfs.TxCount))
		return nil
	}))
}
