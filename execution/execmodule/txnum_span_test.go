package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

// Every canonical block spans exactly its transactions plus its two system slots in the txNum index, whether this
// node sealed it before or after the consensus layer inserted it, and over however many rounds it was built.
func TestEveryBlockSpansItsTransactionsInTheTxNumIndex(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	parent, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	var chain []*types.Block
	for n := uint64(0); n < 4; n++ {
		rounds := [][][]byte{{replica.signed(0, n, nil, 100_000, emitsALog)}, {replica.signed(1, n, nil, 100_000, emitsALog)}}
		paramsP, inP := proposer.attrs(parent)
		proposed := proposer.sealRounds(paramsP, inP, rounds)
		ur, ferr := forkChoiceTo(proposer.ctx, proposer.m.ExecModule, proposed.Header())
		require.NoError(t, ferr)
		require.Equal(t, execmodule.ExecutionStatusSuccess, ur.Status, ur.ValidationError)

		params, in := replica.attrs(parent)
		insertFirst := n%2 == 0
		for _, r := range rounds {
			_, _, vr, rerr := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, r)
			require.NoError(t, rerr)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
		}
		if n == 3 {
			extra := replica.signed(0, n+1, nil, 100_000, emitsALog)
			_, _, vr, rerr := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, [][]byte{extra})
			require.NoError(t, rerr)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
			vr, rerr = replica.m.ExecModule.RebuildFlashblock(replica.ctx, in, [][]byte{rounds[0][0], rounds[1][0]})
			require.NoError(t, rerr)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
		}
		if insertFirst {
			status, ierr := insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{proposed})
			require.NoError(t, ierr)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
		}
		br, serr := replica.m.ExecModule.SealBlock(replica.ctx, params, false)
		require.NoError(t, serr)
		require.Equal(t, proposed.Hash(), br.Block.Hash())
		require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, []*types.Block{proposed}))
		chain = append(chain, proposed)
		parent = proposed.Header()
	}

	require.NoError(t, replica.m.DB.ViewTemporal(replica.ctx, func(tx kv.TemporalTx) error {
		for _, b := range chain {
			first, ferr := replica.m.BlockReader.TxnumReader().Min(replica.ctx, tx, b.NumberU64())
			require.NoError(t, ferr)
			last, lerr := replica.m.BlockReader.TxnumReader().Max(replica.ctx, tx, b.NumberU64())
			require.NoError(t, lerr)
			require.Equal(t, uint64(len(b.Transactions())+2), last-first+1,
				"block %d spans %d txNums for %d transactions", b.NumberU64(), last-first+1, len(b.Transactions()))
		}
		var nextBase uint64
		for i, b := range chain {
			bfs, berr := rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(b.NumberU64(), b.Hash()))
			require.NoError(t, berr)
			require.NotNil(t, bfs, "block %d body record", b.NumberU64())
			require.Equal(t, types.TxCountToTxAmount(len(b.Transactions())), bfs.TxCount, "block %d stored txCount", b.NumberU64())
			if i > 0 {
				require.Equal(t, nextBase, bfs.BaseTxnID.U64(), "block %d base txn id follows block %d", b.NumberU64(), b.NumberU64()-1)
			}
			nextBase = bfs.BaseTxnID.U64() + uint64(bfs.TxCount)
		}
		return nil
	}))
}
