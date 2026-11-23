package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// Folding each transaction's own logs into the block aggregate assigns block-wide
// indices in commit order, independent of the per-tx-local indices the source carries.
func TestBlockState_AppendTxLogsReindexesAcrossTxs(t *testing.T) {
	t.Parallel()
	bs := NewBlockState(nil)

	// tx0 emitted two logs, tx1 one, each numbered from zero in its own per-tx arena.
	tx0 := types.Logs{
		{Address: common.Address{0xa0}, TxIndex: 0, Index: 0, Data: []byte("a")},
		{Address: common.Address{0xa1}, TxIndex: 0, Index: 1, Data: []byte("bb")},
	}
	tx1 := types.Logs{
		{Address: common.Address{0xb0}, TxIndex: 1, Index: 0, Data: []byte("ccc")},
	}

	bs.AppendTxLogs(tx0)
	bs.AppendTxLogs(tx1)

	got := bs.BlockLogs()
	require.Len(t, got, 3)
	for i, l := range got {
		require.Equal(t, hexutil.Uint(i), l.Index, "block-wide index is assigned in fold order")
	}
	require.Equal(t, hexutil.Uint(0), got[0].TxIndex)
	require.Equal(t, hexutil.Uint(0), got[1].TxIndex)
	require.Equal(t, hexutil.Uint(1), got[2].TxIndex)
	require.Equal(t, []byte("a"), []byte(got[0].Data))
	require.Equal(t, []byte("ccc"), []byte(got[2].Data))

	// The aggregate owns deep copies, so mutating a source entry does not reach it.
	tx0[0].Data[0] = 'z'
	require.Equal(t, []byte("a"), []byte(bs.BlockLogs()[0].Data))

	bs.ResetBlockLogs()
	require.Empty(t, bs.BlockLogs())
}
