package ethutils

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func TestMarshalReceiptLogStampsAreIndependent(t *testing.T) {
	t.Parallel()
	to := common.Address{1}
	txn := dynamicFeeTx(&to)
	receipt := &types.Receipt{BlockNumber: uint256.NewInt(1), Logs: types.Logs{{}, {}}}
	header := &types.Header{Number: *uint256.NewInt(1), Time: 42}
	got := MarshalReceipt(receipt, &txn, chain.TestChainBerlinConfig, header, false, true)
	for i, log := range got.Logs {
		require.NotSame(t, receipt.Logs[i], log)
		require.Nil(t, receipt.Logs[i].BlockTimestamp)
		require.EqualValues(t, 42, *log.BlockTimestamp)
	}
	*got.Logs[0].BlockTimestamp = 0
	require.EqualValues(t, 42, *got.Logs[1].BlockTimestamp)
}
