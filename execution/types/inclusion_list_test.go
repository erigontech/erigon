package types

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestInclusionListTypeConversion(t *testing.T) {
	tx1 := NewTransaction(0, common.Address{}, uint256.NewInt(1), 21000, uint256.NewInt(1), nil)
	tx2 := NewTransaction(1, common.Address{}, uint256.NewInt(2), 22000, uint256.NewInt(2), []byte("data"))

	il, err := ConvertTransactionstoInclusionList(Transactions{tx1, tx2})
	require.NoError(t, err)

	data, err := il.MarshalJSON()
	require.NoError(t, err)

	var decoded InclusionList
	err = decoded.UnmarshalJSON(data)
	require.NoError(t, err)

	txs, err := ConvertInclusionListToTransactions(decoded)
	require.NoError(t, err)

	require.Equal(t, 2, len(txs))

	require.Equal(t, tx1.Hash(), txs[0].Hash())
	require.Equal(t, tx2.Hash(), txs[1].Hash())
}
