package ethutils

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestMarshalReceiptSenderCacheParity(t *testing.T) {
	t.Parallel()
	key, err := crypto.HexToECDSA("0123456789012345678901234567890123456789012345678901234567890123")
	require.NoError(t, err)
	want := crypto.PubkeyToAddress(key.PublicKey)
	header := &types.Header{Number: *uint256.NewInt(1)}
	receipt := &types.Receipt{BlockNumber: uint256.NewInt(1)}
	for _, cached := range []bool{false, true} {
		txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), 21000, uint256.NewInt(1), nil),
			*types.LatestSignerForChainID(uint256.NewInt(1)), key)
		require.NoError(t, err)
		if cached {
			txn.SetSender(accounts.InternAddress(want))
		}
		got := MarshalReceipt(receipt, txn, chain.TestChainBerlinConfig, header, true, false)
		require.NotNil(t, got.From)
		require.Equal(t, want, *got.From)
		require.Nil(t, MarshalReceipt(receipt, txn, chain.TestChainBerlinConfig, header, false, false).From)
	}
}
