package merkle_tree_test

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/serialized.ssz_snappy
var beaconState []byte

func TestHashTreeRoot(t *testing.T) {
	t.Skip("Need to update due to data_gas_used")
	bs := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(bs, beaconState, int(clparams.DenebVersion)))
	root, err := bs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x7d085d9f2cce04eefb4c0aafad744fd2ce4ff962b2c3589fda53aab084171406"))
}

func TestHashTreeRootTxs(t *testing.T) {
	txs := [][]byte{
		{1, 2, 3},
		{1, 2, 3},
		{1, 2, 3},
	}
	root, err := merkle_tree.TransactionsListRoot(txs)
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x987269bc1075122edff32bfc38479757103cee5c1ed6e990de7ffee85b5dd18a"))
}
