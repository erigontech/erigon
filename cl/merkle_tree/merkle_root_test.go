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
	bs := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(bs, beaconState, int(clparams.DenebVersion)))
	root, err := bs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x9f684cf34c4ac8eb9056051f93498c552b59de6b0977c453ee099be68e58d90c"))
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
