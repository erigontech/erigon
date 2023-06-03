package ssz2_test

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/serialized.ssz_snappy
var beaconState []byte

func TestEncodeDecode(t *testing.T) {
	t.Skip("Need to update due to data_gas_used")
	bs := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(bs, beaconState, int(clparams.DenebVersion)))
	root, err := bs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x7d085d9f2cce04eefb4c0aafad744fd2ce4ff962b2c3589fda53aab084171406"))
	d, err := bs.EncodeSSZ(nil)
	require.NoError(t, err)
	dec, _ := utils.DecompressSnappy(beaconState)
	require.Equal(t, dec, d)
}
