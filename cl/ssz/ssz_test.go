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
	bs := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(bs, beaconState, int(clparams.CapellaVersion)))
	root, err := bs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x36eb1bb5b4616f9d5046b2a719a8c4217f3dc40c1b7dff7abcc55c47f142a78b"))
	d, err := bs.EncodeSSZ(nil)
	require.NoError(t, err)
	dec, _ := utils.DecompressSnappy(beaconState)
	require.Equal(t, dec, d)
}
