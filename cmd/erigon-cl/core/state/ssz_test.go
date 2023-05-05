package state_test

import (
	_ "embed"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/stretchr/testify/require"
)

//go:embed tests/capella.ssz_snappy
var capellaBeaconSnappyTest []byte

//go:embed tests/phase0.ssz_snappy
var phase0BeaconSnappyTest []byte

func TestBeaconStateCapellaEncodingDecoding(t *testing.T) {
	state := state.New(&clparams.MainnetBeaconConfig)
	decodedSSZ, err := utils.DecompressSnappy(capellaBeaconSnappyTest)
	require.NoError(t, err)
	require.NoError(t, state.DecodeSSZ(decodedSSZ, int(clparams.CapellaVersion)))
	root, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(root), libcommon.HexToHash("0xb3012b73c02ab66b2779d996f9d33d36e58bf71ffc8f3e12e07024606617a9c0"))
	// encoding it back
	dec, err := state.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, dec, decodedSSZ)
}

func TestBeaconStatePhase0EncodingDecoding(t *testing.T) {
	state := state.New(&clparams.MainnetBeaconConfig)
	decodedSSZ, err := utils.DecompressSnappy(phase0BeaconSnappyTest)
	require.NoError(t, err)
	state.DecodeSSZ(decodedSSZ, int(clparams.Phase0Version))
	root, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(root), libcommon.HexToHash("0x5fd0bc1a74028b598f2eb0190a25e58896b26d4aad24fa9cb7672e6114e01446"))
	// encoding it back
	dec, err := state.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, dec, decodedSSZ)
}
