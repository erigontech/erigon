package state_test

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
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
}

func TestBeaconStatePhase0EncodingDecoding(t *testing.T) {
	state := state.New(&clparams.MainnetBeaconConfig)
	decodedSSZ, err := utils.DecompressSnappy(phase0BeaconSnappyTest)
	require.NoError(t, err)
	state.DecodeSSZ(decodedSSZ, int(clparams.Phase0Version))
	root, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(root), libcommon.HexToHash("0xf23b6266af40567516afeee250c1f8c06e9800f34a990a210604c380b506e053"))
}
