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

func TestBeaconStateCapellaEncodingDecoding(t *testing.T) {
	state := state.New(&clparams.MainnetBeaconConfig)
	decodedSSZ, err := utils.DecompressSnappy(capellaBeaconSnappyTest)
	require.NoError(t, err)
	require.NoError(t, state.DecodeSSZWithVersion(decodedSSZ, int(clparams.CapellaVersion)))
	root, err := state.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(root), libcommon.HexToHash("0xb3012b73c02ab66b2779d996f9d33d36e58bf71ffc8f3e12e07024606617a9c0"))
	// encoding it back
	dec, err := state.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, dec, decodedSSZ)
}
