package state_encoding_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"
)

// The test below match prysm output

func TestEth1DataVector(t *testing.T) {
	expected := libcommon.HexToHash("0xaa5de3cc36f794bf4e5f1882a0a3b2f6570ed933b2e12901077781e3b09b4d6a")
	votes := []*cltypes.Eth1Data{
		{
			DepositCount: 1,
		},
		{
			DepositCount: 2,
		},
	}
	root, err := state_encoding.Eth1DataVectorRoot(votes)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}

func TestValidatorsVectorRoot(t *testing.T) {
	expected := libcommon.HexToHash("0xdd9a73f49804c05654ae7a57cff4fd877f290f4a00d7cd2e7af058624fb7a369")
	validators := []*cltypes.Validator{
		{
			WithdrawalCredentials: make([]byte, 32),
			EffectiveBalance:      69,
		},
		{
			WithdrawalCredentials: make([]byte, 32),
			EffectiveBalance:      96,
		},
	}
	root, err := state_encoding.ValidatorsVectorRoot(validators)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}

func TestSlashingsRoot(t *testing.T) {
	expected := libcommon.HexToHash("0xaf328cf63282226acd6da21937c28296ece7a66100089f9f016f9ff47eaf59de")
	nums := []uint64{1, 2, 4, 5, 2, 5, 6, 7, 1, 4, 3, 5, 100, 6, 64, 2}
	root, err := state_encoding.SlashingsRoot(nums)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}
