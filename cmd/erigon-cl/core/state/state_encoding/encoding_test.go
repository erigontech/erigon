package state_encoding_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/state_encoding"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

// The test below match prysm output

func TestEmptyArraysRoot(t *testing.T) {
	expected := common.HexToHash("df6af5f5bbdb6be9ef8aa618e4bf8073960867171e29676f8b284dea6a08a85e")
	root, err := state_encoding.ArraysRoot([][32]byte{}, state_encoding.StateRootsLength)
	require.NoError(t, err)
	require.Equal(t, expected, common.Hash(root))
}

func TestEmptyArraysWithLengthRoot(t *testing.T) {
	expected := common.HexToHash("0xf770287da731841c38eb035da016bd2daad53bf0bca607461c0685b0ea54c5f9")
	roots := [][32]byte{
		common.BytesToHash([]byte{1}),
		common.BytesToHash([]byte{2}),
		common.BytesToHash([]byte{3}),
		common.BytesToHash([]byte{4}),
		common.BytesToHash([]byte{5}),
		common.BytesToHash([]byte{6}),
		common.BytesToHash([]byte{7}),
		common.BytesToHash([]byte{8}),
	}
	root, err := state_encoding.ArraysRootWithLimit(roots, 8192)
	require.NoError(t, err)
	require.Equal(t, expected, common.Hash(root))
}

func TestEth1DataVector(t *testing.T) {
	expected := common.HexToHash("0xaa5de3cc36f794bf4e5f1882a0a3b2f6570ed933b2e12901077781e3b09b4d6a")
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
	require.Equal(t, expected, common.Hash(root))
}

func TestValidatorsVectorRoot(t *testing.T) {
	expected := common.HexToHash("0xdd9a73f49804c05654ae7a57cff4fd877f290f4a00d7cd2e7af058624fb7a369")
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
	require.Equal(t, expected, common.Hash(root))
}

func TestUint64ListRootWithLimit(t *testing.T) {
	expected := common.HexToHash("0xfbe583f8fbcc3683d98c12ae969e93aaa5ac472e15422c14759cb7f3ef60673c")
	nums := []uint64{1, 2, 4, 5, 2, 5, 6, 7, 1, 4, 3, 5, 100, 6, 64, 2}
	root, err := state_encoding.Uint64ListRootWithLimit(nums, 274877906944)
	require.NoError(t, err)
	require.Equal(t, expected, common.Hash(root))
}

func TestSlashingsRoot(t *testing.T) {
	expected := common.HexToHash("0xaf328cf63282226acd6da21937c28296ece7a66100089f9f016f9ff47eaf59de")
	nums := []uint64{1, 2, 4, 5, 2, 5, 6, 7, 1, 4, 3, 5, 100, 6, 64, 2}
	root, err := state_encoding.SlashingsRoot(nums)
	require.NoError(t, err)
	require.Equal(t, expected, common.Hash(root))
}

func TestParticipationBitsRoot(t *testing.T) {
	expected := common.HexToHash("0x8e6653ba3656afddaf5e6c69c149e63a2e26ff91a2e361b3c40b11f08c039572")
	bits := []byte{1, 2, 4, 5, 2, 5, 6, 7, 1, 4, 3, 5, 100, 6, 64, 2}
	root, err := state_encoding.ParticipationBitsRoot(bits)
	require.NoError(t, err)
	require.Equal(t, expected, common.Hash(root))
}
