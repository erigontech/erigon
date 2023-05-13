package state_encoding_test

import (
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/state_encoding"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func TestSlashingsRoot(t *testing.T) {
	expected := libcommon.HexToHash("0xaf328cf63282226acd6da21937c28296ece7a66100089f9f016f9ff47eaf59de")
	nums := []uint64{1, 2, 4, 5, 2, 5, 6, 7, 1, 4, 3, 5, 100, 6, 64, 2}
	root, err := state_encoding.SlashingsRoot(nums)
	require.NoError(t, err)
	require.Equal(t, expected, libcommon.Hash(root))
}
