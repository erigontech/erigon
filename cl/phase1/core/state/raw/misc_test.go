package raw

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func TestGetters(t *testing.T) {
	state := GetTestState()
	require.NotNil(t, state.BeaconConfig())
	valLength := state.ValidatorLength()
	require.Equal(t, state.balances.Length(), valLength)

	val, err := state.ValidatorBalance(0)
	require.NoError(t, err)
	require.Equal(t, val, uint64(0x3d5972c17))

	root, err := state.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x9f1620db18ee06b9cbdf1b7fa9658701063d2bd05d54b09780f6c0a074b4ce5f"))

	copied, err := state.Copy()
	require.NoError(t, err)

	root, err = copied.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x9f1620db18ee06b9cbdf1b7fa9658701063d2bd05d54b09780f6c0a074b4ce5f"))
}
