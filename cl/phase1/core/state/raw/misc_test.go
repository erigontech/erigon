package raw

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func TestGetters(t *testing.T) {
	t.Skip("Need to update due to data_gas_used")
	state := GetTestState()
	require.NotNil(t, state.BeaconConfig())
	valLength := state.ValidatorLength()
	require.Equal(t, state.balances.Length(), valLength)

	val, err := state.ValidatorBalance(0)
	require.NoError(t, err)
	require.Equal(t, val, uint64(0x3d5972c17))

	root, err := state.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0xa33e9a962fe153b2ed0174d30d5c657b9eaca2fba666df40e31ce8cf1a988961"))

	copied, err := state.Copy()
	require.NoError(t, err)

	root, err = copied.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0xa33e9a962fe153b2ed0174d30d5c657b9eaca2fba666df40e31ce8cf1a988961"))
}
