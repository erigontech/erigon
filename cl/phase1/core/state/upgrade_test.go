package state

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed tests/phase0.ssz_snappy
var stateEncoded []byte

func TestUpgradeAndExpectedWithdrawals(t *testing.T) {
	state := New(&clparams.MainnetBeaconConfig)
	utils.DecodeSSZSnappy(state, stateEncoded, int(clparams.Phase0Version))
	require.NoError(t, state.UpgradeToAltair())
	require.NoError(t, state.UpgradeToBellatrix())
	require.NoError(t, state.UpgradeToCapella())
	require.NoError(t, state.UpgradeToDeneb())
	// now WITHDRAWAAALLLLSSSS
	w := ExpectedWithdrawals(state.BeaconState)
	assert.Empty(t, w)

}
