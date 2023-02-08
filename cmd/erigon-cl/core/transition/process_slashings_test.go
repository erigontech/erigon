package transition_test

import (
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessSlashingsNoSlash(t *testing.T) {
	base := state.GetEmptyBeaconStateWithVersion(clparams.AltairVersion)
	base.AddValidator(&cltypes.Validator{
		Slashed: true,
	}, clparams.MainnetBeaconConfig.MaxEffectiveBalance)
	base.SetSlashingSegmentAt(0, 0)
	base.SetSlashingSegmentAt(1, 1e9)
	s := transition.New(base, &clparams.MainnetBeaconConfig, nil, false)
	require.NoError(t, s.ProcessSlashings())
	wanted := clparams.MainnetBeaconConfig.MaxEffectiveBalance
	require.Equal(t, wanted, base.Balances()[0], "Unexpected slashed balance")
}

func getTestStateSlashings1() *state.BeaconState {
	state := state.GetEmptyBeaconStateWithVersion(clparams.AltairVersion)
	state.AddValidator(&cltypes.Validator{Slashed: true,
		WithdrawableEpoch: clparams.MainnetBeaconConfig.EpochsPerSlashingsVector / 2,
		EffectiveBalance:  clparams.MainnetBeaconConfig.MaxEffectiveBalance}, clparams.MainnetBeaconConfig.MaxEffectiveBalance)
	state.AddValidator(&cltypes.Validator{ExitEpoch: clparams.MainnetBeaconConfig.FarFutureEpoch, EffectiveBalance: clparams.MainnetBeaconConfig.MaxEffectiveBalance}, clparams.MainnetBeaconConfig.MaxEffectiveBalance)
	state.SetBalances([]uint64{clparams.MainnetBeaconConfig.MaxEffectiveBalance, clparams.MainnetBeaconConfig.MaxEffectiveBalance})
	state.SetSlashingSegmentAt(0, 0)
	state.SetSlashingSegmentAt(1, 1e9)
	return state
}

func getTestStateSlashings2() *state.BeaconState {
	state := getTestStateSlashings1()
	state.AddValidator(&cltypes.Validator{ExitEpoch: clparams.MainnetBeaconConfig.FarFutureEpoch, EffectiveBalance: clparams.MainnetBeaconConfig.MaxEffectiveBalance}, clparams.MainnetBeaconConfig.MaxEffectiveBalance)
	return state
}

func getTestStateSlashings3() *state.BeaconState {
	state := getTestStateSlashings2()
	state.SetSlashingSegmentAt(1, 2*1e9)
	return state
}

func getTestStateSlashings4() *state.BeaconState {
	state := getTestStateSlashings1()
	state.SetValidatorAt(1, &cltypes.Validator{ExitEpoch: clparams.MainnetBeaconConfig.FarFutureEpoch, EffectiveBalance: clparams.MainnetBeaconConfig.MaxEffectiveBalance - clparams.MainnetBeaconConfig.EffectiveBalanceIncrement})
	state.SetBalances([]uint64{clparams.MainnetBeaconConfig.MaxEffectiveBalance - clparams.MainnetBeaconConfig.EffectiveBalanceIncrement, clparams.MainnetBeaconConfig.MaxEffectiveBalance - clparams.MainnetBeaconConfig.EffectiveBalanceIncrement})
	return state
}

func TestProcessSlashingsSlash(t *testing.T) {
	tests := []struct {
		state *state.BeaconState
		want  uint64
	}{
		{
			state: getTestStateSlashings1(),
			want:  uint64(30000000000),
		},
		{
			state: getTestStateSlashings2(),
			want:  uint64(31000000000),
		},
		{
			state: getTestStateSlashings3(),
			want:  uint64(30000000000),
		},
		{
			state: getTestStateSlashings4(),
			want:  uint64(29000000000),
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			transitistor := transition.New(tt.state, &clparams.MainnetBeaconConfig, nil, false)
			require.NoError(t, transitistor.ProcessSlashings())
			assert.Equal(t, tt.want, tt.state.Balances()[0])
		})
	}
}

/*

func TestProcessSlashings_SlashedLess(t *testing.T) {

}
*/
