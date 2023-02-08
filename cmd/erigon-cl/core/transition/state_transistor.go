package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// StateTransistor takes care of state transition
type StateTransistor struct {
	state         *state.BeaconState
	beaconConfig  *clparams.BeaconChainConfig
	genesisConfig *clparams.GenesisConfig
	noValidate    bool // Whether we want to do cryptography checks.
}

func New(state *state.BeaconState, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, noValidate bool) *StateTransistor {
	return &StateTransistor{
		state:         state,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
		noValidate:    noValidate,
	}
}
