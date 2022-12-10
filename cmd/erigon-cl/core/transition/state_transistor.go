package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// StateTransistor takes care of state transition
type StateTransistor struct {
	state         *cltypes.BeaconStateBellatrix
	beaconConfig  *clparams.BeaconChainConfig
	genesisConfig *clparams.GenesisConfig
}

func New(state *cltypes.BeaconStateBellatrix, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig) *StateTransistor {
	return &StateTransistor{
		state:         state,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
	}
}
