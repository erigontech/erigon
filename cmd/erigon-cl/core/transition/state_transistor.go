package transition

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// StateTransistor takes care of state transition
type StateTransistor struct {
	state         *state.BeaconState
	beaconConfig  *clparams.BeaconChainConfig
	genesisConfig *clparams.GenesisConfig
	noValidate    bool // Whether we want to do cryptography checks.
	// stateRootsCache caches slot => stateRoot
	stateRootsCache *lru.Cache
}

const stateRootsCacheSize = 256

func New(state *state.BeaconState, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, noValidate bool) *StateTransistor {
	stateRootsCache, err := lru.New(stateRootsCacheSize)
	if err != nil {
		panic(err)
	}
	return &StateTransistor{
		state:           state,
		beaconConfig:    beaconConfig,
		genesisConfig:   genesisConfig,
		noValidate:      noValidate,
		stateRootsCache: stateRootsCache,
	}
}
