package genesisdb

import "github.com/erigontech/erigon/cl/phase1/core/state"

type GenesisDB interface {
	// Initialize initializes the genesis database, with either a given genesis state or the hardcoded databases.
	Initialize(state *state.CachingBeaconState) error

	IsInitialized() (bool, error)

	// ReadGenesisState returns the genesis state.
	ReadGenesisState() (*state.CachingBeaconState, error)
}
