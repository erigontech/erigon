package genesisdb

import "github.com/erigontech/erigon/cl/phase1/core/state"

type GenesisDB interface {
	// Initialize initializes the genesis database, with either a given genesis state or the hardcoded databases.
	Initialize(state *state.CachingBeaconState) error

	// Reinitialize overwrites the stored genesis state. Initialize refuses once written; a dev chain whose
	// genesis time is set post-init needs the stored copy to carry the real time.
	Reinitialize(state *state.CachingBeaconState) error

	IsInitialized() (bool, error)

	// ReadGenesisState returns the genesis state.
	ReadGenesisState() (*state.CachingBeaconState, error)
}
