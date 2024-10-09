package initial_state

import (
	_ "embed"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

//go:embed mainnet.state.ssz
var mainnetStateSSZ []byte

//go:embed sepolia.state.ssz
var sepoliaStateSSZ []byte

// Return genesis state
func GetGenesisState(network clparams.NetworkType) (*state.CachingBeaconState, error) {
	_, config := clparams.GetConfigsByNetwork(network)
	returnState := state.New(config)

	switch network {
	case clparams.MainnetNetwork:
		if err := returnState.DecodeSSZ(mainnetStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case clparams.SepoliaNetwork:
		if err := returnState.DecodeSSZ(sepoliaStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case clparams.GoerliNetwork:
		return nil, nil
	}
	return returnState, nil
}

func IsGenesisStateSupported(network clparams.NetworkType) bool {
	switch network {
	case clparams.MainnetNetwork:
		return true
	case clparams.SepoliaNetwork:
		return true
	default:
		return false
	}
}
