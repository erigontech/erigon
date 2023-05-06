package initial_state

import (
	_ "embed"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

//go:embed mainnet.state.ssz
var mainnetStateSSZ []byte

//go:embed sepolia.state.ssz
var sepoliaStateSSZ []byte

//go:embed goerli.state.ssz
var goerliStateSSZ []byte

// Return genesis state
func GetGenesisState(network clparams.NetworkType) (*state.BeaconState, error) {
	_, _, config := clparams.GetConfigsByNetwork(network)
	returnState := state.New(config)

	switch network {
	case clparams.MainnetNetwork:
		if err := returnState.DecodeSSZ(mainnetStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case clparams.GoerliNetwork:
		if err := returnState.DecodeSSZ(goerliStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case clparams.SepoliaNetwork:
		if err := returnState.DecodeSSZ(sepoliaStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported network for genesis fetching")
	}
	return returnState, nil
}
