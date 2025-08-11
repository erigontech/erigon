// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package initial_state

import (
	_ "embed"
	"fmt"
	"io"
	"net/http"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func downloadGenesisState(url string) ([]byte, error) {
	// Download genesis state by wget the url. MUST NOT RETURN NIL thorugh GET request. use go stnadard library
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to download genesis state: %s", resp.Status)
	}
	return io.ReadAll(resp.Body)

}

//go:embed mainnet.state.ssz
var mainnetStateSSZ []byte

//go:embed sepolia.state.ssz
var sepoliaStateSSZ []byte

//go:embed gnosis.state.ssz
var gnosisStateSSZ []byte

//go:embed chiado.state.ssz
var chiadoStateSSZ []byte

// Return genesis state
func GetGenesisState(network clparams.NetworkType) (*state.CachingBeaconState, error) {
	_, config := clparams.GetConfigsByNetwork(network)
	returnState := state.New(config)

	switch network {
	case chainspec.MainnetChainID:
		if err := returnState.DecodeSSZ(mainnetStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case chainspec.SepoliaChainID:
		if err := returnState.DecodeSSZ(sepoliaStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case chainspec.GnosisChainID:
		if err := returnState.DecodeSSZ(gnosisStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case chainspec.ChiadoChainID:
		if err := returnState.DecodeSSZ(chiadoStateSSZ, int(clparams.Phase0Version)); err != nil {
			return nil, err
		}
	case chainspec.HoleskyChainID:
		// Download genesis state by wget the url
		encodedState, err := downloadGenesisState("https://github.com/eth-clients/holesky/raw/main/metadata/genesis.ssz")
		if err != nil {
			return nil, err
		}
		if err := returnState.DecodeSSZ(encodedState, int(clparams.BellatrixVersion)); err != nil {
			return nil, err
		}
	case chainspec.HoodiChainID:
		// Download genesis state by wget the url
		encodedState, err := downloadGenesisState("https://github.com/eth-clients/hoodi/raw/main/metadata/genesis.ssz")
		if err != nil {
			return nil, err
		}
		if err := returnState.DecodeSSZ(encodedState, int(clparams.DenebVersion)); err != nil {
			return nil, err
		}
	default:
		return nil, nil
	}
	return returnState, nil
}

func IsGenesisStateSupported(network clparams.NetworkType) bool {
	return network == chainspec.MainnetChainID || network == chainspec.SepoliaChainID || network == chainspec.GnosisChainID || network == chainspec.ChiadoChainID || network == chainspec.HoleskyChainID || network == chainspec.HoodiChainID
}
