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
	"context"
	_ "embed"
	"fmt"
	"io"
	"net/http"

	"github.com/klauspost/compress/zstd"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func downloadGenesisState(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to download genesis state: %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}

//go:embed mainnet.state.ssz.zst
var mainnetStateCompressed []byte

//go:embed sepolia.state.ssz.zst
var sepoliaStateCompressed []byte

//go:embed gnosis.state.ssz.zst
var gnosisStateCompressed []byte

//go:embed chiado.state.ssz.zst
var chiadoStateCompressed []byte

//go:embed bloatnet.state.ssz.zst
var bloatnetStateCompressed []byte

// Return genesis state
func GetGenesisState(ctx context.Context, network clparams.NetworkType) (*state.CachingBeaconState, error) {
	_, config := clparams.GetConfigsByNetwork(network)
	returnState := state.New(config)

	var compressed []byte
	version := clparams.Phase0Version
	switch network {
	case chainspec.MainnetChainID:
		compressed = mainnetStateCompressed
	case chainspec.SepoliaChainID:
		compressed = sepoliaStateCompressed
	case chainspec.GnosisChainID:
		compressed = gnosisStateCompressed
	case chainspec.ChiadoChainID:
		compressed = chiadoStateCompressed
	case chainspec.HoodiChainID:
		// Download genesis state by wget the url
		encodedState, err := downloadGenesisState(ctx, "https://github.com/eth-clients/hoodi/raw/main/metadata/genesis.ssz")
		if err != nil {
			return nil, err
		}
		if err := returnState.DecodeSSZ(encodedState, int(clparams.DenebVersion)); err != nil {
			return nil, err
		}
		return returnState, nil
	case chainspec.BloatnetNetworkID:
		compressed, version = bloatnetStateCompressed, clparams.ElectraVersion
	default:
		return nil, nil
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	encodedState, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}
	if err := returnState.DecodeSSZ(encodedState, int(version)); err != nil {
		return nil, err
	}
	return returnState, nil
}

func IsGenesisStateSupported(network clparams.NetworkType) bool {
	return network == chainspec.MainnetChainID ||
		network == chainspec.SepoliaChainID ||
		network == chainspec.GnosisChainID ||
		network == chainspec.ChiadoChainID ||
		network == chainspec.HoodiChainID ||
		network == chainspec.BloatnetNetworkID
}
