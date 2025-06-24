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

package chain

import (
	"embed"
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

//go:embed chainspecs
var chainspecs embed.FS

func readChainSpec(filename string) *chain.Config {
	spec := params.ReadChainSpec(chainspecs, filename)
	if spec.BorJSON != nil {
		borConfig := &borcfg.BorConfig{}
		if err := json.Unmarshal(spec.BorJSON, borConfig); err != nil {
			panic(fmt.Sprintf("Could not parse 'bor' chainspec for %s: %v", filename, err))
		}
		spec.Bor = borConfig
	}
	return spec
}

var (
	AmoyChainConfig       = readChainSpec("chainspecs/amoy.json")
	BorMainnetChainConfig = readChainSpec("chainspecs/bor-mainnet.json")
	BorDevnetChainConfig  = readChainSpec("chainspecs/bor-devnet.json")
)

func init() {
	params.RegisterChainConfigByName(networkname.Amoy, AmoyChainConfig)
	params.RegisterChainConfigByName(networkname.BorMainnet, BorMainnetChainConfig)
	params.RegisterChainConfigByName(networkname.BorDevnet, BorDevnetChainConfig)

	params.RegisterChainConfigByGenesisHash(params.AmoyGenesisHash, AmoyChainConfig)
	params.RegisterChainConfigByGenesisHash(params.BorMainnetGenesisHash, BorMainnetChainConfig)
	params.RegisterChainConfigByGenesisHash(params.BorDevnetGenesisHash, BorDevnetChainConfig)
}
