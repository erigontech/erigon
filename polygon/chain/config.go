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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chainspec"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

//go:embed chainspecs
var chainspecs embed.FS

func readChainSpec(filename string) *chain.Config {
	spec := chainspec.ReadChainSpec(chainspecs, filename)
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
	AmoyGenesisHash       = common.HexToHash("0x7202b2b53c5a0836e773e319d18922cc756dd67432f9a1f65352b61f4406c697")
	BorMainnetGenesisHash = common.HexToHash("0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b")
	BorDevnetGenesisHash  = common.HexToHash("0x5a06b25b0c6530708ea0b98a3409290e39dce6be7f558493aeb6e4b99a172a87")

	AmoyChainConfig       = readChainSpec("chainspecs/amoy.json")
	BorMainnetChainConfig = readChainSpec("chainspecs/bor-mainnet.json")
	BorDevnetChainConfig  = readChainSpec("chainspecs/bor-devnet.json")
)

func init() {
	chainspec.RegisterChain(networkname.Amoy, AmoyChainConfig, AmoyGenesisBlock(), AmoyGenesisHash, AmoyBootnodes,
		"enrtree://AKUEZKN7PSKVNR65FZDHECMKOJQSGPARGTPPBI7WS2VUL4EGR6XPC@amoy.polygon-peers.io")
	chainspec.RegisterChain(networkname.BorDevnet, BorDevnetChainConfig, BorDevnetGenesisBlock(), BorDevnetGenesisHash, nil, "")
	chainspec.RegisterChain(networkname.BorMainnet, BorMainnetChainConfig, BorMainnetGenesisBlock(), BorMainnetGenesisHash, BorMainnetBootnodes,
		"enrtree://AKUEZKN7PSKVNR65FZDHECMKOJQSGPARGTPPBI7WS2VUL4EGR6XPC@pos.polygon-peers.io")
}
