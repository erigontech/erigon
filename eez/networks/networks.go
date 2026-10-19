// Copyright 2026 The Erigon Authors
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
// Package networks registers the chain specs of the EEZ networks.
package networks

import (
	_ "embed"
	"fmt"
	"sync"

	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
)

// Devnet is the Kurtosis devnet of eez-rollup0: its genesis.json, with the
// registry a fresh enclave deploys. Deployments rewrite the genesis
// timestamp, so their genesis hash differs from this spec's.
var Devnet = chainspec.Spec{
	Name:             "eez-devnet",
	GenesisHash:      common.HexToHash("0x451fe028e5717390ee41d96085c09f2715d10a15f0210f8944737a18f45a5b2c"),
	GenesisStateRoot: common.HexToHash("0xb015fd4359695a92c09e587870e1c04920dcf308d4e6649259de5596c77de75e"),
	Genesis: mustGenesis(devnetGenesis, &config.L2Config{
		RollupID:            1,
		L1ChainID:           7331,
		Registry:            common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3"),
		RegistryDeployBlock: 12,
		BlockInterval:       2,
		L1BlockTime:         12,
		BlockGasLimit:       30_000_000,
	}),
}

// devnetGenesis is genesis.json of eez-association/eez-rollup0 at
// a270a06a92131d1987d49903a4174eef04ce959c, unchanged.
//
//go:embed devnet-genesis.json
var devnetGenesis []byte

func init() {
	Devnet.Config = Devnet.Genesis.Config
}

var registerOnce sync.Once

// Register makes the EEZ networks available to --chain.
func Register() {
	registerOnce.Do(func() {
		chainspec.RegisterChainSpec(Devnet.Name, Devnet)
	})
}

func mustGenesis(raw []byte, l2 *config.L2Config) *types.Genesis {
	var g types.Genesis
	if err := jsoniter.ConfigFastest.Unmarshal(raw, &g); err != nil {
		panic(fmt.Sprintf("eez genesis: %v", err))
	}
	if err := l2.Validate(); err != nil {
		panic(fmt.Sprintf("eez genesis: %v", err))
	}
	g.Config.L2 = l2
	return &g
}
