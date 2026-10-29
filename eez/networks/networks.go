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

// Chiado699169 is eez.dev, rollup 2 of a registry it shares on Chiado: the
// eez-rollupX v0.1 network that replaced Chiado699069 on 2026-10-28.
// Scanning starts at its registration.
var Chiado699169 = chainspec.Spec{
	Name:             "eez-chiado",
	GenesisHash:      common.HexToHash("0x283b471b65276c23867b5351fe1973fc9438310d865b6f833d1cd6666478ccb0"),
	GenesisStateRoot: common.HexToHash("0x2e09b0500a7fc5d147f7a4ec7f3fa8b447cb89b131585689f8625e0556fab321"),
	Genesis: mustRollupXGenesis(chiado699169Genesis, &config.L2Config{
		RollupID:            2,
		L1ChainID:           10200,
		Registry:            common.HexToAddress("0x6258189d3a04eb31a196a8931077dfe001a9f6fd"),
		RegistryDeployBlock: 23_731_532,
		BlockInterval:       1,
		L1BlockTime:         5,
		BlockGasLimit:       config.RollupXBlockGasLimit,
	}),
}

// Chiado916969 is eez.asuscomm.com, rollup 4 of a registry it shares on
// Chiado: the eez-rollupX v0.1 network that replaced Chiado906969 on
// 2026-10-28. Rollup 1 of the same registry used chain id 916969 before.
// Scanning starts at its registration.
var Chiado916969 = chainspec.Spec{
	Name:             "eez-chiado-asus",
	GenesisHash:      common.HexToHash("0xbb9e52d97581ec62134cdfd6a4c21330de6002557f66fb8bd49b3e2f800d80ae"),
	GenesisStateRoot: common.HexToHash("0x0b4679c7fa435b5f542d605212abb50165397164ae0aa43badf21e2fae7f8549"),
	Genesis: mustRollupXGenesis(chiado916969Genesis, &config.L2Config{
		RollupID:            4,
		L1ChainID:           10200,
		Registry:            common.HexToAddress("0x3b1d212f8f352c2d9334733aa31cf7546f15c239"),
		RegistryDeployBlock: 23_731_484,
		BlockInterval:       1,
		L1BlockTime:         5,
		BlockGasLimit:       config.RollupXBlockGasLimit,
	}),
}

// Chiado699069 is eez.dev until 2026-10-28, the eez-rollupX v0.0.1 network
// of fixture eezdev-699069. Its last batch settled in L1 block 23,723,620.
var Chiado699069 = chainspec.Spec{
	Name:             "eez-chiado-699069",
	GenesisHash:      common.HexToHash("0x05227a1af51a988231b625b276c6b29594f0a93090efd93463934cd1bf6e606e"),
	GenesisStateRoot: common.HexToHash("0x8255029550a16e602838f463ca67bfc560ff9841d576905e2412844de258c18e"),
	Genesis: mustRollupXGenesis(chiado699069Genesis, &config.L2Config{
		RollupID:            1,
		L1ChainID:           10200,
		Registry:            common.HexToAddress("0x96d6617629b90bdf275dd1d4888170ad5266fa33"),
		RegistryDeployBlock: 23_413_742,
		BlockInterval:       1,
		L1BlockTime:         5,
		BlockGasLimit:       config.RollupXBlockGasLimit,
	}),
}

// Chiado906969 is eez.asuscomm.com until 2026-10-28, the eez-rollupX v0.0.1
// network on Chiado that rehearsed protocol upgrades. Its last batch
// settled in L1 block 23,691,477.
var Chiado906969 = chainspec.Spec{
	Name:             "eez-chiado-906969",
	GenesisHash:      common.HexToHash("0x10c9ab15fb55a85ca101320e9bf842a67946704b36b98f5ae18ecac8ba8ffcd5"),
	GenesisStateRoot: common.HexToHash("0xc0082ef15873213ec8a9217c0bfd503c0058dd73422c8e318cc98625f0ebac5e"),
	Genesis: mustRollupXGenesis(chiado906969Genesis, &config.L2Config{
		RollupID:            1,
		L1ChainID:           10200,
		Registry:            common.HexToAddress("0x3c81284dae576d5152c9ca290668537618c44e5e"),
		RegistryDeployBlock: 23_412_936,
		BlockInterval:       1,
		L1BlockTime:         5,
		BlockGasLimit:       config.RollupXBlockGasLimit,
	}),
}

// Mainnet is mainnet.eez.dev, the eez-rollupX network on Ethereum. Its
// registry proxy got its code, and logged Upgraded and AdminChanged, in
// block 26,085,240.
var Mainnet = chainspec.Spec{
	Name:             "eez-mainnet",
	GenesisHash:      common.HexToHash("0xb8d94a6bdaee075d59df35f5577825944f7804dd8cb33c036832ed9daa1309e0"),
	GenesisStateRoot: common.HexToHash("0x162938959b86e35ccd215f51c57501ab39db5688dd3553fcb0586075e176eef0"),
	Genesis: mustRollupXGenesis(ethereum696990Genesis, &config.L2Config{
		RollupID:            1,
		L1ChainID:           1,
		Registry:            common.HexToAddress("0x4b2760f84b83d6b3829e30a7573b0172b6d6402c"),
		RegistryDeployBlock: 26_085_240,
		BlockInterval:       2,
		L1BlockTime:         12,
		BlockGasLimit:       config.RollupXBlockGasLimit,
	}),
}

// chiado699169Genesis and chiado916969Genesis are rebuilt from the state
// the networks' nodes served for block 0 on 2026-10-29: balance, nonce, code
// and storage of the accounts and slots of eez-rollupX's genesis.json at
// d0130ab931fcdb264d0822707fc1ef5889122f6f, with config.rollupX from
// debug_chainConfig and the header fields from block 0. No genesis file is
// published; the hashes are the initial block hashes of the networks'
// RollupCreated logs.
var (
	//go:embed chiado-699169-genesis.json
	chiado699169Genesis []byte
	//go:embed chiado-916969-genesis.json
	chiado916969Genesis []byte
)

// chiado699069Genesis is the genesis.json of fixture eezdev-699069,
// unchanged; eez/testdata/SOURCES.md records its source.
//
//go:embed chiado-699069-genesis.json
var chiado699069Genesis []byte

// chiado906969Genesis and ethereum696990Genesis are the deployments'
// genesis.json of eez-association/eez-rollupX at
// a50c326d99f7cd1c21406eedbf370a344015a6b5, unchanged.
var (
	//go:embed chiado-906969-genesis.json
	chiado906969Genesis []byte
	//go:embed ethereum-696990-genesis.json
	ethereum696990Genesis []byte
)

var all = []*chainspec.Spec{&Devnet, &Chiado699169, &Chiado916969, &Chiado699069, &Chiado906969, &Mainnet}

func init() {
	for _, spec := range all {
		spec.Config = spec.Genesis.Config
	}
}

var registerOnce sync.Once

// Register makes the EEZ networks available to --chain.
func Register() {
	registerOnce.Do(func() {
		for _, spec := range all {
			chainspec.RegisterChainSpec(spec.Name, *spec)
		}
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

// mustRollupXGenesis is mustGenesis for an eez-rollupX genesis, whose
// config.rollupX section is the l2 section's schedule and implies the 2/50
// EIP-1559 rule. A v0.1 section's fee collector is the chain's.
func mustRollupXGenesis(raw []byte, l2 *config.L2Config) *types.Genesis {
	var doc struct {
		Config struct {
			RollupX *config.RollupXConfig `json:"rollupX"`
		} `json:"config"`
	}
	if err := jsoniter.ConfigFastest.Unmarshal(raw, &doc); err != nil || doc.Config.RollupX == nil {
		panic(fmt.Sprintf("eez genesis: no config.rollupX section: %v", err))
	}
	l2.RollupX = doc.Config.RollupX
	g := mustGenesis(raw, l2)
	denominator := config.RollupXBaseFeeChangeDenominator
	g.Config.BaseFeeChangeDenominator = &denominator
	if collector := l2.RollupX.FeeCollector; collector != nil {
		g.Config.FeeCollector = new(*collector)
	}
	if err := l2.ValidateChain(g.Config); err != nil {
		panic(fmt.Sprintf("eez genesis: %v", err))
	}
	return g
}
