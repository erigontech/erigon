// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package chainspec

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/types"
)

func init() {
	RegisterChainSpec(networkname.Mainnet, Mainnet)
	RegisterChainSpec(networkname.Sepolia, Sepolia)
	RegisterChainSpec(networkname.Hoodi, Hoodi)
	RegisterChainSpec(networkname.Holesky, Holesky)
	RegisterChainSpec(networkname.Gnosis, Gnosis)
	RegisterChainSpec(networkname.Chiado, Chiado)
	RegisterChainSpec(networkname.Test, Test)

	// verify registered chains
	for _, spec := range registeredChainsByName {
		if spec.IsEmpty() {
			panic("chain spec is empty for chain " + spec.Name)
		}
		if spec.GenesisHash == (common.Hash{}) {
			panic("genesis hash is not set for chain " + spec.Name)
		}
		if spec.Genesis == nil {
			panic("genesis is not set for chain " + spec.Name)
		}
		if spec.GenesisStateRoot == (common.Hash{}) {
			spec.GenesisStateRoot = empty.RootHash
		}

		if spec.Config == nil {
			panic("chain config is not set for chain " + spec.Name)
		}

		registeredChainsByName[spec.Name] = spec
		registeredChainsByGenesisHash[spec.GenesisHash] = spec
	}

	for _, name := range chainNamesPoS {
		s, err := ChainSpecByName(name)
		if err != nil {
			panic(fmt.Sprintf("chain %s is not registered: %v", name, err))
		}
		chainIdsPoS = append(chainIdsPoS, s.Config.ChainID)
	}
}

//go:embed chainspecs
var chainspecs embed.FS

func ReadChainConfig(fileSys fs.FS, filename string) *chain.Config {
	f, err := fileSys.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open chainspec for %s: %v", filename, err))
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	spec := &chain.Config{}
	err = decoder.Decode(&spec)
	if err != nil {
		panic(fmt.Sprintf("Could not parse chainspec for %s: %v", filename, err))
	}

	return spec
}

var ErrChainSpecUnknown = errors.New("unknown chain spec")

// ChainSpecByName returns the chain spec for the given chain name
func ChainSpecByName(chainName string) (Spec, error) {
	spec, ok := registeredChainsByName[chainName]
	if !ok || spec.IsEmpty() {
		return Spec{}, fmt.Errorf("%w with name %s", ErrChainSpecUnknown, chainName)
	}
	return spec, nil
}

// ChainSpecByGenesisHash returns the chain spec for the given genesis hash
func ChainSpecByGenesisHash(genesisHash common.Hash) (Spec, error) {
	spec, ok := registeredChainsByGenesisHash[genesisHash]
	if !ok || spec.IsEmpty() {
		return Spec{}, fmt.Errorf("%w with genesis %x", ErrChainSpecUnknown, genesisHash)
	}
	return spec, nil
}

// RegisterChainSpec registers a new chain spec with the given name and spec.
// If the name already exists, it will be overwritten.
func RegisterChainSpec(name string, spec Spec) {
	registeredChainsByName[name] = spec
	NetworkNameByID[spec.Config.ChainID.Uint64()] = name

	if spec.GenesisHash != (common.Hash{}) {
		registeredChainsByGenesisHash[spec.GenesisHash] = spec
	}
}

type Spec struct {
	Name             string      // normalized chain name, e.g. "mainnet", "sepolia", etc. Never empty.
	GenesisHash      common.Hash // block hash of the genesis block
	GenesisStateRoot common.Hash // state root of the genesis block
	Genesis          *types.Genesis
	Config           *chain.Config
	Bootnodes        []string // list of bootnodes for the chain, if any
	DNSNetwork       string   // address of a public DNS-based node list. See https://github.com/ethereum/discv4-dns-lists for more information.
}

func (cs Spec) IsEmpty() bool {
	return cs.Name == "" && cs.GenesisHash == (common.Hash{}) && cs.Config == nil && len(cs.Bootnodes) == 0
}

var ( // listings filled by init()
	// mapping of chain genesis hashes to chain specs.
	registeredChainsByGenesisHash = map[common.Hash]Spec{}

	// mapping of chain names to chain specs.
	registeredChainsByName = map[string]Spec{}

	// list of chain IDs that are considered Proof of Stake (PoS) chains
	chainIdsPoS = []*big.Int{}
)

var (
	Mainnet = Spec{
		Name:        networkname.Mainnet,
		GenesisHash: common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
		Bootnodes:   mainnetBootnodes,
		Config:      ReadChainConfig(chainspecs, "chainspecs/mainnet.json"),
		Genesis:     MainnetGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.mainnet.ethdisco.net",
	}

	Holesky = Spec{
		Name:        networkname.Holesky,
		GenesisHash: common.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4"),
		Bootnodes:   holeskyBootnodes,
		Config:      ReadChainConfig(chainspecs, "chainspecs/holesky.json"),
		Genesis:     HoleskyGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.holesky.ethdisco.net",
	}

	Sepolia = Spec{
		Name:        networkname.Sepolia,
		GenesisHash: common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9"),
		Bootnodes:   sepoliaBootnodes,
		Config:      ReadChainConfig(chainspecs, "chainspecs/sepolia.json"),
		Genesis:     SepoliaGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.sepolia.ethdisco.net",
	}

	Hoodi = Spec{
		Name:        networkname.Hoodi,
		GenesisHash: common.HexToHash("0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b"),
		Config:      ReadChainConfig(chainspecs, "chainspecs/hoodi.json"),
		Bootnodes:   hoodiBootnodes,
		Genesis:     HoodiGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.hoodi.ethdisco.net",
	}

	Gnosis = Spec{
		Name:             networkname.Gnosis,
		GenesisHash:      common.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756"),
		GenesisStateRoot: common.HexToHash("0x40cf4430ecaa733787d1a65154a3b9efb560c95d9e324a23b97f0609b539133b"),
		Config:           ReadChainConfig(chainspecs, "chainspecs/gnosis.json"),
		Bootnodes:        gnosisBootnodes,
		Genesis:          GnosisGenesisBlock(),
	}

	Chiado = Spec{
		Name:             networkname.Chiado,
		GenesisHash:      common.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a"),
		GenesisStateRoot: common.HexToHash("0x9ec3eaf4e6188dfbdd6ade76eaa88289b57c63c9a2cde8d35291d5a29e143d31"),
		Config:           ReadChainConfig(chainspecs, "chainspecs/chiado.json"),
		Bootnodes:        chiadoBootnodes,
		Genesis:          ChiadoGenesisBlock(),
	}

	Test = Spec{
		Name:             networkname.Test,
		GenesisHash:      common.HexToHash("0x6116de25352c93149542e950162c7305f207bbc17b0eb725136b78c80aed79cc"),
		GenesisStateRoot: empty.RootHash,
		Config:           chain.TestChainConfig,
		//Bootnodes:   TestBootnodes,
		Genesis: TestGenesisBlock(),
	}
)

var chainNamesPoS = []string{
	networkname.Mainnet,
	networkname.Holesky,
	networkname.Sepolia,
	networkname.Hoodi,
	networkname.Gnosis,
	networkname.Chiado,
}

func IsChainPoS(chainConfig *chain.Config, currentTDProvider func() *big.Int) bool {
	return isChainIDPoS(chainConfig.ChainID) || hasChainPassedTerminalTD(chainConfig, currentTDProvider)
}

func isChainIDPoS(chainID *big.Int) bool {
	for _, id := range chainIdsPoS {
		if id.Cmp(chainID) == 0 {
			return true
		}
	}
	return false
}

func hasChainPassedTerminalTD(chainConfig *chain.Config, currentTDProvider func() *big.Int) bool {
	if chainConfig.TerminalTotalDifficultyPassed {
		return true
	}

	terminalTD := chainConfig.TerminalTotalDifficulty
	if terminalTD == nil {
		return false
	}

	currentTD := currentTDProvider()
	return (currentTD != nil) && (terminalTD.Cmp(currentTD) <= 0)
}
