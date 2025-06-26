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

package params

import (
	"embed"
	"encoding/json"
	"fmt"
	"github.com/erigontech/erigon-lib/common/empty"
	"math/big"
	"path"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/paths"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

//go:embed chainspecs
var chainspecs embed.FS

func readChainConfig(filename string) *chain.Config {
	f, err := chainspecs.Open(filename)
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

	if spec.BorJSON != nil {
		borConfig := &borcfg.BorConfig{}
		err = json.Unmarshal(spec.BorJSON, borConfig)
		if err != nil {
			panic(fmt.Sprintf("Could not parse 'bor' chainspec for %s: %v", filename, err))
		}
		spec.Bor = borConfig
	}

	return spec
}

type ChainSpec struct {
	Name             string      // normalized chain name, e.g. "mainnet", "sepolia", etc. Never empty.
	GenesisHash      common.Hash // block hash of the genesis block
	GenesisStateRoot common.Hash // state root of the genesis block
	Genesis          types.Genesis
	Config           *chain.Config
	Bootnodes        []string // list of bootnodes for the chain, if any
}

func (cs ChainSpec) IsEmpty() bool {
	return cs.Name == "" && cs.GenesisHash == (common.Hash{}) && cs.Config == nil && len(cs.Bootnodes) == 0
}

var supportedChains = map[string]ChainSpec{
	networkname.Mainnet: {
		Name:        networkname.Mainnet,
		GenesisHash: common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
		Config:      readChainConfig("chainspecs/mainnet.json"),
		Bootnodes:   MainnetBootnodes,
		Genesis:     MainnetGenesisBlock(),
	},
	networkname.Holesky: {
		Name:        networkname.Holesky,
		GenesisHash: common.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4"),
		Config:      readChainConfig("chainspecs/holesky.json"),
		Bootnodes:   HoleskyBootnodes,
		Genesis:     HoleskyGenesisBlock(),
	},
	networkname.Sepolia: {
		Name:        networkname.Sepolia,
		GenesisHash: common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9"),
		Config:      readChainConfig("chainspecs/sepolia.json"),
		Bootnodes:   SepoliaBootnodes,
		Genesis:     SepoliaGenesisBlock(),
	},
	networkname.Hoodi: {
		Name:        networkname.Hoodi,
		GenesisHash: common.HexToHash("0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b"),
		Config:      readChainConfig("chainspecs/hoodi.json"),
		Bootnodes:   HoodiBootnodes,
		Genesis:     HoodiGenesisBlock(),
	},
	networkname.Amoy: {
		Name:        networkname.Amoy,
		GenesisHash: common.HexToHash("0x7202b2b53c5a0836e773e319d18922cc756dd67432f9a1f65352b61f4406c697"),
		Config:      readChainConfig("chainspecs/amoy.json"),
		Genesis:     AmoyGenesisBlock(),
		Bootnodes:   AmoyBootnodes,
	},
	networkname.BorMainnet: {
		Name:        networkname.BorMainnet,
		GenesisHash: common.HexToHash("0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b"),
		Config:      readChainConfig("chainspecs/bor-mainnet.json"),
		Bootnodes:   BorMainnetBootnodes,
		Genesis:     BorMainnetGenesisBlock(),
	},
	networkname.BorDevnet: {
		Name:        networkname.BorDevnet,
		GenesisHash: common.HexToHash("0x5a06b25b0c6530708ea0b98a3409290e39dce6be7f558493aeb6e4b99a172a87"),
		Config:      readChainConfig("chainspecs/bor-devnet.json"),
		//Bootnodes:   BorDevnetBootnodes,
		Genesis: BorDevnetGenesisBlock(),
	},
	networkname.Gnosis: {
		Name:             networkname.Gnosis,
		GenesisHash:      common.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756"),
		GenesisStateRoot: common.HexToHash("0x40cf4430ecaa733787d1a65154a3b9efb560c95d9e324a23b97f0609b539133b"),
		Config:           readChainConfig("chainspecs/gnosis.json"),
		Bootnodes:        GnosisBootnodes,
		Genesis:          GnosisGenesisBlock(),
	},
	networkname.Chiado: {
		Name:             networkname.Chiado,
		GenesisHash:      common.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a"),
		GenesisStateRoot: common.HexToHash("0x9ec3eaf4e6188dfbdd6ade76eaa88289b57c63c9a2cde8d35291d5a29e143d31"),
		Config:           readChainConfig("chainspecs/chiado.json"),
		Bootnodes:        ChiadoBootnodes,
		Genesis:          ChiadoGenesisBlock(),
	},
	networkname.Test: {
		Name:             networkname.Test,
		GenesisHash:      common.HexToHash("0x6116de25352c93149542e950162c7305f207bbc17b0eb725136b78c80aed79cc"),
		GenesisStateRoot: empty.RootHash,
		Config:           chain.TestChainConfig,
		//Bootnodes:   TestBootnodes,
		Genesis: TestGenesisBlock(),
	},

	"all-clique-protocol-changes": {
		// AllCliqueProtocolChanges contains every protocol change (EIPs) introduced
		// and accepted by the Ethereum core developers into the Clique consensus.
		Name:        "all-clique-protocol-changes",
		GenesisHash: empty.RootHash,
		Config: &chain.Config{
			ChainID:               big.NewInt(1337),
			Consensus:             chain.CliqueConsensus,
			HomesteadBlock:        big.NewInt(0),
			TangerineWhistleBlock: big.NewInt(0),
			SpuriousDragonBlock:   big.NewInt(0),
			ByzantiumBlock:        big.NewInt(0),
			ConstantinopleBlock:   big.NewInt(0),
			PetersburgBlock:       big.NewInt(0),
			IstanbulBlock:         big.NewInt(0),
			MuirGlacierBlock:      big.NewInt(0),
			BerlinBlock:           big.NewInt(0),
			LondonBlock:           big.NewInt(0),
			Clique:                &chain.CliqueConfig{Period: 0, Epoch: 30000},
		},
	},
}

// Filled by init()
var supportedChainGenesisHashes = map[common.Hash]ChainSpec{}

func init() {
	for _, spec := range supportedChains {
		if spec.IsEmpty() {
			continue
		}
		if spec.GenesisHash == (common.Hash{}) {
			panic(fmt.Sprintf("Genesis hash is not set for chain %s", spec.Name))
		}
		if spec.Config == nil {
			panic(fmt.Sprintf("Chain config is not set for chain %s", spec.Name))
		}
		//if spec.GenesisStateRoot == (common.Hash{}) {
		//	spec.GenesisStateRoot = empty.RootHash
		//}
		spec.Genesis.Config = spec.Config

		supportedChains[spec.Name] = spec
		supportedChainGenesisHashes[spec.GenesisHash] = spec
	}
}

// Genesis hashes to enforce below configs on.
var (
	MainnetGenesisHash    = common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	HoleskyGenesisHash    = common.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4")
	SepoliaGenesisHash    = common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9")
	HoodiGenesisHash      = common.HexToHash("0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b")
	AmoyGenesisHash       = common.HexToHash("0x7202b2b53c5a0836e773e319d18922cc756dd67432f9a1f65352b61f4406c697")
	BorMainnetGenesisHash = common.HexToHash("0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b")
	BorDevnetGenesisHash  = common.HexToHash("0x5a06b25b0c6530708ea0b98a3409290e39dce6be7f558493aeb6e4b99a172a87")
	GnosisGenesisHash     = common.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756")
	ChiadoGenesisHash     = common.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a")
	TestGenesisHash       = common.HexToHash("0x6116de25352c93149542e950162c7305f207bbc17b0eb725136b78c80aed79cc")
)

var (
	// MainnetChainConfig is the chain parameters to run a node on the main network.
	MainnetChainConfig = readChainConfig("chainspecs/mainnet.json")

	// HoleskyChainConfi contains the chain parameters to run a node on the Holesky test network.
	HoleskyChainConfig = readChainConfig("chainspecs/holesky.json")

	// SepoliaChainConfig contains the chain parameters to run a node on the Sepolia test network.
	SepoliaChainConfig = readChainConfig("chainspecs/sepolia.json")

	// HoodiChainConfig contains the chain parameters to run a node on the Hoodi test network.
	HoodiChainConfig = readChainConfig("chainspecs/hoodi.json")

	// AllCliqueProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the Clique consensus.
	AllCliqueProtocolChanges = &chain.Config{
		ChainID:               big.NewInt(1337),
		Consensus:             chain.CliqueConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		Clique:                &chain.CliqueConfig{Period: 0, Epoch: 30000},
	}

	AmoyChainConfig = readChainConfig("chainspecs/amoy.json")

	BorMainnetChainConfig = readChainConfig("chainspecs/bor-mainnet.json")

	BorDevnetChainConfig = readChainConfig("chainspecs/bor-devnet.json")

	GnosisChainConfig = readChainConfig("chainspecs/gnosis.json")

	ChiadoChainConfig = readChainConfig("chainspecs/chiado.json")

	CliqueSnapshot = NewConsensusSnapshotConfig(10, 1024, 16384, true, "")
)

type ConsensusSnapshotConfig struct {
	CheckpointInterval uint64 // Number of blocks after which to save the vote snapshot to the database
	InmemorySnapshots  int    // Number of recent vote snapshots to keep in memory
	InmemorySignatures int    // Number of recent block signatures to keep in memory
	DBPath             string
	InMemory           bool
}

const cliquePath = "clique"

func NewConsensusSnapshotConfig(checkpointInterval uint64, inmemorySnapshots int, inmemorySignatures int, inmemory bool, dbPath string) *ConsensusSnapshotConfig {
	if len(dbPath) == 0 {
		dbPath = paths.DefaultDataDir()
	}

	return &ConsensusSnapshotConfig{
		checkpointInterval,
		inmemorySnapshots,
		inmemorySignatures,
		path.Join(dbPath, cliquePath),
		inmemory,
	}
}

func ChainSpecByName(chainName string) ChainSpec {
	spec, ok := supportedChains[chainName]
	if !ok || spec.IsEmpty() {
		return ChainSpec{}
	}
	return spec
}

func ChainSpecByGenesisHash(genesisHash common.Hash) ChainSpec {
	spec, ok := supportedChainGenesisHashes[genesisHash]
	if !ok || spec.IsEmpty() {
		return ChainSpec{}
	}
	return spec
}

func NetworkIDByChainName(chain string) uint64 {
	spec := supportedChains[chain]
	if spec.IsEmpty() {
		return 0
	}
	return spec.Config.ChainID.Uint64()
}

func IsChainPoS(chainConfig *chain.Config, currentTDProvider func() *big.Int) bool {
	return isChainIDPoS(chainConfig.ChainID) || hasChainPassedTerminalTD(chainConfig, currentTDProvider)
}

func isChainIDPoS(chainID *big.Int) bool {
	ids := []*big.Int{
		MainnetChainConfig.ChainID,
		HoleskyChainConfig.ChainID,
		SepoliaChainConfig.ChainID,
		HoodiChainConfig.ChainID,
		GnosisChainConfig.ChainID,
		ChiadoChainConfig.ChainID,
	}
	for _, id := range ids {
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
