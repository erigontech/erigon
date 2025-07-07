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
	"fmt"
	"io/fs"
	"math/big"
	"path"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/paths"
	"github.com/erigontech/erigon-lib/types"
)

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

var (
	Mainnet = Spec{
		Name:        networkname.Mainnet,
		GenesisHash: common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
		Bootnodes:   MainnetBootnodes,
		Config:      ReadChainConfig(chainspecs, "chainspecs/mainnet.json"),
		Genesis:     MainnetGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.mainnet.ethdisco.net",
	}

	Holesky = Spec{
		Name:        networkname.Holesky,
		GenesisHash: common.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4"),
		Bootnodes:   HoleskyBootnodes,
		Config:      ReadChainConfig(chainspecs, "chainspecs/holesky.json"),
		Genesis:     HoleskyGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.holesky.ethdisco.net",
	}

	Sepolia = Spec{
		Name:        networkname.Sepolia,
		GenesisHash: common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9"),
		Bootnodes:   SepoliaBootnodes,
		Config:      ReadChainConfig(chainspecs, "chainspecs/sepolia.json"),
		Genesis:     SepoliaGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.sepolia.ethdisco.net",
	}

	Hoodi = Spec{
		Name:        networkname.Hoodi,
		GenesisHash: common.HexToHash("0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b"),
		Config:      ReadChainConfig(chainspecs, "chainspecs/hoodi.json"),
		Bootnodes:   HoodiBootnodes,
		Genesis:     HoodiGenesisBlock(),
		DNSNetwork:  dnsPrefix + "all.hoodi.ethdisco.net",
	}

	Gnosis = Spec{
		Name:             networkname.Gnosis,
		GenesisHash:      common.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756"),
		GenesisStateRoot: common.HexToHash("0x40cf4430ecaa733787d1a65154a3b9efb560c95d9e324a23b97f0609b539133b"),
		Config:           ReadChainConfig(chainspecs, "chainspecs/gnosis.json"),
		Bootnodes:        GnosisBootnodes,
		Genesis:          GnosisGenesisBlock(),
	}

	Chiado = Spec{
		Name:             networkname.Chiado,
		GenesisHash:      common.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a"),
		GenesisStateRoot: common.HexToHash("0x9ec3eaf4e6188dfbdd6ade76eaa88289b57c63c9a2cde8d35291d5a29e143d31"),
		Config:           ReadChainConfig(chainspecs, "chainspecs/chiado.json"),
		Bootnodes:        ChiadoBootnodes,
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

// Filled by init(); mapping of chain genesis hashes to chain specs.
var supportedChainGenesisHashes = map[common.Hash]Spec{}

// mapping of chain names to chain specs.
var supportedChains = map[string]Spec{
	//networkname.Mainnet: Mainnet,
	//networkname.Holesky: Holesky,
	//networkname.Sepolia: Sepolia,
	//networkname.Hoodi:   Hoodi,
	//networkname.Gnosis:  Gnosis,
	//networkname.Chiado:  Chiado,
	//networkname.Test:    Test,

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
		Genesis: &types.Genesis{},
	},
}

// Genesis hashes to enforce below configs on.
var (
	MainnetGenesisHash = common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	HoleskyGenesisHash = common.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4")
	SepoliaGenesisHash = common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9")
	HoodiGenesisHash   = common.HexToHash("0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b")
	GnosisGenesisHash  = common.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756")
	ChiadoGenesisHash  = common.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a")
)

var (
	// MainnetChainConfig is the chain parameters to run a node on the main network.
	MainnetChainConfig = ReadChainConfig(chainspecs, "chainspecs/mainnet.json")

	// HoleskyChainConfi contains the chain parameters to run a node on the Holesky test network.
	HoleskyChainConfig = ReadChainConfig(chainspecs, "chainspecs/holesky.json")

	// SepoliaChainConfig contains the chain parameters to run a node on the Sepolia test network.
	SepoliaChainConfig = ReadChainConfig(chainspecs, "chainspecs/sepolia.json")

	// HoodiChainConfig contains the chain parameters to run a node on the Hoodi test network.
	HoodiChainConfig = ReadChainConfig(chainspecs, "chainspecs/hoodi.json")

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

	GnosisChainConfig = ReadChainConfig(chainspecs, "chainspecs/gnosis.json")

	ChiadoChainConfig = ReadChainConfig(chainspecs, "chainspecs/chiado.json")

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

func NetworkIDByChainName(chain string) uint64 {
	spec := ChainSpecByName(chain)
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

func ChainSpecByName(chainName string) Spec {
	spec, ok := supportedChains[chainName]
	if !ok || spec.IsEmpty() {
		panic("spec not found for chain: " + chainName)
		//return Spec{}
	}
	return spec
}

func ChainSpecByGenesisHash(genesisHash common.Hash) Spec {
	spec, ok := supportedChainGenesisHashes[genesisHash]
	if !ok || spec.IsEmpty() {
		return Spec{}
	}
	return spec
}

func RegisterChainSpec(name string, spec Spec) {
	supportedChains[name] = spec
	NetworkNameByID[spec.Config.ChainID.Uint64()] = name

	if spec.GenesisHash != (common.Hash{}) {
		supportedChainGenesisHashes[spec.GenesisHash] = spec
	}
}

func init() {
	for _, spec := range supportedChains {
		if spec.IsEmpty() {
			continue
		}
		if spec.GenesisHash == (common.Hash{}) {
			panic("Genesis hash is not set for chain " + spec.Name)
		}
		if spec.Config == nil {
			panic("Chain config is not set for chain " + spec.Name)
		}
		if spec.GenesisStateRoot == (common.Hash{}) {
			spec.GenesisStateRoot = empty.RootHash
		}
		spec.Genesis.Config = spec.Config // for testing networks

		supportedChains[spec.Name] = spec
		supportedChainGenesisHashes[spec.GenesisHash] = spec
	}

	chainConfigByName[networkname.Dev] = AllCliqueProtocolChanges

	RegisterChainSpec(networkname.Mainnet, Mainnet)
	RegisterChainSpec(networkname.Sepolia, Sepolia)
	RegisterChainSpec(networkname.Hoodi, Hoodi)
	RegisterChainSpec(networkname.Holesky, Holesky)
	RegisterChainSpec(networkname.Gnosis, Gnosis)
	RegisterChainSpec(networkname.Chiado, Chiado)
	RegisterChainSpec(networkname.Test, Test)
}

var chainConfigByName = make(map[string]*chain.Config)

//networkname.Dev
//developer := cfg.Miner.Etherbase
//if developer == (common.Address{}) {
//Fatalf("Please specify developer account address using --miner.etherbase")
//}
//logger.Info("Using developer account", "address", developer)
//
//// Create a new developer genesis block or reuse existing one
//cfg.Genesis = chainspec.DeveloperGenesisBlock(uint64(ctx.Int(DeveloperPeriodFlag.Name)), developer)
//logger.Info("Using custom developer period", "seconds", cfg.Genesis.Config.Clique.Period)
