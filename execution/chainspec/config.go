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
	"github.com/erigontech/erigon-lib/common/paths"
	"github.com/erigontech/erigon-lib/types"
)

//go:embed chainspecs
var chainspecs embed.FS

func ReadChainSpec(fileSys fs.FS, filename string) *chain.Config {
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

// Genesis hashes to enforce below configs on.
var (
	MainnetGenesisHash = common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	HoleskyGenesisHash = common.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4")
	SepoliaGenesisHash = common.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9")
	HoodiGenesisHash   = common.HexToHash("0xbbe312868b376a3001692a646dd2d7d1e4406380dfd86b98aa8a34d1557c971b")
	GnosisGenesisHash  = common.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756")
	ChiadoGenesisHash  = common.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a")
	TestGenesisHash    = common.HexToHash("0x6116de25352c93149542e950162c7305f207bbc17b0eb725136b78c80aed79cc")
)

var (
	GnosisGenesisStateRoot = common.HexToHash("0x40cf4430ecaa733787d1a65154a3b9efb560c95d9e324a23b97f0609b539133b")
	ChiadoGenesisStateRoot = common.HexToHash("0x9ec3eaf4e6188dfbdd6ade76eaa88289b57c63c9a2cde8d35291d5a29e143d31")
	TestGenesisStateRoot   = common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
)

var (
	// MainnetChainConfig is the chain parameters to run a node on the main network.
	MainnetChainConfig = ReadChainSpec(chainspecs, "chainspecs/mainnet.json")

	// HoleskyChainConfi contains the chain parameters to run a node on the Holesky test network.
	HoleskyChainConfig = ReadChainSpec(chainspecs, "chainspecs/holesky.json")

	// SepoliaChainConfig contains the chain parameters to run a node on the Sepolia test network.
	SepoliaChainConfig = ReadChainSpec(chainspecs, "chainspecs/sepolia.json")

	// HoodiChainConfig contains the chain parameters to run a node on the Hoodi test network.
	HoodiChainConfig = ReadChainSpec(chainspecs, "chainspecs/hoodi.json")

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

	GnosisChainConfig = ReadChainSpec(chainspecs, "chainspecs/gnosis.json")

	ChiadoChainConfig = ReadChainSpec(chainspecs, "chainspecs/chiado.json")

	CliqueSnapshot = NewSnapshotConfig(10, 1024, 16384, true, "")
)

type ConsensusSnapshotConfig struct {
	CheckpointInterval uint64 // Number of blocks after which to save the vote snapshot to the database
	InmemorySnapshots  int    // Number of recent vote snapshots to keep in memory
	InmemorySignatures int    // Number of recent block signatures to keep in memory
	DBPath             string
	InMemory           bool
}

const cliquePath = "clique"

func NewSnapshotConfig(checkpointInterval uint64, inmemorySnapshots int, inmemorySignatures int, inmemory bool, dbPath string) *ConsensusSnapshotConfig {
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

var genesisHashByChainName = make(map[string]*common.Hash)
var chainConfigByName = make(map[string]*chain.Config)
var chainConfigByGenesisHash = make(map[common.Hash]*chain.Config)

func ChainConfigByChainName(chainName string) *chain.Config {
	return chainConfigByName[chainName]
}

func GenesisHashByChainName(chain string) *common.Hash {
	return genesisHashByChainName[chain]
}

func ChainConfigByGenesisHash(genesisHash common.Hash) *chain.Config {
	return chainConfigByGenesisHash[genesisHash]
}

func NetworkIDByChainName(chain string) uint64 {
	config := ChainConfigByChainName(chain)
	if config == nil {
		return 0
	}
	return config.ChainID.Uint64()
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

func RegisterChain(name string, config *chain.Config, genesis *types.Genesis, genesisHash common.Hash, bootNodes []string) {
	NetworkNameByID[config.ChainID.Uint64()] = name
	chainConfigByName[name] = config
	chainConfigByGenesisHash[genesisHash] = config
	genesisHashByChainName[name] = &genesisHash
	genesisBlockByChainName[name] = genesis
	bootNodeURLsByChainName[name] = bootNodes
	bootNodeURLsByGenesisHash[genesisHash] = bootNodes
}

func init() {
	chainConfigByName[networkname.Dev] = AllCliqueProtocolChanges

	RegisterChain(networkname.Mainnet, MainnetChainConfig, MainnetGenesisBlock(), MainnetGenesisHash, MainnetBootnodes)
	RegisterChain(networkname.Sepolia, SepoliaChainConfig, SepoliaGenesisBlock(), SepoliaGenesisHash, SepoliaBootnodes)
	RegisterChain(networkname.Holesky, HoleskyChainConfig, HoleskyGenesisBlock(), HoleskyGenesisHash, HoleskyBootnodes)
	RegisterChain(networkname.Hoodi, HoodiChainConfig, HoodiGenesisBlock(), HoodiGenesisHash, HoodiBootnodes)
	RegisterChain(networkname.Gnosis, GnosisChainConfig, GnosisGenesisBlock(), GnosisGenesisHash, GnosisBootnodes)
	RegisterChain(networkname.Chiado, ChiadoChainConfig, ChiadoGenesisBlock(), ChiadoGenesisHash, ChiadoBootnodes)
	RegisterChain(networkname.Test, chain.TestChainConfig, TestGenesisBlock(), TestGenesisHash, nil)
}
