// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package params

import (
	"embed"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"

	"github.com/ledgerwatch/erigon/common/paths"
)

//go:embed chainspecs
var chainspecs embed.FS

func readChainSpec(filename string) *chain.Config {
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

// Genesis hashes to enforce below configs on.
var (
	MainnetGenesisHash                 = libcommon.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	HoleskyGenesisHash                 = libcommon.HexToHash("0xb5f7f912443c940f21fd611f12828d75b534364ed9e95ca4e307729a4661bde4")
	SepoliaGenesisHash                 = libcommon.HexToHash("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9")
	RinkebyGenesisHash                 = libcommon.HexToHash("0x6341fd3daf94b748c72ced5a5b26028f2474f5f00d824504e4fa37a75767e177")
	GoerliGenesisHash                  = libcommon.HexToHash("0xbf7e331f7f7c1dd2e05159666b3bf8bc7a8a3a9eb1d518969eab529dd9b88c1a")
	MumbaiGenesisHash                  = libcommon.HexToHash("0x7b66506a9ebdbf30d32b43c5f15a3b1216269a1ec3a75aa3182b86176a2b1ca7")
	AmoyGenesisHash                    = libcommon.HexToHash("0x7202b2b53c5a0836e773e319d18922cc756dd67432f9a1f65352b61f4406c697")
	BorMainnetGenesisHash              = libcommon.HexToHash("0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b")
	BorDevnetGenesisHash               = libcommon.HexToHash("0x5a06b25b0c6530708ea0b98a3409290e39dce6be7f558493aeb6e4b99a172a87")
	GnosisGenesisHash                  = libcommon.HexToHash("0x4f1dd23188aab3a76b463e4af801b52b1248ef073c648cbdc4c9333d3da79756")
	ChiadoGenesisHash                  = libcommon.HexToHash("0xada44fd8d2ecab8b08f256af07ad3e777f17fb434f8f8e678b312f576212ba9a")
	HermezMainnetGenesisHash           = libcommon.HexToHash("0x81005434635456a16f74ff7023fbe0bf423abbc8a8deb093ffff455c0ad3b741")
	HermezMainnetShadowforkGenesisHash = libcommon.HexToHash("0xe54709058a084845156393707161a7b3347859b1796167ca014354841f68373c")
	HermezLocalDevnetGenesisHash       = libcommon.HexToHash("0x532abde1baf4157008acf46f17c27624b54cab8e24922dac9ddb63da681e1848")
	HermezESTestGenesisHash            = libcommon.HexToHash("0x8c630b598fab24a99b59cdd8257f41b35d0aca992f13cd381c7591f5e89eec58")
	HermezCardonaGenesisHash           = libcommon.HexToHash("0x676c1a76a6c5855a32bdf7c61977a0d1510088a4eeac1330466453b3d08b60b9")
	HermezBaliGenesisHash              = libcommon.HexToHash("0x7311011ce6ab98ef0a15e44fe29f7680909588322534d1736361daa678543038")
	XLayerTestnetGenesisHash           = libcommon.HexToHash("0x22a8085892b367833bd7431fa5a90ff6b5d3769167cdaa29ce8571d07bc8f866")
	XLayerMainnetGenesisHash           = libcommon.HexToHash("0x11f32f605beb94a1acb783cb3b6da6d7975461ce3addf441e7ad60c2ec95e88f")
	HermezEtrogGenesisHash             = libcommon.HexToHash("0x5e14aefe391fafa040ee0a0fff6afbc1c230853b9684afb9363f3af081db0192")
)

var (
	GnosisGenesisStateRoot = libcommon.HexToHash("0x40cf4430ecaa733787d1a65154a3b9efb560c95d9e324a23b97f0609b539133b")
	ChiadoGenesisStateRoot = libcommon.HexToHash("0x9ec3eaf4e6188dfbdd6ade76eaa88289b57c63c9a2cde8d35291d5a29e143d31")
)

var (
	// MainnetChainConfig is the chain parameters to run a node on the main network.
	MainnetChainConfig = readChainSpec("chainspecs/mainnet.json")

	// HoleskyChainConfi contains the chain parameters to run a node on the Holesky test network.
	HoleskyChainConfig = readChainSpec("chainspecs/holesky.json")

	// SepoliaChainConfig contains the chain parameters to run a node on the Sepolia test network.
	SepoliaChainConfig = readChainSpec("chainspecs/sepolia.json")

	// GoerliChainConfig contains the chain parameters to run a node on the Görli test network.
	GoerliChainConfig = readChainSpec("chainspecs/goerli.json")

	// AllProtocolChanges contains every protocol change (EIPs) introduced
	// and accepted by the Ethereum core developers into the main net protocol.
	AllProtocolChanges = &chain.Config{
		ChainID:                       big.NewInt(1337),
		Consensus:                     chain.EtHashConsensus,
		HomesteadBlock:                big.NewInt(0),
		TangerineWhistleBlock:         big.NewInt(0),
		SpuriousDragonBlock:           big.NewInt(0),
		ByzantiumBlock:                big.NewInt(0),
		ConstantinopleBlock:           big.NewInt(0),
		PetersburgBlock:               big.NewInt(0),
		IstanbulBlock:                 big.NewInt(0),
		MuirGlacierBlock:              big.NewInt(0),
		BerlinBlock:                   big.NewInt(0),
		LondonBlock:                   big.NewInt(0),
		ArrowGlacierBlock:             big.NewInt(0),
		GrayGlacierBlock:              big.NewInt(0),
		TerminalTotalDifficulty:       big.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  big.NewInt(0),
		CancunTime:                    big.NewInt(0),
		Ethash:                        new(chain.EthashConfig),
	}

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

	MumbaiChainConfig = readChainSpec("chainspecs/mumbai.json")

	AmoyChainConfig = readChainSpec("chainspecs/amoy.json")

	BorMainnetChainConfig = readChainSpec("chainspecs/bor-mainnet.json")

	BorDevnetChainConfig = readChainSpec("chainspecs/bor-devnet.json")

	GnosisChainConfig = readChainSpec("chainspecs/gnosis.json")

	ChiadoChainConfig = readChainSpec("chainspecs/chiado.json")

	HermezMainnetChainConfig = readChainSpec("chainspecs/hermez.json")

	HermezMainnetShadowforkChainConfig = readChainSpec("chainspecs/hermez-shadowfork.json")

	HermezLocalDevnetChainConfig = readChainSpec("chainspecs/hermez-dev.json")

	HermezESTestChainConfig = readChainSpec("chainspecs/hermez-estest.json")

	HermezEtrogChainConfig = readChainSpec("chainspecs/hermez-etrog.json")

	HermezCardonaChainConfig = readChainSpec("chainspecs/hermez-cardona.json")

	HermezBaliChainConfig = readChainSpec("chainspecs/hermez-bali.json")

	XLayerTestnetChainConfig = readChainSpec("chainspecs/xlayer-testnet.json")

	XLayerMainnetChainConfig = readChainSpec("chainspecs/xlayer-mainnet.json")

	CliqueSnapshot = NewSnapshotConfig(10, 1024, 16384, true, "")

	TestChainConfig = &chain.Config{
		ChainID:               big.NewInt(1337),
		Consensus:             chain.EtHashConsensus,
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		Ethash:                new(chain.EthashConfig),
	}

	TestChainAuraConfig = &chain.Config{
		ChainID:               big.NewInt(1),
		Consensus:             chain.AuRaConsensus,
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
		Aura:                  &chain.AuRaConfig{},
	}

	TestRules = TestChainConfig.Rules(0, 0)

	DynamicChainConfigPath string
)

type ConsensusSnapshotConfig struct {
	CheckpointInterval uint64 // Number of blocks after which to save the vote snapshot to the database
	InmemorySnapshots  int    // Number of recent vote snapshots to keep in memory
	InmemorySignatures int    // Number of recent block signatures to keep in memory
	DBPath             string
	InMemory           bool
}

const cliquePath = "clique"

func DynamicChainConfig(ch string) *chain.Config {
	filename := path.Join(DynamicChainConfigPath, ch+"-chainspec.json")

	f, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("could not open chainspec for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	spec := &chain.Config{}
	err = decoder.Decode(&spec)
	if err != nil {
		panic(fmt.Sprintf("could not parse chainspec for %s: %v", filename, err))
	}

	chainId := spec.ChainID.Uint64()
	chain.SetDynamicChainDetails(chainId, spec.ChainName)

	return spec
}

func DynamicChainTimestamp(ch string) uint64 {
	filename := path.Join(DynamicChainConfigPath, ch+"-conf.json")

	f, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("could not open timestamp for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	type confFile struct {
		Timestamp uint64 `json:"timestamp"`
	}
	var conf confFile
	err = decoder.Decode(&conf)
	if err != nil {
		panic(fmt.Sprintf("could not parse timestamp for %s: %v", filename, err))
	}

	return conf.Timestamp

}

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

func ChainConfigByChainName(chain string) *chain.Config {
	switch chain {
	case networkname.MainnetChainName:
		return MainnetChainConfig
	case networkname.DevChainName:
		return AllCliqueProtocolChanges
	case networkname.HoleskyChainName:
		return HoleskyChainConfig
	case networkname.SepoliaChainName:
		return SepoliaChainConfig
	case networkname.GoerliChainName:
		return GoerliChainConfig
	case networkname.MumbaiChainName:
		return MumbaiChainConfig
	case networkname.AmoyChainName:
		return AmoyChainConfig
	case networkname.BorMainnetChainName:
		return BorMainnetChainConfig
	case networkname.BorDevnetChainName:
		return BorDevnetChainConfig
	case networkname.GnosisChainName:
		return GnosisChainConfig
	case networkname.ChiadoChainName:
		return ChiadoChainConfig
	case networkname.HermezMainnetChainName:
		return HermezMainnetChainConfig
	case networkname.HermezMainnetShadowforkChainName:
		return HermezMainnetShadowforkChainConfig
	case networkname.HermezLocalDevnetChainName:
		return HermezLocalDevnetChainConfig
	case networkname.HermezESTestChainName:
		return HermezESTestChainConfig
	case networkname.HermezEtrogChainName:
		return HermezEtrogChainConfig
	case networkname.HermezCardonaChainName:
		return HermezCardonaChainConfig
	case networkname.HermezBaliChainName:
		return HermezBaliChainConfig
	case networkname.XLayerTestnetChainName:
		return XLayerTestnetChainConfig
	case networkname.XLayerMainnetChainName:
		return XLayerMainnetChainConfig
	default:
		return DynamicChainConfig(chain)
	}
}

func GenesisHashByChainName(chain string) *libcommon.Hash {
	switch chain {
	case networkname.MainnetChainName:
		return &MainnetGenesisHash
	case networkname.HoleskyChainName:
		return &HoleskyGenesisHash
	case networkname.SepoliaChainName:
		return &SepoliaGenesisHash
	case networkname.GoerliChainName:
		return &GoerliGenesisHash
	case networkname.MumbaiChainName:
		return &MumbaiGenesisHash
	case networkname.AmoyChainName:
		return &AmoyGenesisHash
	case networkname.BorMainnetChainName:
		return &BorMainnetGenesisHash
	case networkname.BorDevnetChainName:
		return &BorDevnetGenesisHash
	case networkname.GnosisChainName:
		return &GnosisGenesisHash
	case networkname.ChiadoChainName:
		return &ChiadoGenesisHash
	case networkname.HermezMainnetChainName:
		return &HermezMainnetGenesisHash
	case networkname.HermezMainnetShadowforkChainName:
		return &HermezMainnetShadowforkGenesisHash
	case networkname.HermezLocalDevnetChainName:
		return &HermezLocalDevnetGenesisHash
	case networkname.HermezESTestChainName:
		return &HermezESTestGenesisHash
	case networkname.HermezEtrogChainName:
		return &HermezEtrogGenesisHash
	case networkname.HermezCardonaChainName:
		return &HermezCardonaGenesisHash
	case networkname.HermezBaliChainName:
		return &HermezBaliGenesisHash
	case networkname.XLayerTestnetChainName:
		return &XLayerTestnetGenesisHash
	case networkname.XLayerMainnetChainName:
		return &XLayerMainnetGenesisHash
	default:
		return nil
	}
}

func ChainConfigByGenesisHash(genesisHash libcommon.Hash) *chain.Config {
	switch {
	case genesisHash == MainnetGenesisHash:
		return MainnetChainConfig
	case genesisHash == HoleskyGenesisHash:
		return HoleskyChainConfig
	case genesisHash == SepoliaGenesisHash:
		return SepoliaChainConfig
	case genesisHash == GoerliGenesisHash:
		return GoerliChainConfig
	case genesisHash == MumbaiGenesisHash:
		return MumbaiChainConfig
	case genesisHash == AmoyGenesisHash:
		return AmoyChainConfig
	case genesisHash == BorMainnetGenesisHash:
		return BorMainnetChainConfig
	case genesisHash == BorDevnetGenesisHash:
		return BorDevnetChainConfig
	case genesisHash == GnosisGenesisHash:
		return GnosisChainConfig
	case genesisHash == ChiadoGenesisHash:
		return ChiadoChainConfig
	case genesisHash == HermezMainnetGenesisHash:
		return HermezMainnetChainConfig
	case genesisHash == HermezMainnetShadowforkGenesisHash:
		return HermezMainnetShadowforkChainConfig
	case genesisHash == HermezLocalDevnetGenesisHash:
		return HermezLocalDevnetChainConfig
	case genesisHash == HermezESTestGenesisHash:
		return HermezESTestChainConfig
	case genesisHash == HermezCardonaGenesisHash:
		return HermezCardonaChainConfig
	case genesisHash == HermezBaliGenesisHash:
		return HermezBaliChainConfig
	case genesisHash == XLayerTestnetGenesisHash:
		return XLayerTestnetChainConfig
	case genesisHash == XLayerMainnetGenesisHash:
		return XLayerMainnetChainConfig
	default:
		return nil
	}
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
		GoerliChainConfig.ChainID,
		SepoliaChainConfig.ChainID,
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
