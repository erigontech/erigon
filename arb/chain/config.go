package chain

import (
	"embed"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

//go:embed chainspecs
var chainspecs embed.FS

var (
	ArbSepoliaGenesisHash = common.HexToHash("0x77194da4010e549a7028a9c3c51c3e277823be6ac7d138d0bb8a70197b5c004c")
	ArbSepoliaChainConfig = chainspec.ReadChainConfig(chainspecs, "chainspecs/arb-sepolia.json")

	ArbSepolia = chainspec.Spec{
		Name:        networkname.ArbiturmSepolia,
		GenesisHash: ArbSepoliaGenesisHash,
		Config:      chainspec.ReadChainConfig(chainspecs, "chainspecs/arb-sepolia.json"),
		Genesis:     ArbSepoliaRollupGenesisBlock(),
	}

	Arb1GenesisHash = common.HexToHash("0x7d237dd685b96381544e223f8906e35645d63b89c19983f2246db48568c07986")
	Arb1ChainConfig = chainspec.ReadChainConfig(chainspecs, "chainspecs/arb1.json")

	ArbOne = chainspec.Spec{
		Name:        networkname.ArbitrumOne,
		GenesisHash: Arb1GenesisHash,
		Config:      chainspec.ReadChainConfig(chainspecs, "chainspecs/arb1.json"),
		Genesis:     ArbOneGenesis(),
	}
)

func init() {
	chainspec.RegisterChainSpec(networkname.ArbiturmSepolia, ArbSepolia)
	chainspec.RegisterChainSpec(networkname.ArbitrumOne, ArbOne)

}
