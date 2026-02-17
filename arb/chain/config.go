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

	Arb1GenesisHash      = common.HexToHash("0xd4879c49878f74c5a4049ef78ee9b1fc6bccf0697dd139f7d18f887c82cd94a9")
	Arb1GenesisStateRoot = common.HexToHash("0xed4afb8b378fc27b09db5a9e9245e8dcd20228fbebd4e38a11c990c6aa0e4b55")
	Arb1ChainConfig      = chainspec.ReadChainConfig(chainspecs, "chainspecs/arb1.json")

	ArbOne = chainspec.Spec{
		Name:             networkname.ArbitrumOne,
		GenesisHash:      Arb1GenesisHash,
		GenesisStateRoot: Arb1GenesisStateRoot,
		Config:           chainspec.ReadChainConfig(chainspecs, "chainspecs/arb1.json"),
		Genesis:          ArbOneGenesis(),
	}
)

func init() {
	chainspec.RegisterChainSpec(networkname.ArbiturmSepolia, ArbSepolia)
	chainspec.RegisterChainSpec(networkname.ArbitrumOne, ArbOne)

}
