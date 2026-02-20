package chain

import (
	"embed"

	"github.com/erigontech/erigon/common"
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
)

func init() {
	chainspec.RegisterChainSpec(networkname.ArbiturmSepolia, ArbSepolia)
}
