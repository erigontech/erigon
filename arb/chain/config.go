package chain

import (
	"embed"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/chainspec"
)

//go:embed chainspecs
var chainspecs embed.FS

var (
	ArbSepoliaGenesisHash = common.HexToHash("0x77194da4010e549a7028a9c3c51c3e277823be6ac7d138d0bb8a70197b5c004c")

	ArbSepoliaChainConfig = chainspec.ReadChainSpec(chainspecs, "chainspecs/arb-sepolia.json")
)

func init() {
	chainspec.RegisterChain(networkname.ArbiturmSepolia, ArbSepoliaChainConfig, ArbSepoliaRollupGenesisBlock(), ArbSepoliaGenesisHash, nil, "")
}
