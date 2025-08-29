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
	ArbSepoliaChainConfig = chainspec.ReadChainSpec(chainspecs, "chainspecs/arb-sepolia.json")
	ArbSepoliaGenesisHash = common.HexToHash("0x77194da4010e549a7028a9c3c51c3e277823be6ac7d138d0bb8a70197b5c004c")

	Arb1ChainConfig = chainspec.ReadChainSpec(chainspecs, "chainspecs/arb1.json")
	Arb1GenesisHash = common.HexToHash("0xd1882c626699cd19548720713669993ac8f51500056dfbc1afc180496c7f8e2f")
)

func init() {
	chainspec.RegisterChain(networkname.ArbiturmSepolia, ArbSepoliaChainConfig, ArbSepoliaRollupGenesisBlock(), ArbSepoliaGenesisHash, nil, "")
	chainspec.RegisterChain(networkname.ArbitrumMainnet, Arb1ChainConfig, Arb1RollupGenesisBlock(), Arb1GenesisHash, nil, "")
}
