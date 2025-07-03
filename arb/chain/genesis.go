package chain

import (
	"embed"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/execution/chainspec"
)

//go:embed allocs
var allocs embed.FS

func ArbSepoliaRollupGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     ArbSepoliaChainConfig,
		Nonce:      0x0000000000000001,
		Timestamp:  0x0,
		ExtraData:  common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x4000000000000, // as given in hex
		Difficulty: big.NewInt(1),   // "0x1"
		Mixhash:    common.HexToHash("0x00000000000000000000000000000000000000000000000a0000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Number:     0x0, // block number 0
		GasUsed:    0x0,
		ParentHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		BaseFee:    big.NewInt(0x5f5e100),
		Alloc:      chainspec.ReadPrealloc(allocs, "allocs/arb_sepolia.json"),
	}
}
