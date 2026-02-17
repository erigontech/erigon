package chain

import (
	"embed"
	"github.com/erigontech/erigon/execution/chain/params"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
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

func ArbOneGenesis() *types.Genesis {
	return &types.Genesis{
		Config:     Arb1ChainConfig,
		Nonce:      0x0000000000000001,
		Timestamp:  0x630f70f6,
		ExtraData:  common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x4000000000000, // as given in hex
		Difficulty: big.NewInt(1),   // "0x1"
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000060000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Number:     0x152dd49,
		GasUsed:    0x0,
		ParentHash: common.HexToHash("0xa903d86321a537beab1a892c387c3198a6dd75dbd4a68346b04642770d20d8fe"),
		BaseFee:    big.NewInt(0x5f5e100),
		Alloc:      types.GenesisAlloc{},
	}
}

func ArbOneGenesisBlock() *types.Block {
	g := ArbOneGenesis()

	head := &types.Header{
		Number:        new(big.Int).SetUint64(g.Number),
		Nonce:         types.EncodeNonce(g.Nonce),
		Time:          g.Timestamp,
		ParentHash:    g.ParentHash,
		Extra:         g.ExtraData,
		GasLimit:      g.GasLimit,
		GasUsed:       g.GasUsed,
		Difficulty:    g.Difficulty,
		MixDigest:     g.Mixhash,
		Coinbase:      g.Coinbase,
		BaseFee:       g.BaseFee,
		BlobGasUsed:   g.BlobGasUsed,
		ExcessBlobGas: g.ExcessBlobGas,
		RequestsHash:  g.RequestsHash,
		Root:          Arb1GenesisStateRoot,
	}
	if g.AuRaSeal != nil && len(g.AuRaSeal.AuthorityRound.Signature) > 0 {
		head.AuRaSeal = g.AuRaSeal.AuthorityRound.Signature
		head.AuRaStep = uint64(g.AuRaSeal.AuthorityRound.Step)
	}
	// if g.GasLimit == 0 {
	//      head.GasLimit = params.GenesisGasLimit
	// }
	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && g.Config.IsLondon(0) {
		if g.BaseFee != nil {
			head.BaseFee = g.BaseFee
		} else {
			head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}
	b := types.NewBlock(head, nil, nil, nil, nil)
	return b
}
