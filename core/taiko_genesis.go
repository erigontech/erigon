package core

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	taikoGenesis "github.com/erigontech/erigon/core/taiko_genesis"
	"github.com/erigontech/erigon/params"
)

var (
	logger = log.New("taiko-genesis")
)

// TaikoGenesisBlock returns the Taiko network genesis block configs.
func TaikoGenesisBlock(networkID uint64) *types.Genesis {
	chainConfig := params.TaikoChainConfig

	var allocJSON []byte
	switch networkID {
	case params.TaikoMainnetNetworkID.Uint64():
		chainConfig.ChainID = params.TaikoMainnetNetworkID
		chainConfig.OntakeBlock = params.MainnetOntakeBlock
		chainConfig.PacayaBlock = params.MainnetPacayaBlock
		allocJSON = taikoGenesis.MainnetGenesisAllocJSON
	case params.TaikoInternalL2ANetworkID.Uint64():
		chainConfig.ChainID = params.TaikoInternalL2ANetworkID
		chainConfig.OntakeBlock = params.InternalDevnetOntakeBlock
		chainConfig.PacayaBlock = params.InternalDevnetPacayaBlock
		allocJSON = taikoGenesis.InternalL2AGenesisAllocJSON
	case params.TaikoInternalL2BNetworkID.Uint64():
		chainConfig.ChainID = params.TaikoInternalL2BNetworkID
		allocJSON = taikoGenesis.InternalL2BGenesisAllocJSON
	case params.SnaefellsjokullNetworkID.Uint64():
		chainConfig.ChainID = params.SnaefellsjokullNetworkID
		allocJSON = taikoGenesis.SnaefellsjokullGenesisAllocJSON
	case params.AskjaNetworkID.Uint64():
		chainConfig.ChainID = params.AskjaNetworkID
		allocJSON = taikoGenesis.AskjaGenesisAllocJSON
	case params.GrimsvotnNetworkID.Uint64():
		chainConfig.ChainID = params.GrimsvotnNetworkID
		allocJSON = taikoGenesis.GrimsvotnGenesisAllocJSON
	case params.EldfellNetworkID.Uint64():
		chainConfig.ChainID = params.EldfellNetworkID
		allocJSON = taikoGenesis.EldfellGenesisAllocJSON
	case params.JolnirNetworkID.Uint64():
		chainConfig.ChainID = params.JolnirNetworkID
		allocJSON = taikoGenesis.JolnirGenesisAllocJSON
	case params.KatlaNetworkID.Uint64():
		chainConfig.ChainID = params.KatlaNetworkID
		allocJSON = taikoGenesis.KatlaGenesisAllocJSON
	case params.HeklaNetworkID.Uint64():
		chainConfig.ChainID = params.HeklaNetworkID
		chainConfig.OntakeBlock = params.HeklaOntakeBlock
		chainConfig.PacayaBlock = params.HeklaPacayaBlock
		allocJSON = taikoGenesis.HeklaGenesisAllocJSON
	case params.PreconfDevnetNetworkID.Uint64():
		chainConfig.ChainID = params.PreconfDevnetNetworkID
		chainConfig.OntakeBlock = params.PreconfDevnetOntakeBlock
		chainConfig.PacayaBlock = params.PreconfDevnetPacayaBlock
		allocJSON = taikoGenesis.PreconfDevnetGenesisAllocJSON
	default:
		chainConfig.ChainID = params.TaikoInternalL2ANetworkID
		chainConfig.OntakeBlock = params.InternalDevnetOntakeBlock
		chainConfig.PacayaBlock = params.InternalDevnetPacayaBlock
		allocJSON = taikoGenesis.InternalL2AGenesisAllocJSON
	}

	var alloc types.GenesisAlloc

	if err := alloc.UnmarshalJSON(allocJSON); err != nil {
		log.Crit("unmarshal alloc json error", "error", err)
	}

	return &types.Genesis{
		Config:     chainConfig,
		ExtraData:  []byte{},
		GasLimit:   uint64(15_000_000),
		Difficulty: common.Big0,
		Alloc:      alloc,
		GasUsed:    0,
		BaseFee:    new(big.Int).SetUint64(10_000_000),
	}
}
