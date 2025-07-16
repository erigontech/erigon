package chainspec

import (
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	taikoGenesis "github.com/erigontech/erigon/execution/chainspec/taiko_genesis"
)

var (
	logger = log.New("taiko-genesis")
)

// TaikoGenesisBlock returns the Taiko network genesis block configs.
func TaikoGenesisBlock(networkID uint64) *types.Genesis {
	chainConfig := TaikoChainConfig

	var allocJSON []byte
	switch networkID {
	case TaikoMainnetNetworkID.Uint64():
		chainConfig.ChainID = TaikoMainnetNetworkID
		chainConfig.OntakeBlock = MainnetOntakeBlock
		chainConfig.PacayaBlock = MainnetPacayaBlock
		allocJSON = taikoGenesis.MainnetGenesisAllocJSON
	case TaikoInternalL2ANetworkID.Uint64():
		chainConfig.ChainID = TaikoInternalL2ANetworkID
		chainConfig.OntakeBlock = InternalDevnetOntakeBlock
		chainConfig.PacayaBlock = InternalDevnetPacayaBlock
		allocJSON = taikoGenesis.InternalL2AGenesisAllocJSON
	case TaikoInternalL2BNetworkID.Uint64():
		chainConfig.ChainID = TaikoInternalL2BNetworkID
		allocJSON = taikoGenesis.InternalL2BGenesisAllocJSON
	case SnaefellsjokullNetworkID.Uint64():
		chainConfig.ChainID = SnaefellsjokullNetworkID
		allocJSON = taikoGenesis.SnaefellsjokullGenesisAllocJSON
	case AskjaNetworkID.Uint64():
		chainConfig.ChainID = AskjaNetworkID
		allocJSON = taikoGenesis.AskjaGenesisAllocJSON
	case GrimsvotnNetworkID.Uint64():
		chainConfig.ChainID = GrimsvotnNetworkID
		allocJSON = taikoGenesis.GrimsvotnGenesisAllocJSON
	case EldfellNetworkID.Uint64():
		chainConfig.ChainID = EldfellNetworkID
		allocJSON = taikoGenesis.EldfellGenesisAllocJSON
	case JolnirNetworkID.Uint64():
		chainConfig.ChainID = JolnirNetworkID
		allocJSON = taikoGenesis.JolnirGenesisAllocJSON
	case KatlaNetworkID.Uint64():
		chainConfig.ChainID = KatlaNetworkID
		allocJSON = taikoGenesis.KatlaGenesisAllocJSON
	case HeklaNetworkID.Uint64():
		chainConfig.ChainID = HeklaNetworkID
		chainConfig.OntakeBlock = HeklaOntakeBlock
		chainConfig.PacayaBlock = HeklaPacayaBlock
		allocJSON = taikoGenesis.HeklaGenesisAllocJSON
	case PreconfDevnetNetworkID.Uint64():
		chainConfig.ChainID = PreconfDevnetNetworkID
		chainConfig.OntakeBlock = PreconfDevnetOntakeBlock
		chainConfig.PacayaBlock = PreconfDevnetPacayaBlock
		allocJSON = taikoGenesis.PreconfDevnetGenesisAllocJSON
	default:
		chainConfig.ChainID = TaikoInternalL2ANetworkID
		chainConfig.OntakeBlock = InternalDevnetOntakeBlock
		chainConfig.PacayaBlock = InternalDevnetPacayaBlock
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
