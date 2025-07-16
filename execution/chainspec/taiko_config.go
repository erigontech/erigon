package chainspec

import (
	"encoding/binary"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
)

// To make taiko-geth compatible with the op-service library, we need to define the following constants and functions.
type ProtocolVersion [32]byte
type ProtocolVersionComparison int

func (p ProtocolVersion) String() string {
	return ""
}

func (p ProtocolVersion) Compare(other ProtocolVersion) (cmp ProtocolVersionComparison) {
	return 0
}

type ProtocolVersionV0 struct {
	Build                           [8]byte
	Major, Minor, Patch, PreRelease uint32
}

func (v ProtocolVersionV0) Encode() (out ProtocolVersion) {
	copy(out[8:16], v.Build[:])
	binary.BigEndian.PutUint32(out[16:20], v.Major)
	binary.BigEndian.PutUint32(out[20:24], v.Minor)
	binary.BigEndian.PutUint32(out[24:28], v.Patch)
	binary.BigEndian.PutUint32(out[28:32], v.PreRelease)
	return
}

func u64(val uint64) *uint64 { return &val }

// Network IDs
var (
	TaikoMainnetNetworkID     = big.NewInt(167000)
	TaikoInternalL2ANetworkID = big.NewInt(167001)
	TaikoInternalL2BNetworkID = big.NewInt(167002)
	SnaefellsjokullNetworkID  = big.NewInt(167003)
	AskjaNetworkID            = big.NewInt(167004)
	GrimsvotnNetworkID        = big.NewInt(167005)
	EldfellNetworkID          = big.NewInt(167006)
	JolnirNetworkID           = big.NewInt(167007)
	KatlaNetworkID            = big.NewInt(167008)
	HeklaNetworkID            = big.NewInt(167009)
	PreconfDevnetNetworkID    = big.NewInt(167010)
)

var (
	InternalDevnetOntakeBlock = new(big.Int).SetUint64(0)
	PreconfDevnetOntakeBlock  = common.Big0
	HeklaOntakeBlock          = new(big.Int).SetUint64(840_512)
	MainnetOntakeBlock        = new(big.Int).SetUint64(538_304)

	InternalDevnetPacayaBlock = new(big.Int).SetUint64(0)
	PreconfDevnetPacayaBlock  = common.Big0
	HeklaPacayaBlock          = new(big.Int).SetUint64(1_299_888)
	MainnetPacayaBlock        = new(big.Int).SetUint64(1_166_000)
)

var (
	TaikoAlethiaChainName = "taiko-alethia"
)

var networkIDToChainConfig = map[*big.Int]*chain.Config{
	TaikoMainnetNetworkID:            TaikoChainConfig,
	TaikoInternalL2ANetworkID:        TaikoChainConfig,
	TaikoInternalL2BNetworkID:        TaikoChainConfig,
	SnaefellsjokullNetworkID:         TaikoChainConfig,
	AskjaNetworkID:                   TaikoChainConfig,
	GrimsvotnNetworkID:               TaikoChainConfig,
	EldfellNetworkID:                 TaikoChainConfig,
	JolnirNetworkID:                  TaikoChainConfig,
	KatlaNetworkID:                   TaikoChainConfig,
	HeklaNetworkID:                   TaikoChainConfig,
	PreconfDevnetNetworkID:           TaikoChainConfig,
	MainnetChainConfig.ChainID:       MainnetChainConfig,
	SepoliaChainConfig.ChainID:       SepoliaChainConfig,
	chain.TestChainConfig.ChainID:    chain.TestChainConfig,
	chain.NonActivatedConfig.ChainID: chain.NonActivatedConfig,
}

func NetworkIDToChainConfigOrDefault(networkID *big.Int) *chain.Config {
	if config, ok := networkIDToChainConfig[networkID]; ok {
		return config
	}

	return chain.NonActivatedConfig
}

var TaikoChainConfig = &chain.Config{
	ChainName:               TaikoAlethiaChainName,
	ChainID:                 TaikoInternalL2ANetworkID, // Use Internal Devnet network ID by default.
	HomesteadBlock:          common.Big0,
	TangerineWhistleBlock:   common.Big0,
	SpuriousDragonBlock:     common.Big0,
	ByzantiumBlock:          common.Big0,
	ConstantinopleBlock:     common.Big0,
	PetersburgBlock:         common.Big0,
	IstanbulBlock:           common.Big0,
	BerlinBlock:             common.Big0,
	LondonBlock:             common.Big0,
	ShanghaiTime:            common.Big0,
	MergeNetsplitBlock:      nil,
	TerminalTotalDifficulty: common.Big0,
	Taiko:                   true,
}
