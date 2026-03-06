package l1sync

import (
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/common"
)

type Config struct {
	PollInterval       time.Duration
	StartBatch         uint64
	StartL1Block       uint64
	L1BlocksPerRequest uint64
	ChainID            *big.Int
	SequencerInboxAddr common.Address
	BridgeAddr         common.Address
}

var DefaultConfig = Config{
	PollInterval:       10 * time.Second,
	L1BlocksPerRequest: 512,
}

var ArbitrumOneConfig = Config{
	PollInterval:       10 * time.Second,
	StartL1Block:       15411056,
	L1BlocksPerRequest: 512,
	ChainID:            big.NewInt(42161),
	SequencerInboxAddr: common.HexToAddress("0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6"),
	BridgeAddr:         common.HexToAddress("0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a"),
}

var ArbitrumSepoliaConfig = Config{
	PollInterval:       10 * time.Second,
	StartL1Block:       4139226,
	L1BlocksPerRequest: 512,
	ChainID:            big.NewInt(421614),
	SequencerInboxAddr: common.HexToAddress("0x6c97864CE4bEf387dE0b3310A44230f7E3F1be0D"),
	BridgeAddr:         common.HexToAddress("0x38f918D0E9F1b721EDaA41302E399fa1B79333a9"),
}

// ConfigForChain returns the l1sync config preset for the given chain name, or nil if unsupported.
func ConfigForChain(chainName string) *Config {
	switch chainName {
	case "arb-sepolia":
		cfg := ArbitrumSepoliaConfig
		return &cfg
	case "arb1":
		cfg := ArbitrumOneConfig
		return &cfg
	default:
		return nil
	}
}
