package l1sync

import (
	"time"

	"github.com/erigontech/erigon-lib/common"
)

type Config struct {
	Enable             bool
	PollInterval       time.Duration
	StartBatch         uint64
	StartL1Block       uint64
	MaxBatchesPerPoll  uint64
	L1BlocksPerRequest uint64
	SequencerInboxAddr common.Address
	BridgeAddr         common.Address
}

var DefaultConfig = Config{
	Enable:             false,
	PollInterval:       10 * time.Second,
	MaxBatchesPerPoll:  100,
	L1BlocksPerRequest: 1,
}

var ArbitrumOneConfig = Config{
	Enable:             false,
	PollInterval:       10 * time.Second,
	StartL1Block:       15411056,
	MaxBatchesPerPoll:  100,
	L1BlocksPerRequest: 1,
	SequencerInboxAddr: common.HexToAddress("0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6"),
	BridgeAddr:         common.HexToAddress("0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a"),
}

var ArbitrumSepoliaConfig = Config{
	Enable:             false,
	PollInterval:       1 * time.Second,
	StartL1Block:       4139226,
	MaxBatchesPerPoll:  100,
	L1BlocksPerRequest: 1,
	SequencerInboxAddr: common.HexToAddress("0x6c97864CE4bEf387dE0b3310A44230f7E3F1be0D"),
	BridgeAddr:         common.HexToAddress("0x38f918D0E9F1b721EDaA41302E399fa1B79333a9"),
}
