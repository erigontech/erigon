package ethconfig

import (
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common"
)

type Zk struct {
	L2ChainId                              uint64
	L2RpcUrl                               string
	L2DataStreamerUrl                      string
	L2DataStreamerTimeout                  time.Duration
	L1SyncStartBlock                       uint64
	L1SyncStopBatch                        uint64
	L1ChainId                              uint64
	L1RpcUrl                               string
	AddressSequencer                       common.Address
	AddressAdmin                           common.Address
	AddressRollup                          common.Address
	AddressZkevm                           common.Address
	AddressGerManager                      common.Address
	L1ContractAddressCheck                 bool
	L1ContractAddressRetrieve              bool
	L1RollupId                             uint64
	L1BlockRange                           uint64
	L1QueryDelay                           uint64
	L1HighestBlockType                     string
	L1MaticContractAddress                 common.Address
	L1FirstBlock                           uint64
	L1FinalizedBlockRequirement            uint64
	L1CacheEnabled                         bool
	L1CachePort                            uint
	RpcRateLimits                          int
	RpcGetBatchWitnessConcurrencyLimit     int
	DatastreamVersion                      int
	SequencerBlockSealTime                 time.Duration
	SequencerBatchSealTime                 time.Duration
	SequencerBatchVerificationTimeout      time.Duration
	SequencerBatchVerificationRetries      int
	SequencerTimeoutOnEmptyTxPool          time.Duration
	SequencerHaltOnBatchNumber             uint64
	SequencerResequence                    bool
	SequencerResequenceStrict              bool
	SequencerResequenceReuseL1InfoIndex    bool
	ExecutorUrls                           []string
	ExecutorStrictMode                     bool
	ExecutorRequestTimeout                 time.Duration
	DatastreamNewBlockTimeout              time.Duration
	WitnessMemdbSize                       datasize.ByteSize
	ExecutorMaxConcurrentRequests          int
	Limbo                                  bool
	AllowFreeTransactions                  bool
	AllowPreEIP155Transactions             bool
	EffectiveGasPriceForEthTransfer        uint8
	EffectiveGasPriceForErc20Transfer      uint8
	EffectiveGasPriceForContractInvocation uint8
	EffectiveGasPriceForContractDeployment uint8
	DefaultGasPrice                        uint64
	MaxGasPrice                            uint64
	GasPriceFactor                         float64
	DAUrl                                  string
	DataStreamHost                         string
	DataStreamPort                         uint
	DataStreamWriteTimeout                 time.Duration
	DataStreamInactivityTimeout            time.Duration
	DataStreamInactivityCheckInterval      time.Duration

	RebuildTreeAfter      uint64
	IncrementTreeAlways   bool
	SmtRegenerateInMemory bool
	WitnessFull           bool
	SyncLimit             uint64
	Gasless               bool

	DebugTimers    bool
	DebugNoSync    bool
	DebugLimit     uint64
	DebugStep      uint64
	DebugStepAfter uint64

	PoolManagerUrl              string
	DisableVirtualCounters      bool
	VirtualCountersSmtReduction float64
	ExecutorPayloadOutput       string

	TxPoolRejectSmartContractDeployments bool
}

var DefaultZkConfig = &Zk{}

func (c *Zk) ShouldCountersBeUnlimited(l1Recovery bool) bool {
	return l1Recovery || (c.DisableVirtualCounters && !c.ExecutorStrictMode && !c.HasExecutors())
}

func (c *Zk) HasExecutors() bool {
	return len(c.ExecutorUrls) > 0 && c.ExecutorUrls[0] != ""
}
