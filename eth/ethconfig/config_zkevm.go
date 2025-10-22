package ethconfig

import (
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
)

type Zk struct {
	L2ChainId                              uint64
	L2RpcUrl                               string
	L2DataStreamerUrl                      string
	L2DataStreamerMaxEntryChan             uint64
	L2DataStreamerUseTLS                   bool
	L2DataStreamerTimeout                  time.Duration
	L2ShortCircuitToVerifiedBatch          bool
	L1SyncStartBlock                       uint64
	BlobRecovery                           bool
	BlobDAUrl                              string
	BlobRecoveryBlobLimit                  uint64
	RecoveryStopBatch                      uint64
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
	L1NoActivityTimeout                    time.Duration
	L1FinalizedBlockRequirement            uint64
	RpcRateLimits                          int
	RpcGetBatchWitnessConcurrencyLimit     int
	SequencerBlockSealTime                 time.Duration
	SequencerEmptyBlockSealTime            time.Duration
	SequencerBatchSealTime                 time.Duration
	SequencerBatchVerificationTimeout      time.Duration
	SequencerBatchVerificationRetries      int
	SequencerTimeoutOnEmptyTxPool          time.Duration
	SequencerHaltOnBatchNumber             uint64
	SequencerResequence                    bool
	SequencerResequenceStrict              bool
	SequencerResequenceReuseL1InfoIndex    bool
	SequencerDecodedTxCacheSize            int
	SequencerDecodedTxCacheTTL             time.Duration
	SequencerResequenceInfoTreeOffset      *L1InfoTreeOffset
	ExecutorUrls                           []string
	ExecutorStrictMode                     bool `yaml:"zkevm.executor-strict"`
	ExecutorRequestTimeout                 time.Duration
	ExecutorEnabled                        bool
	DatastreamNewBlockTimeout              time.Duration
	WitnessMemdbSize                       datasize.ByteSize
	WitnessUnwindLimit                     uint64
	ExecutorMaxConcurrentRequests          int
	Limbo                                  bool
	AllowFreeTransactions                  bool
	FreeInjectedBatch                      bool
	AllowPreEIP155Transactions             bool
	EffectiveGasPriceForEthTransfer        uint8
	EffectiveGasPriceForErc20Transfer      uint8
	EffectiveGasPriceForContractInvocation uint8
	EffectiveGasPriceForContractDeployment uint8
	DefaultGasPrice                        uint64
	MaxGasPrice                            uint64
	GasPriceFactor                         float64
	GasPriceCheckFrequency                 time.Duration
	GasPriceHistoryCount                   uint64
	DAUrl                                  string
	DataStreamHost                         string
	DataStreamPort                         uint
	DataStreamWriteTimeout                 time.Duration
	DataStreamInactivityTimeout            time.Duration
	DataStreamInactivityCheckInterval      time.Duration
	PanicOnReorg                           bool
	ShadowSequencer                        bool

	RebuildTreeAfter         uint64
	IncrementTreeAlways      bool
	SmtRegenerateInMemory    bool
	WitnessFull              bool
	SyncLimit                uint64
	SyncLimitVerifiedEnabled bool
	SyncLimitUnverifiedCount uint64
	Gasless                  bool

	DebugTimers                bool
	DebugNoSync                bool
	DebugLimit                 uint64
	DebugStep                  uint64
	DebugStepAfter             uint64
	DebugDisableStateRootCheck bool

	PoolManagerUrl              string
	DisableVirtualCounters      bool
	VirtualCountersSmtReduction float64
	ExecutorPayloadOutput       string

	TxPoolRejectSmartContractDeployments bool

	InitialBatchCfgFile            string
	ACLPrintHistory                int
	ACLJsonLocation                string
	PrioritySendersJsonLocation    string
	InfoTreeUpdateInterval         time.Duration
	BadBatches                     []uint64
	IgnoreBadBatchesCheck          bool
	SealBatchImmediatelyOnOverflow bool
	MockWitnessGeneration          bool
	WitnessCacheEnabled            bool
	WitnessCachePurge              bool
	WitnessCacheBatchAheadOffset   uint64
	WitnessCacheBatchBehindOffset  uint64
	WitnessContractInclusion       []common.Address
	AlwaysGenerateBatchL2Data      bool
	RejectLowGasPriceTransactions  bool
	RejectLowGasPriceTolerance     float64
	LogLevel                       log.Lvl
	BadTxAllowance                 uint64
	BadTxStoreValue                uint64
	BadTxPurge                     bool
	L2InfoTreeUpdatesBatchSize     uint64
	L2InfoTreeUpdatesURL           string

	InjectGers             bool
	HonourChainspec        bool `yaml:"zkevm.honour-chainspec"`
	SimultaneousPmtAndSmt  bool
	SkipSmt                bool
	OnlySmtV2              bool
	SequencerBlockGasLimit uint64
	PessimisticForkNumber  uint64
}

func (c *Zk) ShouldCountersBeUnlimited(l1Recovery bool) bool {
	return l1Recovery || (c.DisableVirtualCounters && !c.ExecutorStrictMode && !c.HasExecutors())
}

func (c *Zk) HasExecutors() bool {
	return len(c.ExecutorUrls) > 0 && c.ExecutorUrls[0] != ""
}

func (c *Zk) UseExecutors() bool {
	return c.HasExecutors() && c.ExecutorEnabled
}

// ShouldImportInitialBatch returns true in case initial batch config file name is non-empty string.
func (c *Zk) ShouldImportInitialBatch() bool {
	return c.InitialBatchCfgFile != ""
}

func (c *Zk) IsL1Recovery() bool {
	return c.L1SyncStartBlock > 0
}

func (c *Zk) IsBlobRecovery() bool {
	return c.BlobRecovery
}

type L1InfoTreeOffset struct {
	Index           uint64
	Offset          int64
	ExpectedGerHash common.Hash
}
