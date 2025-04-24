package arbitrum

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/nitro-erigon/arbos"
	"github.com/erigontech/nitro-erigon/arbos/arbosState"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
	"github.com/erigontech/nitro-erigon/statetransfer"
	flag "github.com/spf13/pflag"
)

type Config struct {
	// RPCGasCap is the global gas cap for eth-call variants.
	RPCGasCap uint64 `koanf:"gas-cap"`

	// RPCTxFeeCap is the global transaction fee(price * gaslimit) cap for
	// send-transction variants. The unit is ether.
	RPCTxFeeCap float64 `koanf:"tx-fee-cap"`

	TxAllowUnprotected bool `koanf:"tx-allow-unprotected"`

	// RPCEVMTimeout is the global timeout for eth-call.
	RPCEVMTimeout time.Duration `koanf:"evm-timeout"`

	// Parameters for the bloom indexer
	BloomBitsBlocks uint64 `koanf:"bloom-bits-blocks"`
	BloomConfirms   uint64 `koanf:"bloom-confirms"`

	// Parameters for the filter system
	FilterLogCacheSize int           `koanf:"filter-log-cache-size"`
	FilterTimeout      time.Duration `koanf:"filter-timeout"`

	// FeeHistoryMaxBlockCount limits the number of historical blocks a fee history request may cover
	FeeHistoryMaxBlockCount uint64 `koanf:"feehistory-max-block-count"`

	ArbDebug ArbDebugConfig `koanf:"arbdebug"`

	ClassicRedirect        string        `koanf:"classic-redirect"`
	ClassicRedirectTimeout time.Duration `koanf:"classic-redirect-timeout"`
	MaxRecreateStateDepth  int64         `koanf:"max-recreate-state-depth"`

	AllowMethod []string `koanf:"allow-method"`
}

type ArbDebugConfig struct {
	BlockRangeBound   uint64 `koanf:"block-range-bound"`
	TimeoutQueueBound uint64 `koanf:"timeout-queue-bound"`
}

func ConfigAddOptions(prefix string, f *flag.FlagSet) {
	f.Uint64(prefix+".gas-cap", DefaultConfig.RPCGasCap, "cap on computation gas that can be used in eth_call/estimateGas (0=infinite)")
	f.Float64(prefix+".tx-fee-cap", DefaultConfig.RPCTxFeeCap, "cap on transaction fee (in ether) that can be sent via the RPC APIs (0 = no cap)")
	f.Bool(prefix+".tx-allow-unprotected", DefaultConfig.TxAllowUnprotected, "allow transactions that aren't EIP-155 replay protected to be submitted over the RPC")
	f.Duration(prefix+".evm-timeout", DefaultConfig.RPCEVMTimeout, "timeout used for eth_call (0=infinite)")
	f.Uint64(prefix+".bloom-bits-blocks", DefaultConfig.BloomBitsBlocks, "number of blocks a single bloom bit section vector holds")
	f.Uint64(prefix+".bloom-confirms", DefaultConfig.BloomConfirms, "number of confirmation blocks before a bloom section is considered final")
	f.Uint64(prefix+".feehistory-max-block-count", DefaultConfig.FeeHistoryMaxBlockCount, "max number of blocks a fee history request may cover")
	f.String(prefix+".classic-redirect", DefaultConfig.ClassicRedirect, "url to redirect classic requests, use \"error:[CODE:]MESSAGE\" to return specified error instead of redirecting")
	f.Duration(prefix+".classic-redirect-timeout", DefaultConfig.ClassicRedirectTimeout, "timeout for forwarded classic requests, where 0 = no timeout")
	f.Int(prefix+".filter-log-cache-size", DefaultConfig.FilterLogCacheSize, "log filter system maximum number of cached blocks")
	f.Duration(prefix+".filter-timeout", DefaultConfig.FilterTimeout, "log filter system maximum time filters stay active")
	f.Int64(prefix+".max-recreate-state-depth", DefaultConfig.MaxRecreateStateDepth, "maximum depth for recreating state, measured in l2 gas (0=don't recreate state, -1=infinite, -2=use default value for archive or non-archive node (whichever is configured))")
	f.StringSlice(prefix+".allow-method", DefaultConfig.AllowMethod, "list of whitelisted rpc methods")
	arbDebug := DefaultConfig.ArbDebug
	f.Uint64(prefix+".arbdebug.block-range-bound", arbDebug.BlockRangeBound, "bounds the number of blocks arbdebug calls may return")
	f.Uint64(prefix+".arbdebug.timeout-queue-bound", arbDebug.TimeoutQueueBound, "bounds the length of timeout queues arbdebug calls may return")
}

const (
	DefaultArchiveNodeMaxRecreateStateDepth    = 30 * 1000 * 1000
	DefaultNonArchiveNodeMaxRecreateStateDepth = 0 // don't recreate state
	UninitializedMaxRecreateStateDepth         = -2
	InfiniteMaxRecreateStateDepth              = -1
)

var DefaultConfig = Config{
	RPCGasCap:               ethconfig.Defaults.RPCGasCap,   // 50,000,000
	RPCTxFeeCap:             ethconfig.Defaults.RPCTxFeeCap, // 1 ether
	TxAllowUnprotected:      true,
	RPCEVMTimeout:           ethconfig.Defaults.ArbRPCEVMTimeout, // 5 seconds
	BloomBitsBlocks:         config3.BloomBitsBlocks * 4,         // we generally have smaller blocks
	BloomConfirms:           config3.BloomConfirms,
	FilterLogCacheSize:      32,
	FilterTimeout:           5 * time.Minute,
	FeeHistoryMaxBlockCount: 1024,
	ClassicRedirect:         "",
	MaxRecreateStateDepth:   UninitializedMaxRecreateStateDepth, // default value should be set for depending on node type (archive / non-archive)
	AllowMethod:             []string{},
	ArbDebug: ArbDebugConfig{
		BlockRangeBound:   256,
		TimeoutQueueBound: 512,
	},
}

type CachingConfig struct {
	Archive                             bool          `koanf:"archive"`
	BlockCount                          uint64        `koanf:"block-count"`
	BlockAge                            time.Duration `koanf:"block-age"`
	TrieTimeLimit                       time.Duration `koanf:"trie-time-limit"`
	TrieDirtyCache                      int           `koanf:"trie-dirty-cache"`
	TrieCleanCache                      int           `koanf:"trie-clean-cache"`
	SnapshotCache                       int           `koanf:"snapshot-cache"`
	DatabaseCache                       int           `koanf:"database-cache"`
	SnapshotRestoreGasLimit             uint64        `koanf:"snapshot-restore-gas-limit"`
	MaxNumberOfBlocksToSkipStateSaving  uint32        `koanf:"max-number-of-blocks-to-skip-state-saving"`
	MaxAmountOfGasToSkipStateSaving     uint64        `koanf:"max-amount-of-gas-to-skip-state-saving"`
	StylusLRUCacheCapacity              uint32        `koanf:"stylus-lru-cache-capacity"`
	DisableStylusCacheMetricsCollection bool          `koanf:"disable-stylus-cache-metrics-collection"`
	StateScheme                         string        `koanf:"state-scheme"`
	StateHistory                        uint64        `koanf:"state-history"`
}

func CachingConfigAddOptions(prefix string, f *flag.FlagSet) {
	f.Bool(prefix+".archive", DefaultCachingConfig.Archive, "retain past block state")
	f.Uint64(prefix+".block-count", DefaultCachingConfig.BlockCount, "minimum number of recent blocks to keep in memory")
	f.Duration(prefix+".block-age", DefaultCachingConfig.BlockAge, "minimum age of recent blocks to keep in memory")
	f.Duration(prefix+".trie-time-limit", DefaultCachingConfig.TrieTimeLimit, "maximum block processing time before trie is written to hard-disk")
	f.Int(prefix+".trie-dirty-cache", DefaultCachingConfig.TrieDirtyCache, "amount of memory in megabytes to cache state diffs against disk with (larger cache lowers database growth)")
	f.Int(prefix+".trie-clean-cache", DefaultCachingConfig.TrieCleanCache, "amount of memory in megabytes to cache unchanged state trie nodes with")
	f.Int(prefix+".snapshot-cache", DefaultCachingConfig.SnapshotCache, "amount of memory in megabytes to cache state snapshots with")
	f.Int(prefix+".database-cache", DefaultCachingConfig.DatabaseCache, "amount of memory in megabytes to cache database contents with")
	f.Uint64(prefix+".snapshot-restore-gas-limit", DefaultCachingConfig.SnapshotRestoreGasLimit, "maximum gas rolled back to recover snapshot")
	f.Uint32(prefix+".max-number-of-blocks-to-skip-state-saving", DefaultCachingConfig.MaxNumberOfBlocksToSkipStateSaving, "maximum number of blocks to skip state saving to persistent storage (archive node only) -- warning: this option seems to cause issues")
	f.Uint64(prefix+".max-amount-of-gas-to-skip-state-saving", DefaultCachingConfig.MaxAmountOfGasToSkipStateSaving, "maximum amount of gas in blocks to skip saving state to Persistent storage (archive node only) -- warning: this option seems to cause issues")
	f.Uint32(prefix+".stylus-lru-cache-capacity", DefaultCachingConfig.StylusLRUCacheCapacity, "capacity, in megabytes, of the LRU cache that keeps initialized stylus programs")
	f.Bool(prefix+".disable-stylus-cache-metrics-collection", DefaultCachingConfig.DisableStylusCacheMetricsCollection, "disable metrics collection for the stylus cache")
	f.String(prefix+".state-scheme", DefaultCachingConfig.StateScheme, "scheme to use for state trie storage (hash, path)")
	f.Uint64(prefix+".state-history", DefaultCachingConfig.StateHistory, "number of recent blocks to retain state history for (path state-scheme only)")
}

func getStateHistory(maxBlockSpeed time.Duration) uint64 {
	// #nosec G115
	return uint64(24 * time.Hour / maxBlockSpeed)
}

var DefaultCachingConfig = CachingConfig{
	Archive:                            false,
	BlockCount:                         128,
	BlockAge:                           30 * time.Minute,
	TrieTimeLimit:                      time.Hour,
	TrieDirtyCache:                     1024,
	TrieCleanCache:                     600,
	SnapshotCache:                      400,
	DatabaseCache:                      2048,
	SnapshotRestoreGasLimit:            300_000_000_000,
	MaxNumberOfBlocksToSkipStateSaving: 0,
	MaxAmountOfGasToSkipStateSaving:    0,
	StylusLRUCacheCapacity:             256,
	// StateScheme:                        rawdb.HashScheme,
	StateHistory: getStateHistory(DefaultSequencerConfig.MaxBlockSpeed),
}

func DefaultCacheConfigFor(cachingConfig *CachingConfig) *CachingConfig {
	// baseConf := ethconfig.Defaults
	// if cachingConfig.Archive {
	// 	baseConf = ethconfig.ArchiveDefaults
	// }
	// return &CachingConfig{
	// 	TrieCleanLimit:                     cachingConfig.TrieCleanCache,
	// 	TrieCleanNoPrefetch:                baseConf.NoPrefetch,
	// 	TrieDirtyLimit:                     cachingConfig.TrieDirtyCache,
	// 	TrieDirtyDisabled:                  cachingConfig.Archive,
	// 	TrieTimeLimit:                      cachingConfig.TrieTimeLimit,
	// 	TriesInMemory:                      cachingConfig.BlockCount,
	// 	TrieRetention:                      cachingConfig.BlockAge,
	// 	SnapshotLimit:                      cachingConfig.SnapshotCache,
	// 	Preimages:                          baseConf.Preimages,
	// 	SnapshotRestoreMaxGas:              cachingConfig.SnapshotRestoreGasLimit,
	// 	MaxNumberOfBlocksToSkipStateSaving: cachingConfig.MaxNumberOfBlocksToSkipStateSaving,
	// 	MaxAmountOfGasToSkipStateSaving:    cachingConfig.MaxAmountOfGasToSkipStateSaving,
	// 	StateScheme:                        cachingConfig.StateScheme,
	// 	StateHistory:                       cachingConfig.StateHistory,
	// }

	return &CachingConfig{
		TrieCleanCache: cachingConfig.TrieCleanCache,
		// NoPrefetch:                         baseConf.NoPrefetch,
		TrieDirtyCache: cachingConfig.TrieDirtyCache,
		Archive:        cachingConfig.Archive,
		TrieTimeLimit:  cachingConfig.TrieTimeLimit,
		BlockCount:     cachingConfig.BlockCount,
		BlockAge:       cachingConfig.BlockAge,
		SnapshotCache:  cachingConfig.SnapshotCache,
		// Preimages:                          baseConf.Preimages,
		SnapshotRestoreGasLimit:            cachingConfig.SnapshotRestoreGasLimit,
		MaxNumberOfBlocksToSkipStateSaving: cachingConfig.MaxNumberOfBlocksToSkipStateSaving,
		MaxAmountOfGasToSkipStateSaving:    cachingConfig.MaxAmountOfGasToSkipStateSaving,
		StateScheme:                        cachingConfig.StateScheme,
		StateHistory:                       cachingConfig.StateHistory,
	}
}

func (c *CachingConfig) validateStateScheme() error {
	// switch c.StateScheme {
	// case rawdb.HashScheme:
	// case rawdb.PathScheme:
	// 	if c.Archive {
	// 		return errors.New("archive cannot be set when using path as the state-scheme")
	// 	}
	// default:
	// 	return errors.New("Invalid StateScheme")
	// }
	return nil
}

func (c *CachingConfig) Validate() error {
	return c.validateStateScheme()
}

func WriteOrTestGenblock(chainDb kv.TemporalRwTx, domains *state2.SharedDomains, initData statetransfer.InitDataReader, chainConfig *chain.Config, initMessage *arbostypes.ParsedInitMessage, accountsPerSync uint) error {
	blockNumber, err := initData.GetNextBlockNumber()
	if err != nil {
		return err
	}
	storedGenHash, err := rawdb.ReadCanonicalHash(chainDb, blockNumber)
	if err != nil {
		return err
	}

	timestamp, prevHash := uint64(0), EmptyHash
	if blockNumber > 0 {
		prevHash, err = rawdb.ReadCanonicalHash(chainDb, blockNumber-1)
		if err != nil {
			return err
		}
		if prevHash == EmptyHash {
			return fmt.Errorf("block number %d not found in database", chainDb)
		}
		prevHeader := rawdb.ReadHeader(chainDb, prevHash, blockNumber-1)
		if prevHeader == nil {
			return fmt.Errorf("block header for block %d not found in database", chainDb)
		}
		timestamp = prevHeader.Time
	}

	reader := state.NewReaderV3(domains)
	ss := state.New(reader)
	ss.SetTrace(true)
	ibsa := state.NewArbitrum(ss)

	stateRoot, err := arbosState.InitializeArbosInDatabase(ibsa, domains, initData, chainConfig, initMessage, timestamp, accountsPerSync)
	if err != nil {
		return err
	}

	chainConfig.ChainName = "sepolia-rollup" // TODO must be parsed/obtained from config but has been rewritten on the way after parsing

	if storedGenHash == EmptyHash {
		// chainDb did not have genesis block. Initialize it.

		gen := core.GenesisBlockByChainName(chainConfig.ChainName)
		genBlock := arbosState.MakeGenesisBlock(prevHash, blockNumber, timestamp, stateRoot, chainConfig)
		err = core.WriteCustomGenesisBlock(chainDb, gen, genBlock, gen.Difficulty, chainConfig.ArbitrumChainParams.GenesisBlockNum, chainConfig)
		if err != nil {
			return fmt.Errorf("failed to write genesis block: %v", err)
		}
		log.Info("wrote genesis block", "number", blockNumber, "hash", genBlock.Hash(), "stateRoot", genBlock.Root())
		// } else if storedGenHash != blockHash {
		// 	return fmt.Errorf("database contains data inconsistent with initialization: database has genesis hash %v but we built genesis hash %v", storedGenHash, blockHash)
	} else {
		log.Crit("genesis block already exists", "number", blockNumber, "hash", storedGenHash)
		// TODO this recreation must be just reading this genesis from db if its really needed.
		// Otherwise its pointless genesis creation
		//genBlock := arbosState.MakeGenesisBlock(prevHash, blockNumber, timestamp, stateRoot, chainConfig)
		//blockHash := genBlock.Hash()
		//log.Info("recreated existing genesis block", "number", blockNumber, "hash", blockHash)
	}

	return nil
}

func TryReadStoredChainConfig(chainDb kv.Tx) *chain.Config {
	block0Hash, err := rawdb.ReadCanonicalHash(chainDb, 0)
	if err != nil || block0Hash == EmptyHash {
		return nil
	}
	cfg, err := rawdb.ReadChainConfig(chainDb, block0Hash)
	if err != nil {
		return nil
	}
	return cfg
}

var EmptyHash common.Hash

func WriteOrTestChainConfig(tx kv.TemporalRwTx, config *chain.Config) error {

	block0Hash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return err
	}
	if block0Hash == EmptyHash {
		return errors.New("block 0 not found")
	}
	storedConfig, err := rawdb.ReadChainConfig(tx, block0Hash)
	if err != nil {
		return fmt.Errorf("error reading chain config: %v", err)
	}
	if storedConfig == nil {
		return rawdb.WriteChainConfig(tx, block0Hash, config)
	}
	height := rawdb.ReadHeaderNumber(tx, rawdb.ReadHeadHeaderHash(tx))
	if height == nil {
		return errors.New("non empty chain config but empty chain")
	}
	cerr := storedConfig.CheckCompatible(config, *height)
	if cerr != nil {
		return fmt.Errorf("config compatibility check: %s", cerr.Error())
	}
	return rawdb.WriteChainConfig(tx, block0Hash, config)
}

func GetBlockChainSD(chainTx kv.TemporalRwDB, sd *state2.SharedDomains, chainConfig *chain.Config, txLookupLimit uint64) (core.BlockChain, error) {
	engine := arbos.Engine{
		IsSequencer: true, // TODO
	}

	vmConfig := vm.Config{
		//	EnablePreimageRecording: false,
	}

	return core.NewBlockChainSD(chainTx, sd, chainConfig, nil, engine, vmConfig, shouldPreserveFalse, &txLookupLimit)
}

// Commits rwtx in the case of success write
func WriteOrTestGenesis(chainRwTx kv.TemporalRwTx, initData statetransfer.InitDataReader, chainConfig *chain.Config, initMessage *arbostypes.ParsedInitMessage, txLookupLimit uint64, accountsPerSync uint, domains *state2.SharedDomains) error {
	err := WriteOrTestGenblock(chainRwTx, domains, initData, chainConfig, initMessage, accountsPerSync)
	if err != nil {
		return err
	}
	return WriteOrTestChainConfig(chainRwTx, chainConfig)
}

// TODO remove
func WriteOrTestBlockChain(db kv.TemporalRwDB, initData statetransfer.InitDataReader, chainConfig *chain.Config, initMessage *arbostypes.ParsedInitMessage, txLookupLimit uint64, accountsPerSync uint, domains *state2.SharedDomains) (core.BlockChain, error) {
	// emptyBlockChain := rawdb.ReadHeadHeader(chainRwTx) == nil
	// if !emptyBlockChain && (cacheConfig.StateScheme == rawdb.PathScheme) {
	// 	// When using path scheme, and the stored state trie is not empty,
	// 	// WriteOrTestGenBlock is not able to recover EmptyRootHash state trie node.
	// 	// In that case Nitro doesn't test genblock, but just returns the BlockChain.
	// 	return GetBlockChain(chainRwTx, cacheConfig, chainConfig, txLookupLimit)
	// }

	chainRwTx, err := db.BeginTemporalRw(context.Background())
	if err != nil {
		return nil, err
	}

	err = WriteOrTestGenblock(chainRwTx, domains, initData, chainConfig, initMessage, accountsPerSync)
	if err != nil {
		return nil, err
	}
	err = WriteOrTestChainConfig(chainRwTx, chainConfig)
	if err != nil {
		return nil, err
	}
	if err = chainRwTx.Commit(); err != nil {
		return nil, err
	}
	return GetBlockChainSD(db, domains, chainConfig, txLookupLimit)
}

// Don't preserve reorg'd out blocks
func shouldPreserveFalse(_ *types.Header) bool {
	return false
}

var DefaultSequencerConfig = SequencerConfig{
	Enable:                      false,
	MaxBlockSpeed:               time.Millisecond * 250,
	MaxRevertGasReject:          0,
	MaxAcceptableTimestampDelta: time.Hour,
	SenderWhitelist:             []string{},
	Forwarder:                   DefaultSequencerForwarderConfig,
	QueueSize:                   1024,
	QueueTimeout:                time.Second * 12,
	NonceCacheSize:              1024,
	// 95% of the default batch poster limit, leaving 5KB for headers and such
	// This default is overridden for L3 chains in applyChainParameters in cmd/nitro/nitro.go
	MaxTxDataSize:                95000,
	NonceFailureCacheSize:        1024,
	NonceFailureCacheExpiry:      time.Second,
	ExpectedSurplusSoftThreshold: "default",
	ExpectedSurplusHardThreshold: "default",
	EnableProfiling:              false,
}

func SequencerConfigAddOptions(prefix string, f *flag.FlagSet) {
	f.Bool(prefix+".enable", DefaultSequencerConfig.Enable, "act and post to l1 as sequencer")
	f.Duration(prefix+".max-block-speed", DefaultSequencerConfig.MaxBlockSpeed, "minimum delay between blocks (sets a maximum speed of block production)")
	f.Uint64(prefix+".max-revert-gas-reject", DefaultSequencerConfig.MaxRevertGasReject, "maximum gas executed in a revert for the sequencer to reject the transaction instead of posting it (anti-DOS)")
	f.Duration(prefix+".max-acceptable-timestamp-delta", DefaultSequencerConfig.MaxAcceptableTimestampDelta, "maximum acceptable time difference between the local time and the latest L1 block's timestamp")
	f.StringSlice(prefix+".sender-whitelist", DefaultSequencerConfig.SenderWhitelist, "comma separated whitelist of authorized senders (if empty, everyone is allowed)")
	AddOptionsForSequencerForwarderConfig(prefix+".forwarder", f)
	f.Int(prefix+".queue-size", DefaultSequencerConfig.QueueSize, "size of the pending tx queue")
	f.Duration(prefix+".queue-timeout", DefaultSequencerConfig.QueueTimeout, "maximum amount of time transaction can wait in queue")
	f.Int(prefix+".nonce-cache-size", DefaultSequencerConfig.NonceCacheSize, "size of the tx sender nonce cache")
	f.Int(prefix+".max-tx-data-size", DefaultSequencerConfig.MaxTxDataSize, "maximum transaction size the sequencer will accept")
	f.Int(prefix+".nonce-failure-cache-size", DefaultSequencerConfig.NonceFailureCacheSize, "number of transactions with too high of a nonce to keep in memory while waiting for their predecessor")
	f.Duration(prefix+".nonce-failure-cache-expiry", DefaultSequencerConfig.NonceFailureCacheExpiry, "maximum amount of time to wait for a predecessor before rejecting a tx with nonce too high")
	f.String(prefix+".expected-surplus-soft-threshold", DefaultSequencerConfig.ExpectedSurplusSoftThreshold, "if expected surplus is lower than this value, warnings are posted")
	f.String(prefix+".expected-surplus-hard-threshold", DefaultSequencerConfig.ExpectedSurplusHardThreshold, "if expected surplus is lower than this value, new incoming transactions will be denied")
	f.Bool(prefix+".enable-profiling", DefaultSequencerConfig.EnableProfiling, "enable CPU profiling and tracing")
}

type SequencerConfig struct {
	Enable                          bool            `koanf:"enable"`
	MaxBlockSpeed                   time.Duration   `koanf:"max-block-speed" reload:"hot"`
	MaxRevertGasReject              uint64          `koanf:"max-revert-gas-reject" reload:"hot"`
	MaxAcceptableTimestampDelta     time.Duration   `koanf:"max-acceptable-timestamp-delta" reload:"hot"`
	SenderWhitelist                 []string        `koanf:"sender-whitelist"`
	Forwarder                       ForwarderConfig `koanf:"forwarder"`
	QueueSize                       int             `koanf:"queue-size"`
	QueueTimeout                    time.Duration   `koanf:"queue-timeout" reload:"hot"`
	NonceCacheSize                  int             `koanf:"nonce-cache-size" reload:"hot"`
	MaxTxDataSize                   int             `koanf:"max-tx-data-size" reload:"hot"`
	NonceFailureCacheSize           int             `koanf:"nonce-failure-cache-size" reload:"hot"`
	NonceFailureCacheExpiry         time.Duration   `koanf:"nonce-failure-cache-expiry" reload:"hot"`
	ExpectedSurplusSoftThreshold    string          `koanf:"expected-surplus-soft-threshold" reload:"hot"`
	ExpectedSurplusHardThreshold    string          `koanf:"expected-surplus-hard-threshold" reload:"hot"`
	EnableProfiling                 bool            `koanf:"enable-profiling" reload:"hot"`
	ExpectedSurplusSoftThresholdInt int
	ExpectedSurplusHardThresholdInt int
}

func (c *SequencerConfig) Validate() error {
	for _, address := range c.SenderWhitelist {
		if len(address) == 0 {
			continue
		}
		if !common.IsHexAddress(address) {
			return fmt.Errorf("sequencer sender whitelist entry \"%v\" is not a valid address", address)
		}
	}
	var err error
	if c.ExpectedSurplusSoftThreshold != "default" {
		if c.ExpectedSurplusSoftThresholdInt, err = strconv.Atoi(c.ExpectedSurplusSoftThreshold); err != nil {
			return fmt.Errorf("invalid expected-surplus-soft-threshold value provided in batchposter config %w", err)
		}
	}
	if c.ExpectedSurplusHardThreshold != "default" {
		if c.ExpectedSurplusHardThresholdInt, err = strconv.Atoi(c.ExpectedSurplusHardThreshold); err != nil {
			return fmt.Errorf("invalid expected-surplus-hard-threshold value provided in batchposter config %w", err)
		}
	}
	if c.ExpectedSurplusSoftThresholdInt < c.ExpectedSurplusHardThresholdInt {
		return errors.New("expected-surplus-soft-threshold cannot be lower than expected-surplus-hard-threshold")
	}
	if c.MaxTxDataSize > arbostypes.MaxL2MessageSize-50000 {
		return errors.New("max-tx-data-size too large for MaxL2MessageSize")
	}
	return nil
}

type ForwarderConfig struct {
	ConnectionTimeout     time.Duration `koanf:"connection-timeout"`
	IdleConnectionTimeout time.Duration `koanf:"idle-connection-timeout"`
	MaxIdleConnections    int           `koanf:"max-idle-connections"`
	RedisUrl              string        `koanf:"redis-url"`
	UpdateInterval        time.Duration `koanf:"update-interval"`
	RetryInterval         time.Duration `koanf:"retry-interval"`
}

var DefaultNodeForwarderConfig = ForwarderConfig{
	ConnectionTimeout:     30 * time.Second,
	IdleConnectionTimeout: 15 * time.Second,
	MaxIdleConnections:    1,
	RedisUrl:              "",
	UpdateInterval:        time.Second,
	RetryInterval:         100 * time.Millisecond,
}

var DefaultSequencerForwarderConfig = ForwarderConfig{
	ConnectionTimeout:     30 * time.Second,
	IdleConnectionTimeout: 60 * time.Second,
	MaxIdleConnections:    100,
	RedisUrl:              "",
	UpdateInterval:        time.Second,
	RetryInterval:         100 * time.Millisecond,
}
