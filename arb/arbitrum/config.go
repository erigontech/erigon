package arbitrum

import (
	"time"

	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/params"
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
	RPCEVMTimeout:           ethconfig.Defaults.RPCEVMTimeout, // 5 seconds
	BloomBitsBlocks:         params.BloomBitsBlocks * 4,       // we generally have smaller blocks
	BloomConfirms:           params.BloomConfirms,
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
