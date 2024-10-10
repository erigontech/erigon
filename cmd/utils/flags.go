// Copyright 2015 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// Package utils contains internal helper functions for go-ethereum commands.
package utils

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/cmp"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/common/metrics"
	downloadercfg2 "github.com/gateway-fm/cdk-erigon-lib/downloader/downloadercfg"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/txpool/txpoolcfg"
	common2 "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash/ethashcfg"
	"github.com/ledgerwatch/erigon/eth/gasprice/gaspricecfg"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloadernat"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/p2p/netutil"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
)

// These are all the command line flags we support.
// If you add to this list, please remember to include the
// flag in the appropriate command definition.
//
// The flags are defined here so their names and help texts
// are the same for all commands.

var (
	// General settings
	DataDirFlag = DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for the databases",
		Value: DirectoryString(paths.DefaultDataDir()),
	}

	AncientFlag = DirectoryFlag{
		Name:  "datadir.ancient",
		Usage: "Data directory for ancient chain segments (default = inside chaindata)",
	}
	MinFreeDiskSpaceFlag = DirectoryFlag{
		Name:  "datadir.minfreedisk",
		Usage: "Minimum free disk space in MB, once reached triggers auto shut down (default = --cache.gc converted to MB, 0 = disabled)",
	}
	NetworkIdFlag = cli.Uint64Flag{
		Name:  "networkid",
		Usage: "Explicitly set network id (integer)(For testnets: use --chain <testnet_name> instead)",
		Value: ethconfig.Defaults.NetworkID,
	}
	DeveloperPeriodFlag = cli.IntFlag{
		Name:  "dev.period",
		Usage: "Block period to use in developer mode (0 = mine only if transaction pending)",
	}
	ChainFlag = cli.StringFlag{
		Name:  "chain",
		Usage: "Name of the testnet to join",
		Value: networkname.MainnetChainName,
	}
	IdentityFlag = cli.StringFlag{
		Name:  "identity",
		Usage: "Custom node name",
	}
	WhitelistFlag = cli.StringFlag{
		Name:  "whitelist",
		Usage: "Comma separated block number-to-hash mappings to enforce (<number>=<hash>)",
	}
	OverrideShanghaiTime = BigFlag{
		Name:  "override.shanghaiTime",
		Usage: "Manually specify Shanghai fork time, overriding the bundled setting",
	}
	// Ethash settings
	EthashCachesInMemoryFlag = cli.IntFlag{
		Name:  "ethash.cachesinmem",
		Usage: "Number of recent ethash caches to keep in memory (16MB each)",
		Value: ethconfig.Defaults.Ethash.CachesInMem,
	}
	EthashCachesLockMmapFlag = cli.BoolFlag{
		Name:  "ethash.cacheslockmmap",
		Usage: "Lock memory maps of recent ethash caches",
	}
	EthashDatasetDirFlag = DirectoryFlag{
		Name:  "ethash.dagdir",
		Usage: "Directory to store the ethash mining DAGs",
		Value: DirectoryString(ethconfig.Defaults.Ethash.DatasetDir),
	}
	EthashDatasetsLockMmapFlag = cli.BoolFlag{
		Name:  "ethash.dagslockmmap",
		Usage: "Lock memory maps for recent ethash mining DAGs",
	}
	SnapshotFlag = cli.BoolFlag{
		Name:  "snapshots",
		Usage: `Default: use snapshots "true" for Mainnet, Goerli, Gnosis Chain and Chiado. use snapshots "false" in all other cases`,
		Value: true,
	}
	ExternalConsensusFlag = cli.BoolFlag{
		Name:  "externalcl",
		Usage: "enables external consensus",
	}
	// Transaction pool settings
	TxPoolDisableFlag = cli.BoolFlag{
		Name:  "txpool.disable",
		Usage: "experimental external pool and block producer, see ./cmd/txpool/readme.md for more info. Disabling internal txpool and block producer.",
	}
	TxPoolLocalsFlag = cli.StringFlag{
		Name:  "txpool.locals",
		Usage: "Comma separated accounts to treat as locals (no flush, priority inclusion)",
	}
	TxPoolNoLocalsFlag = cli.BoolFlag{
		Name:  "txpool.nolocals",
		Usage: "Disables price exemptions for locally submitted transactions",
	}
	TxPoolPriceLimitFlag = cli.Uint64Flag{
		Name:  "txpool.pricelimit",
		Usage: "Minimum gas price (fee cap) limit to enforce for acceptance into the pool",
		Value: ethconfig.Defaults.DeprecatedTxPool.PriceLimit,
	}
	TxPoolPriceBumpFlag = cli.Uint64Flag{
		Name:  "txpool.pricebump",
		Usage: "Price bump percentage to replace an already existing transaction",
		Value: txpoolcfg.DefaultConfig.PriceBump,
	}
	TxPoolAccountSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.accountslots",
		Usage: "Minimum number of executable transaction slots guaranteed per account",
		Value: ethconfig.Defaults.DeprecatedTxPool.AccountSlots,
	}
	TxPoolGlobalSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.globalslots",
		Usage: "Maximum number of executable transaction slots for all accounts",
		Value: ethconfig.Defaults.DeprecatedTxPool.GlobalSlots,
	}
	TxPoolGlobalBaseFeeSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.globalbasefeeslots",
		Usage: "Maximum number of non-executable transactions where only not enough baseFee",
		Value: ethconfig.Defaults.DeprecatedTxPool.GlobalQueue,
	}
	TxPoolAccountQueueFlag = cli.Uint64Flag{
		Name:  "txpool.accountqueue",
		Usage: "Maximum number of non-executable transaction slots permitted per account",
		Value: ethconfig.Defaults.DeprecatedTxPool.AccountQueue,
	}
	TxPoolGlobalQueueFlag = cli.Uint64Flag{
		Name:  "txpool.globalqueue",
		Usage: "Maximum number of non-executable transaction slots for all accounts",
		Value: ethconfig.Defaults.DeprecatedTxPool.GlobalQueue,
	}
	TxPoolLifetimeFlag = cli.DurationFlag{
		Name:  "txpool.lifetime",
		Usage: "Maximum amount of time non-executable transaction are queued",
		Value: ethconfig.Defaults.DeprecatedTxPool.Lifetime,
	}
	TxPoolTraceSendersFlag = cli.StringFlag{
		Name:  "txpool.trace.senders",
		Usage: "Comma separared list of addresses, whoes transactions will traced in transaction pool with debug printing",
		Value: "",
	}
	TxPoolCommitEveryFlag = cli.DurationFlag{
		Name:  "txpool.commit.every",
		Usage: "How often transactions should be committed to the storage",
		Value: txpoolcfg.DefaultConfig.CommitEvery,
	}
	// Miner settings
	MiningEnabledFlag = cli.BoolFlag{
		Name:  "mine",
		Usage: "Enable mining",
	}
	ProposingDisableFlag = cli.BoolFlag{
		Name:  "proposer.disable",
		Usage: "Disables PoS proposer",
	}
	MinerNotifyFlag = cli.StringFlag{
		Name:  "miner.notify",
		Usage: "Comma separated HTTP URL list to notify of new work packages",
	}
	MinerGasLimitFlag = cli.Uint64Flag{
		Name:  "miner.gaslimit",
		Usage: "Target gas limit for mined blocks",
		Value: ethconfig.Defaults.Miner.GasLimit,
	}
	MinerGasPriceFlag = BigFlag{
		Name:  "miner.gasprice",
		Usage: "Minimum gas price for mining a transaction",
		Value: ethconfig.Defaults.Miner.GasPrice,
	}
	MinerEtherbaseFlag = cli.StringFlag{
		Name:  "miner.etherbase",
		Usage: "Public address for block mining rewards",
		Value: "0",
	}
	MinerSigningKeyFileFlag = cli.StringFlag{
		Name:  "miner.sigfile",
		Usage: "Private key to sign blocks with",
		Value: "",
	}
	MinerExtraDataFlag = cli.StringFlag{
		Name:  "miner.extradata",
		Usage: "Block extra data set by the miner (default = client version)",
	}
	MinerRecommitIntervalFlag = cli.DurationFlag{
		Name:  "miner.recommit",
		Usage: "Time interval to recreate the block being mined",
		Value: ethconfig.Defaults.Miner.Recommit,
	}
	MinerNoVerfiyFlag = cli.BoolFlag{
		Name:  "miner.noverify",
		Usage: "Disable remote sealing verification",
	}
	VMEnableDebugFlag = cli.BoolFlag{
		Name:  "vmdebug",
		Usage: "Record information useful for VM and contract debugging",
	}
	InsecureUnlockAllowedFlag = cli.BoolFlag{
		Name:  "allow-insecure-unlock",
		Usage: "Allow insecure account unlocking when account-related RPCs are exposed by http",
	}
	RPCGlobalGasCapFlag = cli.Uint64Flag{
		Name:  "rpc.gascap",
		Usage: "Sets a cap on gas that can be used in eth_call/estimateGas (0=infinite)",
		Value: ethconfig.Defaults.RPCGasCap,
	}
	RPCGlobalTxFeeCapFlag = cli.Float64Flag{
		Name:  "rpc.txfeecap",
		Usage: "Sets a cap on transaction fee (in ether) that can be sent via the RPC APIs (0 = no cap)",
		Value: ethconfig.Defaults.RPCTxFeeCap,
	}
	// Logging and debug settings
	EthStatsURLFlag = cli.StringFlag{
		Name:  "ethstats",
		Usage: "Reporting URL of a ethstats service (nodename:secret@host:port)",
		Value: "",
	}
	FakePoWFlag = cli.BoolFlag{
		Name:  "fakepow",
		Usage: "Disables proof-of-work verification",
	}
	// RPC settings
	IPCDisabledFlag = cli.BoolFlag{
		Name:  "ipcdisable",
		Usage: "Disable the IPC-RPC server",
	}
	IPCPathFlag = DirectoryFlag{
		Name:  "ipcpath",
		Usage: "Filename for IPC socket/pipe within the datadir (explicit paths escape it)",
	}
	GraphQLEnabledFlag = cli.BoolFlag{
		Name:  "graphql",
		Usage: "Enable the graphql endpoint",
		Value: nodecfg.DefaultConfig.GraphQLEnabled,
	}
	HTTPEnabledFlag = cli.BoolFlag{
		Name:  "http",
		Usage: "HTTP-RPC server (enabled by default). Use --http=false to disable it",
		Value: true,
	}
	HTTPListenAddrFlag = cli.StringFlag{
		Name:  "http.addr",
		Usage: "HTTP-RPC server listening interface",
		Value: nodecfg.DefaultHTTPHost,
	}
	HTTPPortFlag = cli.IntFlag{
		Name:  "http.port",
		Usage: "HTTP-RPC server listening port",
		Value: nodecfg.DefaultHTTPPort,
	}
	AuthRpcAddr = cli.StringFlag{
		Name:  "authrpc.addr",
		Usage: "HTTP-RPC server listening interface for the Engine API",
		Value: nodecfg.DefaultHTTPHost,
	}
	AuthRpcPort = cli.UintFlag{
		Name:  "authrpc.port",
		Usage: "HTTP-RPC server listening port for the Engine API",
		Value: nodecfg.DefaultAuthRpcPort,
	}

	JWTSecretPath = cli.StringFlag{
		Name:  "authrpc.jwtsecret",
		Usage: "Path to the token that ensures safe connection between CL and EL",
		Value: "",
	}

	HttpCompressionFlag = cli.BoolFlag{
		Name:  "http.compression",
		Usage: "Enable compression over HTTP-RPC",
	}
	WsCompressionFlag = cli.BoolFlag{
		Name:  "ws.compression",
		Usage: "Enable compression over WebSocket",
	}
	HTTPCORSDomainFlag = cli.StringFlag{
		Name:  "http.corsdomain",
		Usage: "Comma separated list of domains from which to accept cross origin requests (browser enforced)",
		Value: "*",
	}
	HTTPVirtualHostsFlag = cli.StringFlag{
		Name:  "http.vhosts",
		Usage: "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.",
		Value: strings.Join(nodecfg.DefaultConfig.HTTPVirtualHosts, ","),
	}
	AuthRpcVirtualHostsFlag = cli.StringFlag{
		Name:  "authrpc.vhosts",
		Usage: "Comma separated list of virtual hostnames from which to accept Engine API requests (server enforced). Accepts '*' wildcard.",
		Value: strings.Join(nodecfg.DefaultConfig.HTTPVirtualHosts, ","),
	}
	HTTPApiFlag = cli.StringFlag{
		Name:  "http.api",
		Usage: "API's offered over the HTTP-RPC interface",
		Value: "eth,erigon,engine",
	}
	L2ChainIdFlag = cli.Uint64Flag{
		Name:  "zkevm.l2-chain-id",
		Usage: "L2 chain ID",
		Value: 0,
	}
	L2RpcUrlFlag = cli.StringFlag{
		Name:  "zkevm.l2-sequencer-rpc-url",
		Usage: "Upstream L2 node RPC endpoint",
		Value: "",
	}
	L2DataStreamerUrlFlag = cli.StringFlag{
		Name:  "zkevm.l2-datastreamer-url",
		Usage: "L2 datastreamer endpoint",
		Value: "",
	}
	L2DataStreamerTimeout = cli.StringFlag{
		Name:  "zkevm.l2-datastreamer-timeout",
		Usage: "The time to wait for data to arrive from the stream before reporting an error (0s doesn't check)",
		Value: "0s",
	}
	L1SyncStartBlock = cli.Uint64Flag{
		Name:  "zkevm.l1-sync-start-block",
		Usage: "Designed for recovery of the network from the L1 batch data, slower mode of operation than the datastream.  If set the datastream will not be used",
		Value: 0,
	}
	L1SyncStopBatch = cli.Uint64Flag{
		Name:  "zkevm.l1-sync-stop-batch",
		Usage: "Designed mainly for debugging, this will stop the L1 sync going on for too long when you only want to pull a handful of batches from the L1 during recovery",
		Value: 0,
	}
	L1ChainIdFlag = cli.Uint64Flag{
		Name:  "zkevm.l1-chain-id",
		Usage: "Ethereum L1 chain ID",
		Value: 0,
	}
	L1RpcUrlFlag = cli.StringFlag{
		Name:  "zkevm.l1-rpc-url",
		Usage: "Ethereum L1 RPC endpoint",
		Value: "",
	}
	L1CacheEnabledFlag = cli.BoolFlag{
		Name:  "zkevm.l1-cache-enabled",
		Usage: "Enable the L1 cache",
		Value: false,
	}
	L1CachePortFlag = cli.UintFlag{
		Name:  "zkevm.l1-cache-port",
		Usage: "The port used for the L1 cache",
		Value: 6969,
	}
	AddressSequencerFlag = cli.StringFlag{
		Name:  "zkevm.address-sequencer",
		Usage: "Sequencer address",
		Value: "",
	}
	AddressAdminFlag = cli.StringFlag{
		Name:  "zkevm.address-admin",
		Usage: "Admin address (Deprecated)",
		Value: "",
	}
	AddressRollupFlag = cli.StringFlag{
		Name:  "zkevm.address-rollup",
		Usage: "Rollup address",
		Value: "",
	}
	AddressZkevmFlag = cli.StringFlag{
		Name:  "zkevm.address-zkevm",
		Usage: "Zkevm address",
		Value: "",
	}
	AddressGerManagerFlag = cli.StringFlag{
		Name:  "zkevm.address-ger-manager",
		Usage: "Ger Manager address",
		Value: "",
	}
	L1RollupIdFlag = cli.Uint64Flag{
		Name:  "zkevm.l1-rollup-id",
		Usage: "Ethereum L1 Rollup ID",
		Value: 1,
	}
	L1BlockRangeFlag = cli.Uint64Flag{
		Name:  "zkevm.l1-block-range",
		Usage: "Ethereum L1 block range used to filter verifications and sequences",
		Value: 20000,
	}
	L1QueryDelayFlag = cli.Uint64Flag{
		Name:     "zkevm.l1-query-delay",
		Required: false,
		Usage:    "Ethereum L1 delay between queries for verifications and sequences - in milliseconds",
		Value:    6000,
	}
	L1HighestBlockTypeFlag = cli.StringFlag{
		Name:  "zkevm.l1-highest-block-type",
		Usage: "The type of the highest block in the L1 chain. latest, safe, or finalized",
		Value: "finalized",
	}
	L1MaticContractAddressFlag = cli.StringFlag{
		Name:  "zkevm.l1-matic-contract-address",
		Usage: "Ethereum L1 Matic contract address",
		Value: "0x0",
	}
	L1FirstBlockFlag = cli.Uint64Flag{
		Name:  "zkevm.l1-first-block",
		Usage: "First block to start syncing from on the L1",
		Value: 0,
	}
	L1FinalizedBlockRequirementFlag = cli.Uint64Flag{
		Name:  "zkevm.l1-finalized-block-requirement",
		Usage: "The given block must be finalized before sequencer L1 sync continues",
		Value: 0,
	}
	L1ContractAddressCheckFlag = cli.BoolFlag{
		Name:  "zkevm.l1-contract-address-check",
		Usage: "Check the contract address on the L1",
		Value: true,
	}
	L1ContractAddressRetrieveFlag = cli.BoolFlag{
		Name:  "zkevm.l1-contract-address-retrieve",
		Usage: "Retrieve the contracts addresses from the L1",
		Value: true,
	}
	RebuildTreeAfterFlag = cli.Uint64Flag{
		Name:  "zkevm.rebuild-tree-after",
		Usage: "Rebuild the state tree after this many blocks behind",
		Value: 10000,
	}
	IncrementTreeAlways = cli.BoolFlag{
		Name:  "zkevm.increment-tree-always",
		Usage: "Increment the state tree, never rebuild",
		Value: false,
	}
	SmtRegenerateInMemory = cli.BoolFlag{
		Name:  "zkevm.smt-regenerate-in-memory",
		Usage: "Regenerate the SMT in memory (requires a lot of RAM for most chains)",
		Value: false,
	}
	SequencerBlockSealTime = cli.StringFlag{
		Name:  "zkevm.sequencer-block-seal-time",
		Usage: "Block seal time. Defaults to 6s",
		Value: "6s",
	}
	SequencerBatchSealTime = cli.StringFlag{
		Name:  "zkevm.sequencer-batch-seal-time",
		Usage: "Batch seal time. Defaults to 12s",
		Value: "12s",
	}
	SequencerBatchVerificationTimeout = cli.StringFlag{
		Name:  "zkevm.sequencer-batch-verification-timeout",
		Usage: "This is a maximum time that a batch verification could take in terms of executors' errors. Including retries. This could be interpreted as `maximum that that the sequencer can run without executor`. Setting it to 0s will mean infinite timeout. Defaults to 30min",
		Value: "30m",
	}
	SequencerBatchVerificationRetries = cli.StringFlag{
		Name:  "zkevm.sequencer-batch-verification-retries",
		Usage: "Number of attempts that a batch will re-run in case of an internal (not executors') error. This could be interpreted as `maximum attempts to send a batch for verification`. Setting it to -1 will mean unlimited attempts. Defaults to 3",
		Value: "3",
	}
	SequencerTimeoutOnEmptyTxPool = cli.StringFlag{
		Name:  "zkevm.sequencer-timeout-on-empty-tx-pool",
		Usage: "Timeout before requesting txs from the txpool if none were found before. Defaults to 250ms",
		Value: "250ms",
	}
	SequencerHaltOnBatchNumber = cli.Uint64Flag{
		Name:  "zkevm.sequencer-halt-on-batch-number",
		Usage: "Halt the sequencer on this batch number",
		Value: 0,
	}
	SequencerResequence = cli.BoolFlag{
		Name:  "zkevm.sequencer-resequence",
		Usage: "When enabled, the sequencer will automatically resequence unseen batches stored in data stream",
		Value: false,
	}
	SequencerResequenceStrict = cli.BoolFlag{
		Name:  "zkevm.sequencer-resequence-strict",
		Usage: "Strictly resequence the rolledback batches",
		Value: true,
	}
	SequencerResequenceReuseL1InfoIndex = cli.BoolFlag{
		Name:  "zkevm.sequencer-resequence-reuse-l1-info-index",
		Usage: "Reuse the L1 info index for resequencing",
		Value: true,
	}
	ExecutorUrls = cli.StringFlag{
		Name:  "zkevm.executor-urls",
		Usage: "A comma separated list of grpc addresses that host executors",
		Value: "",
	}
	ExecutorStrictMode = cli.BoolFlag{
		Name:  "zkevm.executor-strict",
		Usage: "Defaulted to true to ensure you must set some executor URLs, bypass this restriction by setting to false",
		Value: true,
	}
	ExecutorRequestTimeout = cli.DurationFlag{
		Name:  "zkevm.executor-request-timeout",
		Usage: "The timeout for the executor request",
		Value: 60 * time.Second,
	}
	DatastreamNewBlockTimeout = cli.DurationFlag{
		Name:  "zkevm.datastream-new-block-timeout",
		Usage: "The timeout for the executor request",
		Value: 500 * time.Millisecond,
	}

	WitnessMemdbSize = DatasizeFlag{
		Name:  "zkevm.witness-memdb-size",
		Usage: "A size of the memdb used on witness generation in format \"2GB\". Might fail generation for older batches if not enough for the unwind.",
		Value: datasizeFlagValue(2 * datasize.GB),
	}
	ExecutorMaxConcurrentRequests = cli.IntFlag{
		Name:  "zkevm.executor-max-concurrent-requests",
		Usage: "The maximum number of concurrent requests to the executor",
		Value: 1,
	}
	RpcRateLimitsFlag = cli.IntFlag{
		Name:  "zkevm.rpc-ratelimit",
		Usage: "RPC rate limit in requests per second.",
		Value: 0,
	}
	RpcGetBatchWitnessConcurrencyLimitFlag = cli.IntFlag{
		Name:  "zkevm.rpc-get-batch-witness-concurrency-limit",
		Usage: "The maximum number of concurrent requests to the executor for getBatchWitness.",
		Value: 1,
	}
	DatastreamVersionFlag = cli.IntFlag{
		Name:  "zkevm.datastream-version",
		Usage: "Stream version indicator 1: PreBigEndian, 2: BigEndian.",
		Value: 2,
	}
	DataStreamPort = cli.UintFlag{
		Name:  "zkevm.data-stream-port",
		Usage: "Define the port used for the zkevm data stream",
		Value: 0,
	}
	DataStreamHost = cli.StringFlag{
		Name:  "zkevm.data-stream-host",
		Usage: "Define the host used for the zkevm data stream",
		Value: "",
	}
	DataStreamWriteTimeout = cli.DurationFlag{
		Name:  "zkevm.data-stream-writeTimeout",
		Usage: "Define the TCP write timeout when sending data to a datastream client",
		Value: 20 * time.Second,
	}
	DataStreamInactivityTimeout = cli.DurationFlag{
		Name:  "zkevm.data-stream-inactivity-timeout",
		Usage: "Define the inactivity timeout when interacting with a data stream server",
		Value: 10 * time.Minute,
	}
	DataStreamInactivityCheckInterval = cli.DurationFlag{
		Name:  "zkevm.data-stream-inactivity-check-interval",
		Usage: "Define the inactivity check interval timeout when interacting with a data stream server",
		Value: 5 * time.Minute,
	}
	Limbo = cli.BoolFlag{
		Name:  "zkevm.limbo",
		Usage: "Enable limbo processing on batches that failed verification",
		Value: false,
	}
	AllowFreeTransactions = cli.BoolFlag{
		Name:  "zkevm.allow-free-transactions",
		Usage: "Allow the sequencer to proceed transactions with 0 gas price",
		Value: false,
	}
	AllowPreEIP155Transactions = cli.BoolFlag{
		Name:  "zkevm.allow-pre-eip155-transactions",
		Usage: "Allow the sequencer to proceed pre-EIP155 transactions",
		Value: false,
	}
	EffectiveGasPriceForEthTransfer = cli.Float64Flag{
		Name:  "zkevm.effective-gas-price-eth-transfer",
		Usage: "Set the effective gas price in percentage for transfers",
		Value: 1,
	}
	EffectiveGasPriceForErc20Transfer = cli.Float64Flag{
		Name:  "zkevm.effective-gas-price-erc20-transfer",
		Usage: "Set the effective gas price in percentage for transfers",
		Value: 1,
	}
	EffectiveGasPriceForContractInvocation = cli.Float64Flag{
		Name:  "zkevm.effective-gas-price-contract-invocation",
		Usage: "Set the effective gas price in percentage for contract invocation",
		Value: 1,
	}
	EffectiveGasPriceForContractDeployment = cli.Float64Flag{
		Name:  "zkevm.effective-gas-price-contract-deployment",
		Usage: "Set the effective gas price in percentage for contract deployment",
		Value: 1,
	}
	DefaultGasPrice = cli.Uint64Flag{
		Name:  "zkevm.default-gas-price",
		Usage: "Set the default/min gas price",
		// 0.01 gwei
		Value: 10000000,
	}
	MaxGasPrice = cli.Uint64Flag{
		Name:  "zkevm.max-gas-price",
		Usage: "Set the max gas price",
		Value: 0,
	}
	GasPriceFactor = cli.Float64Flag{
		Name:  "zkevm.gas-price-factor",
		Usage: "Apply factor to L1 gas price to calculate l2 gasPrice",
		Value: 1,
	}
	WitnessFullFlag = cli.BoolFlag{
		Name:  "zkevm.witness-full",
		Usage: "Enable/Diable witness full",
		Value: true,
	}
	SyncLimit = cli.UintFlag{
		Name:  "zkevm.sync-limit",
		Usage: "Limit the number of blocks to sync, this will halt batches and execution to this number but keep the node active",
		Value: 0,
	}
	PoolManagerUrl = cli.StringFlag{
		Name:  "zkevm.pool-manager-url",
		Usage: "The URL of the pool manager. If set, eth_sendRawTransaction will be redirected there.",
		Value: "",
	}
	TxPoolRejectSmartContractDeployments = cli.BoolFlag{
		Name:  "zkevm.reject-smart-contract-deployments",
		Usage: "Reject smart contract deployments",
		Value: false,
	}
	DisableVirtualCounters = cli.BoolFlag{
		Name:  "zkevm.disable-virtual-counters",
		Usage: "Disable the virtual counters. This has an effect on on sequencer node and when external executor is not enabled.",
		Value: false,
	}
	ExecutorPayloadOutput = cli.StringFlag{
		Name:  "zkevm.executor-payload-output",
		Usage: "Output the payload of the executor, serialised requests stored to disk by batch number",
		Value: "",
	}
	DAUrl = cli.StringFlag{
		Name:  "zkevm.da-url",
		Usage: "The URL of the data availability service",
		Value: "",
	}
	VirtualCountersSmtReduction = cli.Float64Flag{
		Name:  "zkevm.virtual-counters-smt-reduction",
		Usage: "The multiplier to reduce the SMT depth by when calculating virtual counters",
		Value: 0.6,
	}
	DebugTimers = cli.BoolFlag{
		Name:  "debug.timers",
		Usage: "Enable debug timers",
		Value: false,
	}
	DebugNoSync = cli.BoolFlag{
		Name:  "debug.no-sync",
		Usage: "Disable syncing",
		Value: false,
	}
	DebugLimit = cli.UintFlag{
		Name:  "debug.limit",
		Usage: "Limit the number of blocks to sync",
		Value: 0,
	}
	DebugStep = cli.UintFlag{
		Name:  "debug.step",
		Usage: "Number of blocks to process each run of the stage loop",
	}
	DebugStepAfter = cli.UintFlag{
		Name:  "debug.step-after",
		Usage: "Start incrementing by debug.step after this block",
	}
	RpcBatchConcurrencyFlag = cli.UintFlag{
		Name:  "rpc.batch.concurrency",
		Usage: "Does limit amount of goroutines to process 1 batch request. Means 1 bach request can't overload server. 1 batch still can have unlimited amount of request",
		Value: 2,
	}
	RpcStreamingDisableFlag = cli.BoolFlag{
		Name:  "rpc.streaming.disable",
		Usage: "Erigon has enalbed json streaming for some heavy endpoints (like trace_*). It's treadoff: greatly reduce amount of RAM (in some cases from 30GB to 30mb), but it produce invalid json format if error happened in the middle of streaming (because json is not streaming-friendly format)",
	}
	RpcBatchLimit = cli.IntFlag{
		Name:  "rpc.batch.limit",
		Usage: "Maximum number of requests in a batch",
		Value: 100,
	}
	RpcReturnDataLimit = cli.IntFlag{
		Name:  "rpc.returndata.limit",
		Usage: "Maximum number of bytes returned from eth_call or similar invocations",
		Value: 100_000,
	}
	HTTPTraceFlag = cli.BoolFlag{
		Name:  "http.trace",
		Usage: "Trace HTTP requests with INFO level",
	}
	DBReadConcurrencyFlag = cli.IntFlag{
		Name:  "db.read.concurrency",
		Usage: "Does limit amount of parallel db reads. Default: equal to GOMAXPROCS (or number of CPU)",
		Value: cmp.Max(10, runtime.GOMAXPROCS(-1)*8),
	}
	RpcAccessListFlag = cli.StringFlag{
		Name:  "rpc.accessList",
		Usage: "Specify granular (method-by-method) API allowlist",
	}

	RpcGasCapFlag = cli.UintFlag{
		Name:  "rpc.gascap",
		Usage: "Sets a cap on gas that can be used in eth_call/estimateGas",
		Value: 50000000,
	}
	RpcTraceCompatFlag = cli.BoolFlag{
		Name:  "trace.compat",
		Usage: "Bug for bug compatibility with OE for trace_ routines",
	}

	TxpoolApiAddrFlag = cli.StringFlag{
		Name:  "txpool.api.addr",
		Usage: "txpool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)",
	}

	TraceMaxtracesFlag = cli.UintFlag{
		Name:  "trace.maxtraces",
		Usage: "Sets a limit on traces that can be returned in trace_filter",
		Value: 200,
	}

	HTTPPathPrefixFlag = cli.StringFlag{
		Name:  "http.rpcprefix",
		Usage: "HTTP path path prefix on which JSON-RPC is served. Use '/' to serve on all paths.",
		Value: "",
	}
	TLSFlag = cli.BoolFlag{
		Name:  "tls",
		Usage: "Enable TLS handshake",
	}
	TLSCertFlag = cli.StringFlag{
		Name:  "tls.cert",
		Usage: "Specify certificate",
		Value: "",
	}
	TLSKeyFlag = cli.StringFlag{
		Name:  "tls.key",
		Usage: "Specify key file",
		Value: "",
	}
	TLSCACertFlag = cli.StringFlag{
		Name:  "tls.cacert",
		Usage: "Specify certificate authority",
		Value: "",
	}
	WSEnabledFlag = cli.BoolFlag{
		Name:  "ws",
		Usage: "Enable the WS-RPC server",
	}
	WSListenAddrFlag = cli.StringFlag{
		Name:  "ws.addr",
		Usage: "WS-RPC server listening interface",
		Value: nodecfg.DefaultWSHost,
	}
	WSPortFlag = cli.IntFlag{
		Name:  "ws.port",
		Usage: "WS-RPC server listening port",
		Value: nodecfg.DefaultWSPort,
	}
	WSApiFlag = cli.StringFlag{
		Name:  "ws.api",
		Usage: "API's offered over the WS-RPC interface",
		Value: "",
	}
	WSAllowedOriginsFlag = cli.StringFlag{
		Name:  "ws.origins",
		Usage: "Origins from which to accept websockets requests",
		Value: "",
	}
	WSPathPrefixFlag = cli.StringFlag{
		Name:  "ws.rpcprefix",
		Usage: "HTTP path prefix on which JSON-RPC is served. Use '/' to serve on all paths.",
		Value: "",
	}
	ExecFlag = cli.StringFlag{
		Name:  "exec",
		Usage: "Execute JavaScript statement",
	}
	PreloadJSFlag = cli.StringFlag{
		Name:  "preload",
		Usage: "Comma separated list of JavaScript files to preload into the console",
	}
	AllowUnprotectedTxs = cli.BoolFlag{
		Name:  "rpc.allow-unprotected-txs",
		Usage: "Allow for unprotected (non EIP155 signed) transactions to be submitted via RPC",
	}
	StateCacheFlag = cli.StringFlag{
		Name:  "state.cache",
		Value: "0MB",
		Usage: "Amount of data to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. Defaults to 0MB",
	}

	// Network Settings
	MaxPeersFlag = cli.IntFlag{
		Name:  "maxpeers",
		Usage: "Maximum number of network peers (network disabled if set to 0)",
		Value: nodecfg.DefaultConfig.P2P.MaxPeers,
	}
	MaxPendingPeersFlag = cli.IntFlag{
		Name:  "maxpendpeers",
		Usage: "Maximum number of TCP connections pending to become connected peers",
		Value: nodecfg.DefaultConfig.P2P.MaxPendingPeers,
	}
	ListenPortFlag = cli.IntFlag{
		Name:  "port",
		Usage: "Network listening port",
		Value: 30303,
	}
	P2pProtocolVersionFlag = cli.UintSliceFlag{
		Name:  "p2p.protocol",
		Usage: "Version of eth p2p protocol",
		Value: cli.NewUintSlice(nodecfg.DefaultConfig.P2P.ProtocolVersion...),
	}
	P2pProtocolAllowedPorts = cli.UintSliceFlag{
		Name:  "p2p.allowed-ports",
		Usage: "Allowed ports to pick for different eth p2p protocol versions as follows <porta>,<portb>,..,<porti>",
		Value: cli.NewUintSlice(uint(ListenPortFlag.Value), 30304, 30305, 30306, 30307),
	}
	SentryAddrFlag = cli.StringFlag{
		Name:  "sentry.api.addr",
		Usage: "comma separated sentry addresses '<host>:<port>,<host>:<port>'",
	}
	SentryLogPeerInfoFlag = cli.BoolFlag{
		Name:  "sentry.log-peer-info",
		Usage: "Log detailed peer info when a peer connects or disconnects. Enable to integrate with observer.",
	}
	SentryDropUselessPeers = cli.BoolFlag{
		Name:  "sentry.drop-useless-peers",
		Usage: "Drop useless peers, those returning empty body or header responses",
		Value: false,
	}
	DownloaderAddrFlag = cli.StringFlag{
		Name:  "downloader.api.addr",
		Usage: "downloader address '<host>:<port>'",
	}
	BootnodesFlag = cli.StringFlag{
		Name:  "bootnodes",
		Usage: "Comma separated enode URLs for P2P discovery bootstrap",
		Value: "",
	}
	StaticPeersFlag = cli.StringFlag{
		Name:  "staticpeers",
		Usage: "Comma separated enode URLs to connect to",
		Value: "",
	}
	TrustedPeersFlag = cli.StringFlag{
		Name:  "trustedpeers",
		Usage: "Comma separated enode URLs which are always allowed to connect, even above the peer limit",
		Value: "",
	}
	NodeKeyFileFlag = cli.StringFlag{
		Name:  "nodekey",
		Usage: "P2P node key file",
	}
	NodeKeyHexFlag = cli.StringFlag{
		Name:  "nodekeyhex",
		Usage: "P2P node key as hex (for testing)",
	}
	NATFlag = cli.StringFlag{
		Name: "nat",
		Usage: `NAT port mapping mechanism (any|none|upnp|pmp|stun|extip:<IP>)
	     "" or "none"         default - do not nat
	     "extip:77.12.33.4"   will assume the local machine is reachable on the given IP
	     "any"                uses the first auto-detected mechanism
	     "upnp"               uses the Universal Plug and Play protocol
	     "pmp"                uses NAT-PMP with an auto-detected gateway address
	     "pmp:192.168.0.1"    uses NAT-PMP with the given gateway address
	     "stun"               uses STUN to detect an external IP using a default server
	     "stun:<server>"      uses STUN to detect an external IP using the given server (host:port)
`,
		Value: "",
	}
	NoDiscoverFlag = cli.BoolFlag{
		Name:  "nodiscover",
		Usage: "Disables the peer discovery mechanism (manual peer addition)",
	}
	DiscoveryV5Flag = cli.BoolFlag{
		Name:  "v5disc",
		Usage: "Enables the experimental RLPx V5 (Topic Discovery) mechanism",
	}
	NetrestrictFlag = cli.StringFlag{
		Name:  "netrestrict",
		Usage: "Restricts network communication to the given IP networks (CIDR masks)",
	}
	DNSDiscoveryFlag = cli.StringFlag{
		Name:  "discovery.dns",
		Usage: "Sets DNS discovery entry points (use \"\" to disable DNS)",
	}

	// ATM the url is left to the user and deployment to
	JSpathFlag = cli.StringFlag{
		Name:  "jspath",
		Usage: "JavaScript root path for `loadScript`",
		Value: ".",
	}

	// Gas price oracle settings
	GpoBlocksFlag = cli.IntFlag{
		Name:  "gpo.blocks",
		Usage: "Number of recent blocks to check for gas prices",
		Value: ethconfig.Defaults.GPO.Blocks,
	}
	GpoPercentileFlag = cli.IntFlag{
		Name:  "gpo.percentile",
		Usage: "Suggested gas price is the given percentile of a set of recent transaction gas prices",
		Value: ethconfig.Defaults.GPO.Percentile,
	}
	GpoMaxGasPriceFlag = cli.Int64Flag{
		Name:  "gpo.maxprice",
		Usage: "Maximum gas price will be recommended by gpo",
		Value: ethconfig.Defaults.GPO.MaxPrice.Int64(),
	}

	// Metrics flags
	MetricsEnabledFlag = cli.BoolFlag{
		Name:  "metrics",
		Usage: "Enable metrics collection and reporting",
	}

	// MetricsHTTPFlag defines the endpoint for a stand-alone metrics HTTP endpoint.
	// Since the pprof service enables sensitive/vulnerable behavior, this allows a user
	// to enable a public-OK metrics endpoint without having to worry about ALSO exposing
	// other profiling behavior or information.
	MetricsHTTPFlag = cli.StringFlag{
		Name:  "metrics.addr",
		Usage: "Enable stand-alone metrics HTTP server listening interface",
		Value: metrics.DefaultConfig.HTTP,
	}
	MetricsPortFlag = cli.IntFlag{
		Name:  "metrics.port",
		Usage: "Metrics HTTP server listening port",
		Value: metrics.DefaultConfig.Port,
	}
	HistoryV3Flag = cli.BoolFlag{
		Name:  "experimental.history.v3",
		Usage: "(also known as Erigon3) Not recommended yet: Can't change this flag after node creation. New DB and Snapshots format of history allows: parallel blocks execution, get state as of given transaction without executing whole block.",
	}
	TransactionV3Flag = cli.BoolFlag{
		Name:  "experimental.transactions.v3",
		Usage: "(this flag is in testing stage) Not recommended yet: Can't change this flag after node creation. New DB table for transactions allows keeping multiple branches of block bodies in the DB simultaneously",
	}

	CliqueSnapshotCheckpointIntervalFlag = cli.UintFlag{
		Name:  "clique.checkpoint",
		Usage: "number of blocks after which to save the vote snapshot to the database",
		Value: 10,
	}
	CliqueSnapshotInmemorySnapshotsFlag = cli.IntFlag{
		Name:  "clique.snapshots",
		Usage: "number of recent vote snapshots to keep in memory",
		Value: 1024,
	}
	CliqueSnapshotInmemorySignaturesFlag = cli.IntFlag{
		Name:  "clique.signatures",
		Usage: "number of recent block signatures to keep in memory",
		Value: 16384,
	}
	CliqueDataDirFlag = DirectoryFlag{
		Name:  "clique.datadir",
		Usage: "a path to clique db folder",
		Value: "",
	}

	SnapKeepBlocksFlag = cli.BoolFlag{
		Name:  ethconfig.FlagSnapKeepBlocks,
		Usage: "Keep ancient blocks in db (useful for debug)",
	}
	SnapStopFlag = cli.BoolFlag{
		Name:  ethconfig.FlagSnapStop,
		Usage: "Workaround to stop producing new snapshots, if you meet some snapshots-related critical bug. It will stop move historical data from DB to new immutable snapshots. DB will grow and may slightly slow-down - and removing this flag in future will not fix this effect (db size will not greatly reduce).",
	}
	TorrentVerbosityFlag = cli.IntFlag{
		Name:  "torrent.verbosity",
		Value: 2,
		Usage: "0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail (must set --verbosity to equal or higher level and has defeault: 3)",
	}
	TorrentDownloadRateFlag = cli.StringFlag{
		Name:  "torrent.download.rate",
		Value: "16mb",
		Usage: "bytes per second, example: 32mb",
	}
	TorrentUploadRateFlag = cli.StringFlag{
		Name:  "torrent.upload.rate",
		Value: "4mb",
		Usage: "bytes per second, example: 32mb",
	}
	TorrentDownloadSlotsFlag = cli.IntFlag{
		Name:  "torrent.download.slots",
		Value: 3,
		Usage: "amount of files to download in parallel. If network has enough seeders 1-3 slot enough, if network has lack of seeders increase to 5-7 (too big value will slow down everything).",
	}
	TorrentStaticPeersFlag = cli.StringFlag{
		Name:  "torrent.staticpeers",
		Usage: "Comma separated enode URLs to connect to",
		Value: "",
	}
	NoDownloaderFlag = cli.BoolFlag{
		Name:  "no-downloader",
		Usage: "to disable downloader component",
	}
	DownloaderVerifyFlag = cli.BoolFlag{
		Name:  "downloader.verify",
		Usage: "verify snapshots on startup. it will not report founded problems but just re-download broken pieces",
	}
	DisableIPV6 = cli.BoolFlag{
		Name:  "downloader.disable.ipv6",
		Usage: "Turns off ipv6 for the downlaoder",
		Value: false,
	}

	DisableIPV4 = cli.BoolFlag{
		Name:  "downloader.disable.ipv4",
		Usage: "Turn off ipv4 for the downloader",
		Value: false,
	}
	TorrentPortFlag = cli.IntFlag{
		Name:  "torrent.port",
		Value: 42069,
		Usage: "port to listen and serve BitTorrent protocol",
	}
	TorrentMaxPeersFlag = cli.IntFlag{
		Name:  "torrent.maxpeers",
		Value: 100,
		Usage: "unused parameter (reserved for future use)",
	}
	TorrentConnsPerFileFlag = cli.IntFlag{
		Name:  "torrent.conns.perfile",
		Value: 10,
		Usage: "connections per file",
	}
	DbPageSizeFlag = cli.StringFlag{
		Name:  "db.pagesize",
		Usage: "set mdbx pagesize on db creation: must be power of 2 and '256b <= pagesize <= 64kb'. default: equal to OperationSystem's pageSize",
		Value: datasize.ByteSize(kv.DefaultPageSize()).String(),
	}
	DbSizeLimitFlag = cli.StringFlag{
		Name:  "db.size.limit",
		Value: (8 * datasize.TB).String(),
	}

	HealthCheckFlag = cli.BoolFlag{
		Name:  "healthcheck",
		Usage: "Enabling grpc health check",
	}

	HeimdallURLFlag = cli.StringFlag{
		Name:  "bor.heimdall",
		Usage: "URL of Heimdall service",
		Value: "http://localhost:1317",
	}

	// WithoutHeimdallFlag no heimdall (for testing purpose)
	WithoutHeimdallFlag = cli.BoolFlag{
		Name:  "bor.withoutheimdall",
		Usage: "Run without Heimdall service (for testing purpose)",
	}

	// HeimdallgRPCAddressFlag flag for heimdall gRPC address
	HeimdallgRPCAddressFlag = cli.StringFlag{
		Name:  "bor.heimdallgRPC",
		Usage: "Address of Heimdall gRPC service",
		Value: "",
	}

	ConfigFlag = cli.StringFlag{
		Name:  "config",
		Usage: "Sets erigon flags from YAML/TOML file",
		Value: "",
	}
	LightClientDiscoveryAddrFlag = cli.StringFlag{
		Name:  "lightclient.discovery.addr",
		Usage: "Address for lightclient DISCV5 protocol",
		Value: "127.0.0.1",
	}
	LightClientDiscoveryPortFlag = cli.Uint64Flag{
		Name:  "lightclient.discovery.port",
		Usage: "Port for lightclient DISCV5 protocol",
		Value: 4000,
	}
	LightClientDiscoveryTCPPortFlag = cli.Uint64Flag{
		Name:  "lightclient.discovery.tcpport",
		Usage: "TCP Port for lightclient DISCV5 protocol",
		Value: 4001,
	}
	SentinelAddrFlag = cli.StringFlag{
		Name:  "sentinel.addr",
		Usage: "Address for sentinel",
		Value: "localhost",
	}
	SentinelPortFlag = cli.Uint64Flag{
		Name:  "sentinel.port",
		Usage: "Port for sentinel",
		Value: 7777,
	}
	YieldSizeFlag = cli.Uint64Flag{
		Name:  "yieldsize",
		Usage: "transaction count fetched from txpool each time",
		Value: 1000,
	}
)

var MetricFlags = []cli.Flag{&MetricsEnabledFlag, &MetricsHTTPFlag, &MetricsPortFlag}

// setNodeKey loads a node key from command line flags if provided,
// otherwise it tries to load it from datadir,
// otherwise it generates a new key in datadir.
func setNodeKey(ctx *cli.Context, cfg *p2p.Config, datadir string) {
	file := ctx.String(NodeKeyFileFlag.Name)
	hex := ctx.String(NodeKeyHexFlag.Name)

	config := p2p.NodeKeyConfig{}
	key, err := config.LoadOrParseOrGenerateAndSave(file, hex, datadir)
	if err != nil {
		Fatalf("%v", err)
	}
	cfg.PrivateKey = key
}

// setNodeUserIdent creates the user identifier from CLI flags.
func setNodeUserIdent(ctx *cli.Context, cfg *nodecfg.Config) {
	if identity := ctx.String(IdentityFlag.Name); len(identity) > 0 {
		cfg.UserIdent = identity
	}
}
func setNodeUserIdentCobra(f *pflag.FlagSet, cfg *nodecfg.Config) {
	if identity := f.String(IdentityFlag.Name, IdentityFlag.Value, IdentityFlag.Usage); identity != nil && len(*identity) > 0 {
		cfg.UserIdent = *identity
	}
}

func setBootstrapNodes(ctx *cli.Context, cfg *p2p.Config) {
	// If already set, don't apply defaults.
	if cfg.BootstrapNodes != nil {
		return
	}

	nodes, err := GetBootnodesFromFlags(ctx.String(BootnodesFlag.Name), ctx.String(ChainFlag.Name))
	if err != nil {
		Fatalf("Option %s: %v", BootnodesFlag.Name, err)
	}

	cfg.BootstrapNodes = nodes
}

func setBootstrapNodesV5(ctx *cli.Context, cfg *p2p.Config) {
	// If already set, don't apply defaults.
	if cfg.BootstrapNodesV5 != nil {
		return
	}

	nodes, err := GetBootnodesFromFlags(ctx.String(BootnodesFlag.Name), ctx.String(ChainFlag.Name))
	if err != nil {
		Fatalf("Option %s: %v", BootnodesFlag.Name, err)
	}

	cfg.BootstrapNodesV5 = nodes
}

// GetBootnodesFromFlags makes a list of bootnodes from command line flags.
// If urlsStr is given, it is used and parsed as a comma-separated list of enode:// urls,
// otherwise a list of preconfigured bootnodes of the specified chain is returned.
func GetBootnodesFromFlags(urlsStr, chain string) ([]*enode.Node, error) {
	var urls []string
	if urlsStr != "" {
		urls = SplitAndTrim(urlsStr)
	} else {
		urls = params.BootnodeURLsOfChain(chain)
	}
	return ParseNodesFromURLs(urls)
}

func setStaticPeers(ctx *cli.Context, cfg *p2p.Config) {
	var urls []string
	if ctx.IsSet(StaticPeersFlag.Name) {
		urls = SplitAndTrim(ctx.String(StaticPeersFlag.Name))
	} else {
		chain := ctx.String(ChainFlag.Name)
		urls = params.StaticPeerURLsOfChain(chain)
	}

	nodes, err := ParseNodesFromURLs(urls)
	if err != nil {
		Fatalf("Option %s: %v", StaticPeersFlag.Name, err)
	}

	cfg.StaticNodes = nodes
}

func setTrustedPeers(ctx *cli.Context, cfg *p2p.Config) {
	if !ctx.IsSet(TrustedPeersFlag.Name) {
		return
	}

	urls := SplitAndTrim(ctx.String(TrustedPeersFlag.Name))
	trustedNodes, err := ParseNodesFromURLs(urls)
	if err != nil {
		Fatalf("Option %s: %v", TrustedPeersFlag.Name, err)
	}

	cfg.TrustedNodes = append(cfg.TrustedNodes, trustedNodes...)
}

func ParseNodesFromURLs(urls []string) ([]*enode.Node, error) {
	nodes := make([]*enode.Node, 0, len(urls))
	for _, url := range urls {
		if url == "" {
			continue
		}
		n, err := enode.Parse(enode.ValidSchemes, url)
		if err != nil {
			return nil, fmt.Errorf("invalid node URL %s: %w", url, err)
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// NewP2PConfig
//   - doesn't setup bootnodes - they will set when genesisHash will know
func NewP2PConfig(
	nodiscover bool,
	dirs datadir.Dirs,
	netRestrict string,
	natSetting string,
	maxPeers int,
	maxPendPeers int,
	nodeName string,
	staticPeers []string,
	trustedPeers []string,
	port uint,
	protocol uint,
	allowedPorts []uint,
	metricsEnabled bool,
) (*p2p.Config, error) {
	var enodeDBPath string
	switch protocol {
	case eth.ETH66:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth66")
	case eth.ETH67:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth67")
	case eth.ETH68:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth68")
	default:
		return nil, fmt.Errorf("unknown protocol: %v", protocol)
	}

	serverKey, err := nodeKey(dirs.DataDir)
	if err != nil {
		return nil, err
	}

	cfg := &p2p.Config{
		ListenAddr:      fmt.Sprintf(":%d", port),
		MaxPeers:        maxPeers,
		MaxPendingPeers: maxPendPeers,
		NAT:             nat.Any(),
		NoDiscovery:     nodiscover,
		PrivateKey:      serverKey,
		Name:            nodeName,
		Log:             log.New(),
		NodeDatabase:    enodeDBPath,
		AllowedPorts:    allowedPorts,
		TmpDir:          dirs.Tmp,
		MetricsEnabled:  metricsEnabled,
	}
	if netRestrict != "" {
		cfg.NetRestrict = new(netutil.Netlist)
		cfg.NetRestrict.Add(netRestrict)
	}
	if staticPeers != nil {
		staticNodes, err := ParseNodesFromURLs(staticPeers)
		if err != nil {
			return nil, fmt.Errorf("bad option %s: %w", StaticPeersFlag.Name, err)
		}
		cfg.StaticNodes = staticNodes
	}
	if trustedPeers != nil {
		trustedNodes, err := ParseNodesFromURLs(trustedPeers)
		if err != nil {
			return nil, fmt.Errorf("bad option %s: %w", TrustedPeersFlag.Name, err)
		}
		cfg.TrustedNodes = trustedNodes
	}
	natif, err := nat.Parse(natSetting)
	if err != nil {
		return nil, fmt.Errorf("invalid nat option %s: %w", natSetting, err)
	}
	cfg.NAT = natif
	return cfg, nil
}

// nodeKey loads a node key from datadir if it exists,
// otherwise it generates a new key in datadir.
func nodeKey(datadir string) (*ecdsa.PrivateKey, error) {
	config := p2p.NodeKeyConfig{}
	keyfile := config.DefaultPath(datadir)
	return config.LoadOrGenerateAndSave(keyfile)
}

// setListenAddress creates a TCP listening address string from set command
// line flags.
func setListenAddress(ctx *cli.Context, cfg *p2p.Config) {
	if ctx.IsSet(ListenPortFlag.Name) {
		cfg.ListenAddr = fmt.Sprintf(":%d", ctx.Int(ListenPortFlag.Name))
	}
	if ctx.IsSet(P2pProtocolVersionFlag.Name) {
		cfg.ProtocolVersion = ctx.UintSlice(P2pProtocolVersionFlag.Name)
	}
	if ctx.IsSet(SentryAddrFlag.Name) {
		cfg.SentryAddr = SplitAndTrim(ctx.String(SentryAddrFlag.Name))
	}
	// TODO cli lib doesn't store defaults for UintSlice properly so we have to get value directly
	cfg.AllowedPorts = P2pProtocolAllowedPorts.Value.Value()
	if ctx.IsSet(P2pProtocolAllowedPorts.Name) {
		cfg.AllowedPorts = ctx.UintSlice(P2pProtocolAllowedPorts.Name)
	}

	if ctx.IsSet(ListenPortFlag.Name) {
		// add non-default port to allowed port list
		lp := ctx.Int(ListenPortFlag.Name)
		found := false
		for _, p := range cfg.AllowedPorts {
			if int(p) == lp {
				found = true
				break
			}
		}
		if !found {
			cfg.AllowedPorts = append([]uint{uint(lp)}, cfg.AllowedPorts...)
		}
	}
}

// setNAT creates a port mapper from command line flags.
func setNAT(ctx *cli.Context, cfg *p2p.Config) {
	if ctx.IsSet(NATFlag.Name) {
		natif, err := nat.Parse(ctx.String(NATFlag.Name))
		if err != nil {
			Fatalf("Option %s: %v", NATFlag.Name, err)
		}
		cfg.NAT = natif
	}
}

// SplitAndTrim splits input separated by a comma
// and trims excessive white space from the substrings.
func SplitAndTrim(input string) (ret []string) {
	l := strings.Split(input, ",")
	for _, r := range l {
		if r = strings.TrimSpace(r); r != "" {
			ret = append(ret, r)
		}
	}
	return ret
}

// setEtherbase retrieves the etherbase from the directly specified
// command line flags.
func setEtherbase(ctx *cli.Context, cfg *ethconfig.Config) {
	var etherbase string
	if ctx.IsSet(MinerEtherbaseFlag.Name) {
		etherbase = ctx.String(MinerEtherbaseFlag.Name)
		if etherbase != "" {
			cfg.Miner.Etherbase = libcommon.HexToAddress(etherbase)
		}
	}

	setSigKey := func(ctx *cli.Context, cfg *ethconfig.Config) {
		if ctx.IsSet(MinerSigningKeyFileFlag.Name) {
			signingKeyFileName := ctx.String(MinerSigningKeyFileFlag.Name)
			key, err := crypto.LoadECDSA(signingKeyFileName)
			if err != nil {
				panic(err)
			}
			cfg.Miner.SigKey = key
		}
	}

	if ctx.String(ChainFlag.Name) == networkname.DevChainName || ctx.String(ChainFlag.Name) == networkname.BorDevnetChainName {
		if etherbase == "" {
			cfg.Miner.SigKey = core.DevnetSignPrivateKey
			cfg.Miner.Etherbase = core.DevnetEtherbase
		}
		setSigKey(ctx, cfg)
	}

	chainsWithValidatorMode := map[string]bool{}
	if _, ok := chainsWithValidatorMode[ctx.String(ChainFlag.Name)]; ok || ctx.IsSet(MinerSigningKeyFileFlag.Name) {
		if ctx.IsSet(MiningEnabledFlag.Name) && !ctx.IsSet(MinerSigningKeyFileFlag.Name) {
			panic(fmt.Sprintf("Flag --%s is required in %s chain with --%s flag", MinerSigningKeyFileFlag.Name, ChainFlag.Name, MiningEnabledFlag.Name))
		}
		setSigKey(ctx, cfg)
		if cfg.Miner.SigKey != nil {
			cfg.Miner.Etherbase = crypto.PubkeyToAddress(cfg.Miner.SigKey.PublicKey)
		}
	}
}

func SetP2PConfig(ctx *cli.Context, cfg *p2p.Config, nodeName, datadir string) {
	cfg.Name = nodeName
	setNodeKey(ctx, cfg, datadir)
	setNAT(ctx, cfg)
	setListenAddress(ctx, cfg)
	setBootstrapNodes(ctx, cfg)
	setBootstrapNodesV5(ctx, cfg)
	setStaticPeers(ctx, cfg)
	setTrustedPeers(ctx, cfg)

	if ctx.IsSet(MaxPeersFlag.Name) {
		cfg.MaxPeers = ctx.Int(MaxPeersFlag.Name)
	}

	if ctx.IsSet(MaxPendingPeersFlag.Name) {
		cfg.MaxPendingPeers = ctx.Int(MaxPendingPeersFlag.Name)
	}
	if ctx.IsSet(NoDiscoverFlag.Name) {
		cfg.NoDiscovery = true
	}

	if ctx.IsSet(DiscoveryV5Flag.Name) {
		cfg.DiscoveryV5 = ctx.Bool(DiscoveryV5Flag.Name)
	}

	if ctx.IsSet(MetricsEnabledFlag.Name) {
		cfg.MetricsEnabled = ctx.Bool(MetricsEnabledFlag.Name)
	}

	ethPeers := cfg.MaxPeers
	cfg.Name = nodeName
	log.Info("Maximum peer count", "ETH", ethPeers, "total", cfg.MaxPeers)

	if netrestrict := ctx.String(NetrestrictFlag.Name); netrestrict != "" {
		list, err := netutil.ParseNetlist(netrestrict)
		if err != nil {
			Fatalf("Option %q: %v", NetrestrictFlag.Name, err)
		}
		cfg.NetRestrict = list
	}

	if ctx.String(ChainFlag.Name) == networkname.DevChainName {
		// --dev mode can't use p2p networking.
		//cfg.MaxPeers = 0 // It can have peers otherwise local sync is not possible
		if !ctx.IsSet(ListenPortFlag.Name) {
			cfg.ListenAddr = ":0"
		}
		cfg.NoDiscovery = true
		cfg.DiscoveryV5 = false
		log.Info("Development chain flags set", "--nodiscover", cfg.NoDiscovery, "--v5disc", cfg.DiscoveryV5, "--port", cfg.ListenAddr)
	}
}

// SetNodeConfig applies node-related command line flags to the config.
func SetNodeConfig(ctx *cli.Context, cfg *nodecfg.Config) {
	setDataDir(ctx, cfg)
	setNodeUserIdent(ctx, cfg)
	SetP2PConfig(ctx, &cfg.P2P, cfg.NodeName(), cfg.Dirs.DataDir)

	cfg.SentryLogPeerInfo = ctx.IsSet(SentryLogPeerInfoFlag.Name)
}

func SetNodeConfigCobra(cmd *cobra.Command, cfg *nodecfg.Config) {
	flags := cmd.Flags()
	//SetP2PConfig(ctx, &cfg.P2P)
	setNodeUserIdentCobra(flags, cfg)
	setDataDirCobra(flags, cfg)
}

func setDataDir(ctx *cli.Context, cfg *nodecfg.Config) {
	if ctx.IsSet(DataDirFlag.Name) {
		cfg.Dirs.DataDir = ctx.String(DataDirFlag.Name)
	} else {
		cfg.Dirs.DataDir = paths.DataDirForNetwork(cfg.Dirs.DataDir, ctx.String(ChainFlag.Name))
	}
	cfg.Dirs = datadir.New(cfg.Dirs.DataDir)

	if err := cfg.MdbxPageSize.UnmarshalText([]byte(ctx.String(DbPageSizeFlag.Name))); err != nil {
		panic(err)
	}
	if err := cfg.MdbxDBSizeLimit.UnmarshalText([]byte(ctx.String(DbSizeLimitFlag.Name))); err != nil {
		panic(err)
	}
	sz := cfg.MdbxPageSize.Bytes()
	if !isPowerOfTwo(sz) || sz < 256 || sz > 64*1024 {
		panic(fmt.Errorf("invalid --db.pagesize: %s=%d, see: %s", ctx.String(DbPageSizeFlag.Name), sz, DbPageSizeFlag.Usage))
	}
	szLimit := cfg.MdbxDBSizeLimit.Bytes()
	if !isPowerOfTwo(szLimit) || szLimit < 256 {
		panic(fmt.Errorf("invalid --db.size.limit: %s=%d, see: %s", ctx.String(DbSizeLimitFlag.Name), sz, DbSizeLimitFlag.Usage))
	}
}

func isPowerOfTwo(n uint64) bool {
	if n == 0 { //corner case: if n is zero it will also consider as power 2
		return true
	}
	return n&(n-1) == 0
}

func setDataDirCobra(f *pflag.FlagSet, cfg *nodecfg.Config) {
	dirname, err := f.GetString(DataDirFlag.Name)
	if err != nil {
		panic(err)
	}
	chain, err := f.GetString(ChainFlag.Name)
	if err != nil {
		panic(err)
	}
	if dirname != "" {
		cfg.Dirs.DataDir = dirname
	} else {
		cfg.Dirs.DataDir = paths.DataDirForNetwork(cfg.Dirs.DataDir, chain)
	}

	cfg.Dirs.DataDir = paths.DataDirForNetwork(cfg.Dirs.DataDir, chain)
	cfg.Dirs = datadir.New(cfg.Dirs.DataDir)
}

func setGPO(ctx *cli.Context, cfg *gaspricecfg.Config) {
	if ctx.IsSet(GpoBlocksFlag.Name) {
		cfg.Blocks = ctx.Int(GpoBlocksFlag.Name)
	}
	if ctx.IsSet(GpoPercentileFlag.Name) {
		cfg.Percentile = ctx.Int(GpoPercentileFlag.Name)
	}
	if ctx.IsSet(GpoMaxGasPriceFlag.Name) {
		cfg.MaxPrice = big.NewInt(ctx.Int64(GpoMaxGasPriceFlag.Name))
	}
}

// nolint
func setGPOCobra(f *pflag.FlagSet, cfg *gaspricecfg.Config) {
	if v := f.Int(GpoBlocksFlag.Name, GpoBlocksFlag.Value, GpoBlocksFlag.Usage); v != nil {
		cfg.Blocks = *v
	}
	if v := f.Int(GpoPercentileFlag.Name, GpoPercentileFlag.Value, GpoPercentileFlag.Usage); v != nil {
		cfg.Percentile = *v
	}
	if v := f.Int64(GpoMaxGasPriceFlag.Name, GpoMaxGasPriceFlag.Value, GpoMaxGasPriceFlag.Usage); v != nil {
		cfg.MaxPrice = big.NewInt(*v)
	}
}

func setTxPool(ctx *cli.Context, cfg *ethconfig.DeprecatedTxPoolConfig) {
	if ctx.IsSet(TxPoolDisableFlag.Name) {
		cfg.Disable = ctx.Bool(TxPoolDisableFlag.Name)
	}
	if ctx.IsSet(TxPoolLocalsFlag.Name) {
		locals := strings.Split(ctx.String(TxPoolLocalsFlag.Name), ",")
		for _, account := range locals {
			if trimmed := strings.TrimSpace(account); !libcommon.IsHexAddress(trimmed) {
				Fatalf("Invalid account in --txpool.locals: %s", trimmed)
			} else {
				cfg.Locals = append(cfg.Locals, libcommon.HexToAddress(account))
			}
		}
	}
	if ctx.IsSet(TxPoolNoLocalsFlag.Name) {
		cfg.NoLocals = ctx.Bool(TxPoolNoLocalsFlag.Name)
	}
	if ctx.IsSet(TxPoolPriceLimitFlag.Name) {
		cfg.PriceLimit = ctx.Uint64(TxPoolPriceLimitFlag.Name)
	}
	if ctx.IsSet(TxPoolPriceBumpFlag.Name) {
		cfg.PriceBump = ctx.Uint64(TxPoolPriceBumpFlag.Name)
	}
	if ctx.IsSet(TxPoolAccountSlotsFlag.Name) {
		cfg.AccountSlots = ctx.Uint64(TxPoolAccountSlotsFlag.Name)
	}
	if ctx.IsSet(TxPoolGlobalSlotsFlag.Name) {
		cfg.GlobalSlots = ctx.Uint64(TxPoolGlobalSlotsFlag.Name)
	}
	if ctx.IsSet(TxPoolAccountQueueFlag.Name) {
		cfg.AccountQueue = ctx.Uint64(TxPoolAccountQueueFlag.Name)
	}
	if ctx.IsSet(TxPoolGlobalQueueFlag.Name) {
		cfg.GlobalQueue = ctx.Uint64(TxPoolGlobalQueueFlag.Name)
	}
	if ctx.IsSet(TxPoolGlobalBaseFeeSlotsFlag.Name) {
		cfg.GlobalBaseFeeQueue = ctx.Uint64(TxPoolGlobalBaseFeeSlotsFlag.Name)
	}
	if ctx.IsSet(TxPoolLifetimeFlag.Name) {
		cfg.Lifetime = ctx.Duration(TxPoolLifetimeFlag.Name)
	}
	if ctx.IsSet(TxPoolTraceSendersFlag.Name) {
		// Parse the command separated flag
		senderHexes := SplitAndTrim(ctx.String(TxPoolTraceSendersFlag.Name))
		cfg.TracedSenders = make([]string, len(senderHexes))
		for i, senderHex := range senderHexes {
			sender := libcommon.HexToAddress(senderHex)
			cfg.TracedSenders[i] = string(sender[:])
		}
	}

	cfg.CommitEvery = common2.RandomizeDuration(ctx.Duration(TxPoolCommitEveryFlag.Name))
}

func setEthash(ctx *cli.Context, datadir string, cfg *ethconfig.Config) {
	if ctx.IsSet(EthashDatasetDirFlag.Name) {
		cfg.Ethash.DatasetDir = ctx.String(EthashDatasetDirFlag.Name)
	} else {
		cfg.Ethash.DatasetDir = filepath.Join(datadir, "ethash-dags")
	}
	if ctx.IsSet(EthashCachesInMemoryFlag.Name) {
		cfg.Ethash.CachesInMem = ctx.Int(EthashCachesInMemoryFlag.Name)
	}
	if ctx.IsSet(EthashCachesLockMmapFlag.Name) {
		cfg.Ethash.CachesLockMmap = ctx.Bool(EthashCachesLockMmapFlag.Name)
	}
	if ctx.IsSet(FakePoWFlag.Name) {
		cfg.Ethash.PowMode = ethashcfg.ModeFake
	}
	if ctx.IsSet(EthashDatasetsLockMmapFlag.Name) {
		cfg.Ethash.DatasetsLockMmap = ctx.Bool(EthashDatasetsLockMmapFlag.Name)
	}
}

func SetupMinerCobra(cmd *cobra.Command, cfg *params.MiningConfig) {
	flags := cmd.Flags()
	var err error
	cfg.Enabled, err = flags.GetBool(MiningEnabledFlag.Name)
	if err != nil {
		panic(err)
	}
	if cfg.Enabled && len(cfg.Etherbase.Bytes()) == 0 {
		panic(fmt.Sprintf("Erigon supports only remote miners. Flag --%s or --%s is required", MinerNotifyFlag.Name, MinerSigningKeyFileFlag.Name))
	}
	cfg.Notify, err = flags.GetStringArray(MinerNotifyFlag.Name)
	if err != nil {
		panic(err)
	}
	extraDataStr, err := flags.GetString(MinerExtraDataFlag.Name)
	if err != nil {
		panic(err)
	}
	cfg.ExtraData = []byte(extraDataStr)
	cfg.GasLimit, err = flags.GetUint64(MinerGasLimitFlag.Name)
	if err != nil {
		panic(err)
	}
	price, err := flags.GetInt64(MinerGasPriceFlag.Name)
	if err != nil {
		panic(err)
	}
	cfg.GasPrice = big.NewInt(price)
	cfg.Recommit, err = flags.GetDuration(MinerRecommitIntervalFlag.Name)
	if err != nil {
		panic(err)
	}
	cfg.Noverify, err = flags.GetBool(MinerNoVerfiyFlag.Name)
	if err != nil {
		panic(err)
	}

	// Extract the current etherbase, new flag overriding legacy one
	var etherbase string
	etherbase, err = flags.GetString(MinerEtherbaseFlag.Name)
	if err != nil {
		panic(err)
	}

	// Convert the etherbase into an address and configure it
	if etherbase == "" {
		Fatalf("No etherbase configured")
	}
	cfg.Etherbase = libcommon.HexToAddress(etherbase)
}

func setClique(ctx *cli.Context, cfg *params.ConsensusSnapshotConfig, datadir string) {
	cfg.CheckpointInterval = ctx.Uint64(CliqueSnapshotCheckpointIntervalFlag.Name)
	cfg.InmemorySnapshots = ctx.Int(CliqueSnapshotInmemorySnapshotsFlag.Name)
	cfg.InmemorySignatures = ctx.Int(CliqueSnapshotInmemorySignaturesFlag.Name)
	if ctx.IsSet(CliqueDataDirFlag.Name) {
		cfg.DBPath = filepath.Join(ctx.String(CliqueDataDirFlag.Name), "clique", "db")
	} else {
		cfg.DBPath = filepath.Join(datadir, "clique", "db")
	}
}

func setBorConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	cfg.HeimdallURL = ctx.String(HeimdallURLFlag.Name)
	cfg.WithoutHeimdall = ctx.Bool(WithoutHeimdallFlag.Name)
	cfg.HeimdallgRPCAddress = ctx.String(HeimdallgRPCAddressFlag.Name)
}

func setMiner(ctx *cli.Context, cfg *params.MiningConfig) {
	cfg.Enabled = ctx.IsSet(MiningEnabledFlag.Name)
	cfg.EnabledPOS = !ctx.IsSet(ProposingDisableFlag.Name)

	if cfg.Enabled && len(cfg.Etherbase.Bytes()) == 0 {
		panic(fmt.Sprintf("Erigon supports only remote miners. Flag --%s or --%s is required", MinerNotifyFlag.Name, MinerSigningKeyFileFlag.Name))
	}
	if ctx.IsSet(MinerNotifyFlag.Name) {
		cfg.Notify = strings.Split(ctx.String(MinerNotifyFlag.Name), ",")
	}
	if ctx.IsSet(MinerExtraDataFlag.Name) {
		cfg.ExtraData = []byte(ctx.String(MinerExtraDataFlag.Name))
	}
	if ctx.IsSet(MinerGasLimitFlag.Name) {
		cfg.GasLimit = ctx.Uint64(MinerGasLimitFlag.Name)
	}
	if ctx.IsSet(MinerGasPriceFlag.Name) {
		cfg.GasPrice = BigFlagValue(ctx, MinerGasPriceFlag.Name)
	}
	if ctx.IsSet(MinerRecommitIntervalFlag.Name) {
		cfg.Recommit = ctx.Duration(MinerRecommitIntervalFlag.Name)
	}
	if ctx.IsSet(MinerNoVerfiyFlag.Name) {
		cfg.Noverify = ctx.Bool(MinerNoVerfiyFlag.Name)
	}
}

func setWhitelist(ctx *cli.Context, cfg *ethconfig.Config) {
	whitelist := ctx.String(WhitelistFlag.Name)
	if whitelist == "" {
		return
	}
	cfg.Whitelist = make(map[uint64]libcommon.Hash)
	for _, entry := range strings.Split(whitelist, ",") {
		parts := strings.Split(entry, "=")
		if len(parts) != 2 {
			Fatalf("Invalid whitelist entry: %s", entry)
		}
		number, err := strconv.ParseUint(parts[0], 0, 64)
		if err != nil {
			Fatalf("Invalid whitelist block number %s: %v", parts[0], err)
		}
		var hash libcommon.Hash
		if err = hash.UnmarshalText([]byte(parts[1])); err != nil {
			Fatalf("Invalid whitelist hash %s: %v", parts[1], err)
		}
		cfg.Whitelist[number] = hash
	}
}

type DynamicConfig struct {
	Root       string `json:"root"`
	Timestamp  uint64 `json:"timestamp"`
	GasLimit   uint64 `json:"gasLimit"`
	Difficulty int64  `json:"difficulty"`
}

// CheckExclusive verifies that only a single instance of the provided flags was
// set by the user. Each flag might optionally be followed by a string type to
// specialize it further.
func CheckExclusive(ctx *cli.Context, args ...interface{}) {
	set := make([]string, 0, 1)
	for i := 0; i < len(args); i++ {
		// Make sure the next argument is a flag and skip if not set
		flag, ok := args[i].(cli.Flag)
		if !ok {
			panic(fmt.Sprintf("invalid argument, not cli.Flag type: %T", args[i]))
		}
		// Check if next arg extends current and expand its name if so
		name := flag.Names()[0]

		if i+1 < len(args) {
			switch option := args[i+1].(type) {
			case string:
				// Extended flag check, make sure value set doesn't conflict with passed in option
				if ctx.String(flag.Names()[0]) == option {
					name += "=" + option
					set = append(set, "--"+name)
				}
				// shift arguments and continue
				i++
				continue

			case cli.Flag:
			default:
				panic(fmt.Sprintf("invalid argument, not cli.Flag or string extension: %T", args[i+1]))
			}
		}
		// Mark the flag if it's set
		if ctx.IsSet(flag.Names()[0]) {
			set = append(set, "--"+name)
		}
	}
	if len(set) > 1 {
		Fatalf("Flags %v can't be used at the same time", strings.Join(set, ", "))
	}
}

// SetEthConfig applies eth-related command line flags to the config.
func SetEthConfig(ctx *cli.Context, nodeConfig *nodecfg.Config, cfg *ethconfig.Config) {
	cfg.LightClientDiscoveryAddr = ctx.String(LightClientDiscoveryAddrFlag.Name)
	cfg.LightClientDiscoveryPort = ctx.Uint64(LightClientDiscoveryPortFlag.Name)
	cfg.LightClientDiscoveryTCPPort = ctx.Uint64(LightClientDiscoveryTCPPortFlag.Name)
	cfg.SentinelAddr = ctx.String(SentinelAddrFlag.Name)
	cfg.SentinelPort = ctx.Uint64(SentinelPortFlag.Name)

	cfg.Sync.UseSnapshots = ethconfig.UseSnapshotsByChainName(ctx.String(ChainFlag.Name))
	if ctx.IsSet(SnapshotFlag.Name) { //force override default by cli
		cfg.Sync.UseSnapshots = ctx.Bool(SnapshotFlag.Name)
	}

	cfg.Dirs = nodeConfig.Dirs
	cfg.Snapshot.KeepBlocks = ctx.Bool(SnapKeepBlocksFlag.Name)
	cfg.Snapshot.Produce = !ctx.Bool(SnapStopFlag.Name)
	cfg.Snapshot.NoDownloader = ctx.Bool(NoDownloaderFlag.Name)
	cfg.Snapshot.Verify = ctx.Bool(DownloaderVerifyFlag.Name)
	cfg.Snapshot.DownloaderAddr = strings.TrimSpace(ctx.String(DownloaderAddrFlag.Name))
	if cfg.Snapshot.DownloaderAddr == "" {
		downloadRateStr := ctx.String(TorrentDownloadRateFlag.Name)
		uploadRateStr := ctx.String(TorrentUploadRateFlag.Name)
		var downloadRate, uploadRate datasize.ByteSize
		if err := downloadRate.UnmarshalText([]byte(downloadRateStr)); err != nil {
			panic(err)
		}
		if err := uploadRate.UnmarshalText([]byte(uploadRateStr)); err != nil {
			panic(err)
		}
		lvl, _, err := downloadercfg2.Int2LogLevel(ctx.Int(TorrentVerbosityFlag.Name))
		if err != nil {
			panic(err)
		}
		log.Info("torrent verbosity", "level", lvl.LogString())
		version := "erigon: " + params.VersionWithCommit(params.GitCommit)
		cfg.Downloader, err = downloadercfg2.New(cfg.Dirs.Snap, version, lvl, downloadRate, uploadRate, ctx.Int(TorrentPortFlag.Name), ctx.Int(TorrentConnsPerFileFlag.Name), ctx.Int(TorrentDownloadSlotsFlag.Name), ctx.StringSlice(TorrentDownloadSlotsFlag.Name))
		if err != nil {
			panic(err)
		}
		downloadernat.DoNat(nodeConfig.P2P.NAT, cfg.Downloader)
	}

	nodeConfig.Http.Snap = cfg.Snapshot

	if ctx.Command.Name == "import" {
		cfg.ImportMode = true
	}

	setEtherbase(ctx, cfg)
	setGPO(ctx, &cfg.GPO)

	setTxPool(ctx, &cfg.DeprecatedTxPool)
	cfg.TxPool = ethconfig.DefaultTxPool2Config(cfg.DeprecatedTxPool)
	cfg.TxPool.DBDir = nodeConfig.Dirs.TxPool
	cfg.YieldSize = ctx.Uint64(YieldSizeFlag.Name)

	setEthash(ctx, nodeConfig.Dirs.DataDir, cfg)
	setClique(ctx, &cfg.Clique, nodeConfig.Dirs.DataDir)
	setMiner(ctx, &cfg.Miner)
	setWhitelist(ctx, cfg)
	setBorConfig(ctx, cfg)

	cfg.Ethstats = ctx.String(EthStatsURLFlag.Name)
	cfg.P2PEnabled = len(nodeConfig.P2P.SentryAddr) == 0
	cfg.HistoryV3 = ctx.Bool(HistoryV3Flag.Name)
	cfg.TransactionsV3 = ctx.Bool(TransactionV3Flag.Name)
	if ctx.IsSet(NetworkIdFlag.Name) {
		cfg.NetworkID = ctx.Uint64(NetworkIdFlag.Name)
	}

	if ctx.IsSet(RPCGlobalGasCapFlag.Name) {
		cfg.RPCGasCap = ctx.Uint64(RPCGlobalGasCapFlag.Name)
	}
	if cfg.RPCGasCap != 0 {
		log.Info("Set global gas cap", "cap", cfg.RPCGasCap)
	} else {
		log.Info("Global gas cap disabled")
	}
	if ctx.IsSet(RPCGlobalTxFeeCapFlag.Name) {
		cfg.RPCTxFeeCap = ctx.Float64(RPCGlobalTxFeeCapFlag.Name)
	}
	if ctx.IsSet(NoDiscoverFlag.Name) {
		cfg.EthDiscoveryURLs = []string{}
	} else if ctx.IsSet(DNSDiscoveryFlag.Name) {
		urls := ctx.String(DNSDiscoveryFlag.Name)
		if urls == "" {
			cfg.EthDiscoveryURLs = []string{}
		} else {
			cfg.EthDiscoveryURLs = SplitAndTrim(urls)
		}
	}
	// Override any default configs for hard coded networks.
	chain := ctx.String(ChainFlag.Name)
	if strings.HasPrefix(chain, "dynamic") {
		configFilePath := ctx.String(ConfigFlag.Name)
		if configFilePath == "" {
			Fatalf("Config file is required for dynamic chain")
		}

		// Be sure to set this first
		params.DynamicChainConfigPath = filepath.Dir(configFilePath)
		filename := path.Join(params.DynamicChainConfigPath, chain+"-conf.json")

		genesis := core.GenesisBlockByChainName(chain)

		dConf := DynamicConfig{}

		if _, err := os.Stat(filename); err == nil {
			dConfBytes, err := os.ReadFile(filename)
			if err != nil {
				panic(err)
			}
			if err := json.Unmarshal(dConfBytes, &dConf); err != nil {
				panic(err)
			}
		}

		genesis.Timestamp = dConf.Timestamp
		genesis.GasLimit = dConf.GasLimit
		genesis.Difficulty = big.NewInt(dConf.Difficulty)

		cfg.Genesis = genesis

		genesisHash := libcommon.HexToHash(dConf.Root)
		if !ctx.IsSet(NetworkIdFlag.Name) {
			log.Warn("NetworkID is not set for dynamic chain", "chain", chain, "networkID", cfg.NetworkID)
		}
		SetDNSDiscoveryDefaults(cfg, genesisHash)
	} else {
		switch chain {
		default:
			genesis := core.GenesisBlockByChainName(chain)
			genesisHash := params.GenesisHashByChainName(chain)
			if (genesis == nil) || (genesisHash == nil) {
				Fatalf("ChainDB name is not recognized: %s", chain)
				return
			}
			cfg.Genesis = genesis
			if !ctx.IsSet(NetworkIdFlag.Name) {
				cfg.NetworkID = params.NetworkIDByChainName(chain)
			}
			SetDNSDiscoveryDefaults(cfg, *genesisHash)
		case "":
			if cfg.NetworkID == 1 {
				SetDNSDiscoveryDefaults(cfg, params.MainnetGenesisHash)
			}
		case networkname.DevChainName:
			if !ctx.IsSet(NetworkIdFlag.Name) {
				cfg.NetworkID = 1337
			}

			// Create new developer account or reuse existing one
			developer := cfg.Miner.Etherbase
			if developer == (libcommon.Address{}) {
				Fatalf("Please specify developer account address using --miner.etherbase")
			}
			log.Info("Using developer account", "address", developer)

			// Create a new developer genesis block or reuse existing one
			cfg.Genesis = core.DeveloperGenesisBlock(uint64(ctx.Int(DeveloperPeriodFlag.Name)), developer)
			log.Info("Using custom developer period", "seconds", cfg.Genesis.Config.Clique.Period)
			if !ctx.IsSet(MinerGasPriceFlag.Name) {
				cfg.Miner.GasPrice = big.NewInt(1)
			}
		}
	}

	if ctx.IsSet(OverrideShanghaiTime.Name) {
		cfg.OverrideShanghaiTime = BigFlagValue(ctx, OverrideShanghaiTime.Name)
		cfg.TxPool.OverrideShanghaiTime = cfg.OverrideShanghaiTime
	}

	if ctx.IsSet(ExternalConsensusFlag.Name) {
		cfg.ExternalCL = ctx.Bool(ExternalConsensusFlag.Name)
	} else {
		cfg.ExternalCL = !clparams.EmbeddedEnabledByDefault(cfg.NetworkID)
	}

	nodeConfig.Http.InternalCL = !cfg.ExternalCL

	if ctx.IsSet(SentryDropUselessPeers.Name) {
		cfg.DropUselessPeers = ctx.Bool(SentryDropUselessPeers.Name)
	}
}

// SetDNSDiscoveryDefaults configures DNS discovery with the given URL if
// no URLs are set.
func SetDNSDiscoveryDefaults(cfg *ethconfig.Config, genesis libcommon.Hash) {
	if cfg.EthDiscoveryURLs != nil {
		return // already set through flags/config
	}
	protocol := "all"
	if url := params.KnownDNSNetwork(genesis, protocol); url != "" {
		cfg.EthDiscoveryURLs = []string{url}
	}
}

func SplitTagsFlag(tagsFlag string) map[string]string {
	tags := strings.Split(tagsFlag, ",")
	tagsMap := map[string]string{}

	for _, t := range tags {
		if t != "" {
			kv := strings.Split(t, "=")

			if len(kv) == 2 {
				tagsMap[kv[0]] = kv[1]
			}
		}
	}

	return tagsMap
}

// MakeConsolePreloads retrieves the absolute paths for the console JavaScript
// scripts to preload before starting.
func MakeConsolePreloads(ctx *cli.Context) []string {
	// Skip preloading if there's nothing to preload
	if ctx.String(PreloadJSFlag.Name) == "" {
		return nil
	}
	// Otherwise resolve absolute paths and return them
	files := strings.Split(ctx.String(PreloadJSFlag.Name), ",")
	preloads := make([]string, 0, len(files))
	for _, file := range files {
		preloads = append(preloads, strings.TrimSpace(file))
	}
	return preloads
}

func CobraFlags(cmd *cobra.Command, urfaveCliFlagsLists ...[]cli.Flag) {
	flags := cmd.PersistentFlags()
	for _, urfaveCliFlags := range urfaveCliFlagsLists {
		for _, flag := range urfaveCliFlags {
			switch f := flag.(type) {
			case *cli.IntFlag:
				flags.Int(f.Name, f.Value, f.Usage)
			case *cli.StringFlag:
				flags.String(f.Name, f.Value, f.Usage)
			case *cli.BoolFlag:
				flags.Bool(f.Name, false, f.Usage)
			default:
				panic(fmt.Errorf("unexpected type: %T", flag))
			}
		}
	}
}
