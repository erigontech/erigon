// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

// Package ethconfig contains the configuration of the ETH and LES protocols.
package ethconfig

import (
	"math/big"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash/ethashcfg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/gasprice/gaspricecfg"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// BorDefaultMinerGasPrice defines the minimum gas price for bor validators to mine a transaction.
var BorDefaultMinerGasPrice = big.NewInt(25 * common.GWei)

// Fail-back block gas limit. Better specify one in the chain config.
const DefaultBlockGasLimit uint64 = 60_000_000

func DefaultBlockGasLimitByChain(chainConfig *chain.Config) uint64 {
	if chainConfig.DefaultBlockGasLimit == nil {
		return DefaultBlockGasLimit
	}
	return *chainConfig.DefaultBlockGasLimit
}

// FullNodeGPO contains default gasprice oracle settings for full node.
var FullNodeGPO = gaspricecfg.Config{
	Blocks:           20,
	Default:          uint256.NewInt(0),
	Percentile:       60,
	MaxHeaderHistory: 0,
	MaxBlockHistory:  0,
	MaxPrice:         gaspricecfg.DefaultMaxPrice,
	IgnorePrice:      gaspricecfg.DefaultIgnorePrice,
}

// LightClientGPO contains default gasprice oracle settings for light client.
var LightClientGPO = gaspricecfg.Config{
	Blocks:           2,
	Percentile:       60,
	MaxHeaderHistory: 300,
	MaxBlockHistory:  5,
	MaxPrice:         gaspricecfg.DefaultMaxPrice,
	IgnorePrice:      gaspricecfg.DefaultIgnorePrice,
}

// Defaults contains default settings for use on the Ethereum main net.
var Defaults = Config{
	Sync: Sync{
		ExecWorkerCount:            dbg.Exec3Workers, //only half of CPU, other half will spend for snapshots build/merge/prune
		BodyCacheLimit:             256 * 1024 * 1024,
		BodyDownloadTimeoutSeconds: 2,
		//LoopBlockLimit:             100_000,
		ParallelStateFlushing:    true,
		ChaosMonkey:              false,
		AlwaysGenerateChangesets: !dbg.BatchCommitments,
		MaxReorgDepth:            dbg.MaxReorgDepth,
	},
	Ethash: ethashcfg.Config{
		CachesInMem:      2,
		CachesLockMmap:   false,
		DatasetsInMem:    1,
		DatasetsOnDisk:   2,
		DatasetsLockMmap: false,
	},
	NetworkID:   1,
	Prune:       prune.DefaultMode,
	TxPool:      txpoolcfg.DefaultConfig,
	RPCGasCap:   50000000,
	GPO:         FullNodeGPO,
	RPCTxFeeCap: 1, // 1 ether
	Snapshot: BlocksFreezing{
		KeepBlocks: false,
		ProduceE2:  true,
		ProduceE3:  true,
	},
	FcuTimeout:          1 * time.Second,
	FcuBackgroundPrune:  true,
	FcuBackgroundCommit: false, // to enable, we need to 1) have rawdb API go via execctx and 2) revive Coherent cache for rpcdaemon
	ExperimentalBAL:     false,
}

const DefaultChainDBPageSize = 16 * datasize.KB

func init() {
	home := os.Getenv("HOME")
	if home == "" {
		if user, err := user.Current(); err == nil {
			home = user.HomeDir
		}
	}
	if runtime.GOOS == "darwin" {
		Defaults.Ethash.DatasetDir = filepath.Join(home, "Library", "erigon-ethash")
	} else if runtime.GOOS == "windows" {
		localappdata := os.Getenv("LOCALAPPDATA")
		if localappdata != "" {
			Defaults.Ethash.DatasetDir = filepath.Join(localappdata, "erigon-thash")
		} else {
			Defaults.Ethash.DatasetDir = filepath.Join(home, "AppData", "Local", "erigon-ethash")
		}
	} else {
		if xdgDataDir := os.Getenv("XDG_DATA_HOME"); xdgDataDir != "" {
			Defaults.Ethash.DatasetDir = filepath.Join(xdgDataDir, "erigon-ethash")
		}
		Defaults.Ethash.DatasetDir = filepath.Join(home, ".local/share/erigon-ethash") //nolint:gocritic
	}
}

//go:generate gencodec -dir . -type Config -formats toml -out gen_config.go

type BlocksFreezing struct {
	KeepBlocks        bool // produce new snapshots of blocks but don't remove blocks from DB
	ProduceE2         bool // produce new block files
	ProduceE3         bool // produce new state files
	NoDownloader      bool // possible to use snapshots without calling Downloader
	P2PManifest       bool // discover snapshot manifest from P2P peers instead of centralized preverified.toml
	DisableDownloadE3 bool // disable download state snapshots
	DownloaderAddr    string
	ChainName         string
	// ManifestReady is closed when P2P manifest discovery completes.
	// Set by the backend when P2PManifest is enabled. Nil otherwise.
	ManifestReady <-chan struct{}

	// InitialStateReady is closed when the storage component's flow
	// orchestrator publishes flow.InitialStateReady (header chain +
	// state at snapshot tip is in place). Plumbed into staged-sync's
	// OtterSync gate so stages 2-6 run concurrently with the
	// historical-tail download. Set by backend.go from
	// storage.Provider.InitialStateReady when LifecycleDrivenByStorage
	// is enabled. Nil otherwise.
	InitialStateReady <-chan struct{}

	// LifecycleDrivenByStorage cuts over file-import orchestration
	// (BuildMissedIndices, BuildMissedAccessors) from the stage loop
	// to the storage component's lifecycle driver. Defaults false:
	// the existing stage-driven path is authoritative.
	//
	// When flipped to true, stage_snapshots.go's index-build calls
	// no-op and storage.Provider's lifecycle.Driver runs
	// runIndexing / runValidation autonomously.
	//
	// Survives the bedding-in period as a kill-switch (per
	// docs/plans/20260501-storage-lifecycle-spec.md). Removed only
	// after the storage-driven path has accumulated production hours
	// across multiple releases.
	LifecycleDrivenByStorage bool

	// BootstrapFromPreverified opts a node into using preverified.toml
	// as its initial download set + the seed of its published
	// chain.toml. Default false — V2 nodes use peer-discovered
	// chain.toml exclusively.
	//
	// Operators set this on the initial publisher for a chain rollout
	// or for recovery scenarios. Once running, the bootstrap publishes
	// chain.toml = preverified ∪ local-files via P2P; subsequent V2
	// nodes inherit that view. preverified is invisible to non-bootstrap
	// V2 nodes.
	//
	// See docs/plans/20260502-app-integration-completion.md §5b for the
	// behaviour matrix. Active only when P2PManifest is true.
	BootstrapFromPreverified bool

	// DelegationPath is the operator-configured path to the snapshotauth
	// UCAN delegation this node attests its V2 manifests with
	// (--snapshot.delegation). Empty → snapshotauth.LoadOrGenerateDelegation
	// falls back to <datadir>/snapshot.ucan and self-signs a bootstrap
	// delegation on first run.
	DelegationPath string

	// TrustRoots is the operator override for the consumer-side UCAN
	// trust gate (--snapshot.trust-roots): a comma-separated list of
	// enr:/did:key:/hex-pubkey roots. Empty → use the compiled-in
	// per-chain default (snapcfg.GetEmbeddedTrustRoots). "any" → trust
	// every peer even when the binary ships a populated default.
	TrustRoots string

	// QuorumFloor overrides the canonical quorum floor Q_floor — the
	// minimum number of distinct trust-verified publishers that must
	// advertise an entry before it is promoted to canonical
	// (--snapshot.quorum). 0 → use the per-chain default
	// (snapcfg.QuorumConfigFor).
	QuorumFloor int

	// AdoptionPolicy governs how far a minority publisher carries
	// staged canonical adoption automatically (--snapshot.adoption-policy):
	// "auto", "stage" or "warn". Parsed by snapshotsync.ParseAdoptionPolicy;
	// empty defaults to "auto".
	AdoptionPolicy string

	// RevalidationPolicy governs how a publisher reacts when startup
	// re-validation finds a bad local snapshot file
	// (--snapshot.revalidation-policy): "redownload", "stop" or "warn".
	// Parsed by snapshotsync.ParseRevalidationPolicy; empty defaults to
	// "redownload".
	RevalidationPolicy string

	// AdoptionGrace is how long a minority verdict must persist before
	// the publisher triggers staged canonical adoption
	// (--snapshot.adoption-grace). A grace window so a transient swarm
	// disagreement settles instead of kicking off a fetch + cutover.
	// 0 → adopt on first minority detection.
	AdoptionGrace time.Duration

	// BlockAlignedBoundaries opts the publisher into the aligned
	// snapshot-boundary convention (--snap.block-aligned-boundaries):
	// chooseSegmentEnd returns literal block coordinates instead of
	// rounding down to the nearest 1k. Eliminates partial-block
	// straddles by construction; every retired file's To matches a
	// real block. Defaults false → legacy 1k-rounded convention used
	// by every existing preverified.toml. See
	// memory/block-slot-aligned-storage-model-2026-05-24.
	BlockAlignedBoundaries bool
}

func (s BlocksFreezing) String() string {
	var out []string
	if s.KeepBlocks {
		out = append(out, "--"+FlagSnapKeepBlocks+"=true")
	}
	if !s.ProduceE2 {
		out = append(out, "--"+FlagSnapStop+"=true")
	}
	return strings.Join(out, " ")
}

var (
	FlagSnapKeepBlocks = "snap.keepblocks"
	FlagSnapStop       = "snap.stop"
	FlagSnapStateStop  = "snap.state.stop"
)

func NewSnapCfg(keepBlocks, produceE2, produceE3 bool, chainName string) BlocksFreezing {
	return BlocksFreezing{KeepBlocks: keepBlocks, ProduceE2: produceE2, ProduceE3: produceE3, ChainName: chainName}
}

// Config contains configuration options for ETH protocol.
type Config struct {
	Sync

	// The genesis block, which is inserted if the database is empty.
	// If nil, the Ethereum main net block is used.
	Genesis *types.Genesis `toml:",omitempty"`

	// Protocol options
	NetworkID uint64 // Network ID to use for selecting peers to connect to

	// This can be set to list of enrtree:// URLs which will be queried for
	// for nodes to connect to.
	EthDiscoveryURLs []string

	Prune     prune.Mode
	BatchSize datasize.ByteSize // Batch size for execution stage

	BadBlockHash common.Hash // hash of the block marked as bad

	Snapshot     BlocksFreezing
	Downloader   *downloadercfg.Cfg
	CaplinConfig clparams.CaplinConfig

	Dirs datadir.Dirs

	// Address to connect to external snapshot downloader
	// empty if you want to use internal bittorrent snapshot downloader
	ExternalSnapshotDownloaderAddr string

	// Whitelist of required block number -> hash values to accept
	Whitelist map[uint64]common.Hash `toml:"-"`

	// Block builder options
	Builder buildercfg.BuilderConfig

	// Ethash options
	Ethash ethashcfg.Config

	Aura chain.AuRaConfig

	// Transaction pool options
	TxPool  txpoolcfg.Config
	Shutter shuttercfg.Config

	// Gas Price Oracle options
	GPO gaspricecfg.Config

	// RPCGasCap is the global gas cap for eth-call variants.
	RPCGasCap uint64 `toml:",omitempty"`

	// RPCTxFeeCap is the global transaction fee(price * gaslimit) cap for
	// send-transction variants. The unit is ether.
	RPCTxFeeCap float64 `toml:",omitempty"`

	StateStream bool

	ExperimentalBAL bool

	// URL to connect to Heimdall node
	HeimdallURL string
	// No heimdall service
	WithoutHeimdall bool

	// Ethstats service
	Ethstats string
	// Consensus layer
	InternalCL bool

	OverrideOsakaTime     *uint64 `toml:",omitempty"`
	OverrideAmsterdamTime *uint64 `toml:",omitempty"`

	// Whether to avoid overriding chain config already stored in the DB
	KeepStoredChainConfig bool

	// PoS Single Slot finality
	PolygonPosSingleSlotFinality        bool
	PolygonPosSingleSlotFinalityBlockAt uint64

	// Account Abstraction
	AllowAA bool

	// fork choice update timeout
	FcuTimeout          time.Duration
	FcuBackgroundPrune  bool
	FcuBackgroundCommit bool

	MCPAddress string

	// ErigondbDomainStepsInFrozenFile overrides erigondb.toml stepsInFrozenFile for the
	// domain merge cap only (history/II are unaffected). nil = no override;
	// config3.UnboundedDomainMerge disables the cap; any other positive value is used
	// directly as the cap in steps.
	ErigondbDomainStepsInFrozenFile *uint64 `toml:",omitempty"`
}

type Sync struct {
	// LoopThrottle sets a minimum time between staged loop iterations
	LoopThrottle     time.Duration
	ExecWorkerCount  int
	ReconWorkerCount int

	BodyCacheLimit             datasize.ByteSize
	BodyDownloadTimeoutSeconds int // TODO: change to duration
	BreakAfterStage            string
	LoopBlockLimit             uint
	ParallelStateFlushing      bool

	ChaosMonkey                      bool
	AlwaysGenerateChangesets         bool
	MaxReorgDepth                    uint64
	KeepExecutionProofs              bool
	ExperimentalConcurrentCommitment bool
	PersistReceiptsCacheV2           bool
	SnapshotDownloadToBlock          uint64 // exclusive [0,toBlock)
}
