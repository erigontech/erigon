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

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/consensus/ethash/ethashcfg"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/gasprice/gaspricecfg"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
)

// BorDefaultMinerGasPrice defines the minimum gas price for bor validators to mine a transaction.
var BorDefaultMinerGasPrice = big.NewInt(25 * params.GWei)

// FullNodeGPO contains default gasprice oracle settings for full node.
var FullNodeGPO = gaspricecfg.Config{
	Blocks:           20,
	Default:          big.NewInt(0),
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
		ExecWorkerCount:            estimate.ReconstituteState.WorkersHalf(), //only half of CPU, other half will spend for snapshots build/merge/prune
		ReconWorkerCount:           estimate.ReconstituteState.Workers(),
		BodyCacheLimit:             256 * 1024 * 1024,
		BodyDownloadTimeoutSeconds: 2,
		//LoopBlockLimit:             100_000,
		PruneLimit: 100,
	},
	Ethash: ethashcfg.Config{
		CachesInMem:      2,
		CachesLockMmap:   false,
		DatasetsInMem:    1,
		DatasetsOnDisk:   2,
		DatasetsLockMmap: false,
	},
	NetworkID: 1,
	Prune:     prune.DefaultMode,
	Miner: params.MiningConfig{
		GasLimit: 30_000_000,
		GasPrice: big.NewInt(params.GWei),
		Recommit: 3 * time.Second,
	},
	DeprecatedTxPool: DeprecatedDefaultTxPoolConfig,
	TxPool:           txpoolcfg.DefaultConfig,
	RPCGasCap:        50000000,
	GPO:              FullNodeGPO,
	RPCTxFeeCap:      1, // 1 ether

	ImportMode: false,
	Snapshot: BlocksFreezing{
		KeepBlocks: false,
		ProduceE2:  true,
		ProduceE3:  true,
	},
}

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
	KeepBlocks     bool // produce new snapshots of blocks but don't remove blocks from DB
	ProduceE2      bool // produce new block files
	ProduceE3      bool // produce new state files
	NoDownloader   bool // possible to use snapshots without calling Downloader
	Verify         bool // verify snapshots on startup
	DownloaderAddr string
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

func NewSnapCfg(keepBlocks, produceE2, produceE3 bool) BlocksFreezing {
	return BlocksFreezing{KeepBlocks: keepBlocks, ProduceE2: produceE2, ProduceE3: produceE3}
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

	ImportMode bool

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

	// Mining options
	Miner params.MiningConfig

	// Ethash options
	Ethash ethashcfg.Config

	Clique params.ConsensusSnapshotConfig
	Aura   chain.AuRaConfig

	// Transaction pool options
	DeprecatedTxPool DeprecatedTxPoolConfig
	TxPool           txpoolcfg.Config

	// Gas Price Oracle options
	GPO gaspricecfg.Config

	// RPCGasCap is the global gas cap for eth-call variants.
	RPCGasCap uint64 `toml:",omitempty"`

	// RPCTxFeeCap is the global transaction fee(price * gaslimit) cap for
	// send-transction variants. The unit is ether.
	RPCTxFeeCap float64 `toml:",omitempty"`

	StateStream bool

	// URL to connect to Heimdall node
	HeimdallURL string
	// No heimdall service
	WithoutHeimdall bool
	// Heimdall services active
	WithHeimdallMilestones bool
	// Heimdall waypoint recording active
	WithHeimdallWaypointRecording bool
	// Use polygon checkpoint sync in preference to POW downloader
	PolygonSync      bool
	PolygonSyncStage bool

	// Ethstats service
	Ethstats string
	// Consensus layer
	InternalCL bool

	OverridePragueTime *big.Int `toml:",omitempty"`

	// Embedded Silkworm support
	SilkwormExecution            bool
	SilkwormRpcDaemon            bool
	SilkwormSentry               bool
	SilkwormVerbosity            string
	SilkwormNumContexts          uint32
	SilkwormRpcLogEnabled        bool
	SilkwormRpcLogDirPath        string
	SilkwormRpcLogMaxFileSize    uint16
	SilkwormRpcLogMaxFiles       uint16
	SilkwormRpcLogDumpResponse   bool
	SilkwormRpcNumWorkers        uint32
	SilkwormRpcJsonCompatibility bool

	DisableTxPoolGossip bool
}

type Sync struct {
	// LoopThrottle sets a minimum time between staged loop iterations
	LoopThrottle     time.Duration
	ExecWorkerCount  int
	ReconWorkerCount int

	BodyCacheLimit             datasize.ByteSize
	BodyDownloadTimeoutSeconds int // TODO: change to duration
	PruneLimit                 int //the maximum records to delete from the DB during pruning
	BreakAfterStage            string
	LoopBlockLimit             uint

	UploadLocation   string
	UploadFrom       rpc.BlockNumber
	FrozenBlockLimit uint64
}
