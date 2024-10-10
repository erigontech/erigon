// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

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
	"github.com/gateway-fm/cdk-erigon-lib/chain"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/downloader/downloadercfg"
	"github.com/gateway-fm/cdk-erigon-lib/txpool/txpoolcfg"

	"github.com/ledgerwatch/erigon/consensus/ethash/ethashcfg"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/gasprice/gaspricecfg"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
)

// AggregationStep number of transactions in smallest static file
const HistoryV3AggregationStep = 3_125_000 // 100M / 32
//const HistoryV3AggregationStep = 3_125_000 / 100 // use this to reduce step size for dev/debug

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
		UseSnapshots:               false,
		ExecWorkerCount:            estimate.ReconstituteState.WorkersHalf(), //only half of CPU, other half will spend for snapshots build/merge/prune
		ReconWorkerCount:           estimate.ReconstituteState.Workers(),
		BodyCacheLimit:             256 * 1024 * 1024,
		BodyDownloadTimeoutSeconds: 30,
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
	RPCGasCap:        50000000,
	GPO:              FullNodeGPO,
	RPCTxFeeCap:      0, // removed by default

	ImportMode: false,
	Snapshot: Snapshot{
		Enabled:    false,
		KeepBlocks: false,
		Produce:    true,
	},
	DropUselessPeers: false,
	Zk:               &Zk{},
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
		Defaults.Ethash.DatasetDir = filepath.Join(home, ".local/share/erigon-ethash")
	}
}

//go:generate gencodec -dir . -type Config -formats toml -out gen_config.go

type Snapshot struct {
	Enabled        bool
	KeepBlocks     bool // produce new snapshots of blocks but don't remove blocks from DB
	Produce        bool // produce new snapshots
	NoDownloader   bool // possible to use snapshots without calling Downloader
	Verify         bool // verify snapshots on startup
	DownloaderAddr string
}

func (s Snapshot) String() string {
	var out []string
	if s.Enabled {
		out = append(out, "--snapshots=true")
	}
	if s.KeepBlocks {
		out = append(out, "--"+FlagSnapKeepBlocks+"=true")
	}
	if !s.Produce {
		out = append(out, "--"+FlagSnapStop+"=true")
	}
	return strings.Join(out, " ")
}

var (
	FlagSnapKeepBlocks = "snap.keepblocks"
	FlagSnapStop       = "snap.stop"
)

func NewSnapCfg(enabled, keepBlocks, produce bool) Snapshot {
	return Snapshot{Enabled: enabled, KeepBlocks: keepBlocks, Produce: produce}
}

// Config contains configuration options for ETH protocol.
type Config struct {
	Sync Sync

	// The genesis block, which is inserted if the database is empty.
	// If nil, the Ethereum main net block is used.
	Genesis *types.Genesis `toml:",omitempty"`

	// Protocol options
	NetworkID uint64 // Network ID to use for selecting peers to connect to

	// This can be set to list of enrtree:// URLs which will be queried for
	// for nodes to connect to.
	EthDiscoveryURLs []string

	P2PEnabled bool

	Prune     prune.Mode
	BatchSize datasize.ByteSize // Batch size for execution stage

	ImportMode bool

	BadBlockHash common.Hash // hash of the block marked as bad

	Snapshot   Snapshot
	Downloader *downloadercfg.Cfg

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
	Bor    chain.BorConfig

	// Transaction pool options
	DeprecatedTxPool DeprecatedTxPoolConfig
	TxPool           txpoolcfg.Config

	YieldSize uint64

	// Gas Price Oracle options
	GPO gaspricecfg.Config

	// RPCGasCap is the global gas cap for eth-call variants.
	RPCGasCap uint64 `toml:",omitempty"`

	// RPCTxFeeCap is the global transaction fee(price * gaslimit) cap for
	// send-transction variants. The unit is ether.
	RPCTxFeeCap float64 `toml:",omitempty"`

	StateStream bool

	//  New DB and Snapshots format of history allows: parallel blocks execution, get state as of given transaction without executing whole block.",
	HistoryV3 bool

	// gRPC Address to connect to Heimdall node
	HeimdallgRPCAddress string

	//  New DB table for storing transactions allows: keeping multiple branches of block bodies in the DB simultaneously
	TransactionsV3 bool

	// URL to connect to Heimdall node
	HeimdallURL string

	// No heimdall service
	WithoutHeimdall bool
	// Ethstats service
	Ethstats string
	// Consensus layer
	ExternalCL                  bool
	LightClientDiscoveryAddr    string
	LightClientDiscoveryPort    uint64
	LightClientDiscoveryTCPPort uint64
	SentinelAddr                string
	SentinelPort                uint64

	OverrideShanghaiTime *big.Int `toml:",omitempty"`

	DropUselessPeers bool

	//[zkevm]
	*Zk
}

type Sync struct {
	UseSnapshots bool
	// LoopThrottle sets a minimum time between staged loop iterations
	LoopThrottle     time.Duration
	ExecWorkerCount  int
	ReconWorkerCount int

	BodyCacheLimit             datasize.ByteSize
	BodyDownloadTimeoutSeconds int // TODO: change to duration
}

// Chains where snapshots are enabled by default
var ChainsWithSnapshots = map[string]struct{}{
	networkname.MainnetChainName:    {},
	networkname.SepoliaChainName:    {},
	networkname.GoerliChainName:     {},
	networkname.MumbaiChainName:     {},
	networkname.BorMainnetChainName: {},
	networkname.GnosisChainName:     {},
	networkname.ChiadoChainName:     {},
}

func UseSnapshotsByChainName(chain string) bool {
	_, ok := ChainsWithSnapshots[chain]
	return ok
}
