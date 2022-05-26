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
	"fmt"
	"math/big"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	txpool2 "github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/torrentcfg"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/gasprice"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
)

// FullNodeGPO contains default gasprice oracle settings for full node.
var FullNodeGPO = gasprice.Config{
	Blocks:           20,
	Default:          big.NewInt(0),
	Percentile:       60,
	MaxHeaderHistory: 0,
	MaxBlockHistory:  0,
	MaxPrice:         gasprice.DefaultMaxPrice,
	IgnorePrice:      gasprice.DefaultIgnorePrice,
}

// LightClientGPO contains default gasprice oracle settings for light client.
var LightClientGPO = gasprice.Config{
	Blocks:           2,
	Percentile:       60,
	MaxHeaderHistory: 300,
	MaxBlockHistory:  5,
	MaxPrice:         gasprice.DefaultMaxPrice,
	IgnorePrice:      gasprice.DefaultIgnorePrice,
}

// Defaults contains default settings for use on the Ethereum main net.
var Defaults = Config{
	SyncMode: FullSync,
	Ethash: ethash.Config{
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
	DeprecatedTxPool: core.DeprecatedDefaultTxPoolConfig,
	RPCGasCap:        50000000,
	GPO:              FullNodeGPO,
	RPCTxFeeCap:      1, // 1 ether

	BodyDownloadTimeoutSeconds: 30,

	ImportMode: false,
	Snapshot: Snapshot{
		Enabled:    false,
		KeepBlocks: false,
		Produce:    true,
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
		Defaults.Ethash.DatasetDir = filepath.Join(home, ".local/share/erigon-ethash")
	}
}

//go:generate gencodec -type Config -formats toml -out gen_config.go

type Snapshot struct {
	Enabled    bool
	KeepBlocks bool
	Produce    bool // produce new snapshots
}

func (s Snapshot) String() string {
	var out []string
	if s.Enabled {
		out = append(out, "--syncmode=snap")
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

func NewSnapshotCfg(enabled, keepBlocks, produce bool) Snapshot {
	return Snapshot{Enabled: enabled, KeepBlocks: keepBlocks, Produce: produce}
}

// Config contains configuration options for ETH protocol.
type Config struct {
	txpoolCfg   txpool2.Config
	SyncModeCli string
	SyncMode    SyncMode

	// The genesis block, which is inserted if the database is empty.
	// If nil, the Ethereum main net block is used.
	Genesis *core.Genesis `toml:",omitempty"`

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

	Snapshot Snapshot
	Torrent  *torrentcfg.Cfg

	SnapDir string

	BlockDownloaderWindow int

	// Address to connect to external snapshot downloader
	// empty if you want to use internal bittorrent snapshot downloader
	ExternalSnapshotDownloaderAddr string

	// Whitelist of required block number -> hash values to accept
	Whitelist map[uint64]common.Hash `toml:"-"`

	// Mining options
	Miner params.MiningConfig

	// Ethash options
	Ethash ethash.Config

	Clique params.ConsensusSnapshotConfig
	Aura   params.AuRaConfig
	Parlia params.ParliaConfig
	Bor    params.BorConfig

	// Transaction pool options
	DeprecatedTxPool core.TxPoolConfig
	TxPool           txpool2.Config

	// Gas Price Oracle options
	GPO gasprice.Config

	// RPCGasCap is the global gas cap for eth-call variants.
	RPCGasCap uint64 `toml:",omitempty"`

	// RPCTxFeeCap is the global transaction fee(price * gaslimit) cap for
	// send-transction variants. The unit is ether.
	RPCTxFeeCap float64 `toml:",omitempty"`

	StateStream                bool
	BodyDownloadTimeoutSeconds int // TODO change to duration

	// SyncLoopThrottle sets a minimum time between staged loop iterations
	SyncLoopThrottle time.Duration

	// Enable WatchTheBurn stage
	EnabledIssuance bool

	// URL to connect to Heimdall node
	HeimdallURL string

	// No heimdall service
	WithoutHeimdall bool
	// Ethstats service
	Ethstats string
}

type SyncMode string

const (
	FullSync SyncMode = "full"
	SnapSync SyncMode = "snap"
)

func SyncModeByChainName(chain, syncCliFlag string) (SyncMode, error) {
	if syncCliFlag == "full" {
		return FullSync, nil
	} else if syncCliFlag == "snap" {
		return SnapSync, nil
	} else if syncCliFlag != "" {
		return FullSync, fmt.Errorf("unexpected syncmode: %s, only next options are valid: %s, %s", syncCliFlag, FullSync, SnapSync)
	}
	switch chain {
	case networkname.MainnetChainName, networkname.BSCChainName, networkname.GoerliChainName,
		networkname.RopstenChainName:
		return SnapSync, nil
	default:
		return FullSync, nil
	}
}
