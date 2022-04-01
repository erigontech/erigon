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
	"io"
	"math/big"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/c2h5oh/datasize"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon/consensus/bor"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/consensusconfig"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/db"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/parlia"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/gasprice"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
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
	SyncMode: FastSync,
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
	TxPool:      core.DefaultTxPoolConfig,
	RPCGasCap:   50000000,
	GPO:         FullNodeGPO,
	RPCTxFeeCap: 1, // 1 ether

	BodyDownloadTimeoutSeconds: 30,

	ImportMode: false,
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
}

func (s Snapshot) String() string {
	var out []string
	if s.Enabled {
		out = append(out, "--syncmode=snap")
	}
	if s.KeepBlocks {
		out = append(out, "--"+FlagSnapshotKeepBlocks+"=true")
	}
	return strings.Join(out, " ")
}

var (
	FlagSnapshot           = "snapshot"
	FlagSnapshotKeepBlocks = "snap.keepblocks"
)

func NewSnapshotCfg(enabled, keepBlocks bool) Snapshot {
	return Snapshot{Enabled: enabled, KeepBlocks: keepBlocks}
}

// Config contains configuration options for ETH protocol.
type Config struct {
	SyncMode SyncMode

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
	Torrent  *torrent.ClientConfig

	TorrentDirCloser io.Closer
	SnapshotDir      *dir.Rw

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
	TxPool core.TxPoolConfig

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
}

func CreateConsensusEngine(chainConfig *params.ChainConfig, logger log.Logger, config interface{}, notify []string, noverify bool, HeimdallURL string, WithoutHeimdall bool, datadir string) consensus.Engine {
	var eng consensus.Engine

	switch consensusCfg := config.(type) {
	case *ethash.Config:
		switch consensusCfg.PowMode {
		case ethash.ModeFake:
			log.Warn("Ethash used in fake mode")
			eng = ethash.NewFaker()
		case ethash.ModeTest:
			log.Warn("Ethash used in test mode")
			eng = ethash.NewTester(nil, noverify)
		case ethash.ModeShared:
			log.Warn("Ethash used in shared mode")
			eng = ethash.NewShared()
		default:
			eng = ethash.New(ethash.Config{
				CachesInMem:      consensusCfg.CachesInMem,
				CachesLockMmap:   consensusCfg.CachesLockMmap,
				DatasetDir:       consensusCfg.DatasetDir,
				DatasetsInMem:    consensusCfg.DatasetsInMem,
				DatasetsOnDisk:   consensusCfg.DatasetsOnDisk,
				DatasetsLockMmap: consensusCfg.DatasetsLockMmap,
			}, notify, noverify)
		}
	case *params.ConsensusSnapshotConfig:
		if chainConfig.Clique != nil {
			eng = clique.New(chainConfig, consensusCfg, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory))
		}
	case *params.AuRaConfig:
		if chainConfig.Aura != nil {
			var err error
			eng, err = aura.NewAuRa(chainConfig.Aura, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory), chainConfig.Aura.Etherbase, consensusconfig.GetConfigByChain(chainConfig.ChainName))
			if err != nil {
				panic(err)
			}
		}
	case *params.ParliaConfig:
		if chainConfig.Parlia != nil {
			eng = parlia.New(chainConfig, db.OpenDatabase(consensusCfg.DBPath, logger, consensusCfg.InMemory))
		}
	case *params.BorConfig:
		if chainConfig.Bor != nil {
			borDbPath := filepath.Join(datadir, "bor") // bor consensus path: datadir/bor
			eng = bor.New(chainConfig, db.OpenDatabase(borDbPath, logger, false), HeimdallURL, WithoutHeimdall)
		}
	}

	if eng == nil {
		panic("unknown config" + spew.Sdump(config))
	}

	if chainConfig.TerminalTotalDifficulty == nil {
		return eng
	} else {
		return serenity.New(eng) // the Merge
	}
}

type SyncMode string

const (
	FastSync SyncMode = "fast"
	SnapSync SyncMode = "snap"
)
