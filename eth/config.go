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

package eth

import (
	"fmt"
	"math/big"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/eth/gasprice"
	"github.com/ledgerwatch/turbo-geth/miner"
	"github.com/ledgerwatch/turbo-geth/params"
)

// DefaultConfig contains default settings for use on the Ethereum main net.
var DefaultConfig = Config{
	SyncMode: downloader.FullSync,
	Ethash: ethash.Config{
		CacheDir:       "ethash",
		CachesInMem:    2,
		CachesOnDisk:   3,
		DatasetsInMem:  1,
		DatasetsOnDisk: 2,
	},
	NetworkID:          1,
	LightPeers:         100,
	UltraLightFraction: 75,
	DatabaseCache:      512,
	TrieCleanCache:     256,
	TrieDirtyCache:     256,
	TrieTimeout:        60 * time.Minute,
	StorageMode:        DefaultStorageMode,
	Miner: miner.Config{
		GasFloor: 8000000,
		GasCeil:  8000000,
		GasPrice: big.NewInt(params.GWei),
		Recommit: 3 * time.Second,
	},
	TxPool: core.DefaultTxPoolConfig,
	GPO: gasprice.Config{
		Blocks:     20,
		Percentile: 60,
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
		DefaultConfig.Ethash.DatasetDir = filepath.Join(home, "Library", "Ethash")
	} else if runtime.GOOS == "windows" {
		localappdata := os.Getenv("LOCALAPPDATA")
		if localappdata != "" {
			DefaultConfig.Ethash.DatasetDir = filepath.Join(localappdata, "Ethash")
		} else {
			DefaultConfig.Ethash.DatasetDir = filepath.Join(home, "AppData", "Local", "Ethash")
		}
	} else {
		DefaultConfig.Ethash.DatasetDir = filepath.Join(home, ".ethash")
	}
}

type StorageMode struct {
	History              bool
	Receipts             bool
	TxIndex              bool
	Preimages            bool
	IntermediateTrieHash bool
}

var DefaultStorageMode = StorageMode{History: true, Receipts: false, TxIndex: true, Preimages: true}

func (m StorageMode) ToString() string {
	modeString := ""
	if m.History {
		modeString += "h"
	}
	if m.Preimages {
		modeString += "p"
	}
	if m.Receipts {
		modeString += "r"
	}
	if m.TxIndex {
		modeString += "t"
	}
	if m.IntermediateTrieHash {
		modeString += "i"
	}
	return modeString
}

func StorageModeFromString(flags string) (StorageMode, error) {
	mode := StorageMode{}
	for _, flag := range flags {
		switch flag {
		case 'h':
			mode.History = true
		case 'r':
			mode.Receipts = true
		case 't':
			mode.TxIndex = true
		case 'p':
			mode.Preimages = true
		case 'i':
			mode.IntermediateTrieHash = true
		default:
			return mode, fmt.Errorf("unexpected flag found: %c", flag)
		}
	}

	return mode, nil
}

//go:generate gencodec -type Config -formats toml -out gen_config.go

type Config struct {
	// The genesis block, which is inserted if the database is empty.
	// If nil, the Ethereum main net block is used.
	Genesis *core.Genesis `toml:",omitempty"`

	// Protocol options
	NetworkID uint64 // Network ID to use for selecting peers to connect to
	SyncMode  downloader.SyncMode

	// This can be set to list of enrtree:// URLs which will be queried for
	// for nodes to connect to.
	DiscoveryURLs []string

	NoPruning  bool // Whether to disable pruning and flush everything to disk
	NoPrefetch bool // Whether to disable prefetching and only load state on demand

	StorageMode StorageMode

	// DownloadOnly is set when the node does not need to process the blocks, but simply
	// download them
	DownloadOnly        bool
	ArchiveSyncInterval int
	BlocksBeforePruning uint64
	BlocksToPrune       uint64
	PruningTimeout      time.Duration

	// Whitelist of required block number -> hash values to accept
	Whitelist map[uint64]common.Hash `toml:"-"`

	// Light client options
	LightServ    int `toml:",omitempty"` // Maximum percentage of time allowed for serving LES requests
	LightIngress int `toml:",omitempty"` // Incoming bandwidth limit for light servers
	LightEgress  int `toml:",omitempty"` // Outgoing bandwidth limit for light servers
	LightPeers   int `toml:",omitempty"` // Maximum number of LES client peers

	// Ultra Light client options
	UltraLightServers      []string `toml:",omitempty"` // List of trusted ultra light servers
	UltraLightFraction     int      `toml:",omitempty"` // Percentage of trusted servers to accept an announcement
	UltraLightOnlyAnnounce bool     `toml:",omitempty"` // Whether to only announce headers, or also serve them

	// Database options
	SkipBcVersionCheck bool `toml:"-"`
	DatabaseHandles    int  `toml:"-"`
	DatabaseCache      int
	DatabaseFreezer    string

	TrieCleanCache int
	TrieDirtyCache int
	TrieTimeout    time.Duration

	// Mining options
	Miner miner.Config

	// Ethash options
	Ethash ethash.Config

	// Transaction pool options
	TxPool core.TxPoolConfig

	// Gas Price Oracle options
	GPO gasprice.Config

	// Enables tracking of SHA3 preimages in the VM
	EnablePreimageRecording bool

	// Miscellaneous options
	DocRoot string `toml:"-"`

	// Type of the EWASM interpreter ("" for default)
	EWASMInterpreter string

	// Type of the EVM interpreter ("" for default)
	EVMInterpreter string

	// RPCGasCap is the global gas cap for eth-call variants.
	RPCGasCap *big.Int `toml:",omitempty"`

	// Checkpoint is a hardcoded checkpoint which can be nil.
	Checkpoint *params.TrustedCheckpoint `toml:",omitempty"`

	// CheckpointOracle is the configuration for checkpoint oracle.
	CheckpointOracle *params.CheckpointOracleConfig `toml:",omitempty"`

	// Istanbul block override (TODO: remove after the fork)
	OverrideIstanbul *big.Int `toml:",omitempty"`

	// MuirGlacier block override (TODO: remove after the fork)
	OverrideMuirGlacier *big.Int `toml:",omitempty"`
}
