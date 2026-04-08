// Copyright 2018 The go-ethereum Authors
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

package rawdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

// ReadChainConfig retrieves the consensus settings based on the given genesis hash.
func ReadChainConfig(db kv.Getter, hash common.Hash) (*chain.Config, error) {
	data, err := db.GetOne(kv.ConfigTable, hash[:])
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	var config chain.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("invalid chain config JSON: %x, %w", hash, err)
	}

	if config.BorJSON != nil {
		borConfig := &borcfg.BorConfig{}
		if err := json.Unmarshal(config.BorJSON, borConfig); err != nil {
			return nil, fmt.Errorf("invalid chain config 'bor' JSON: %x, %w", hash, err)
		}
		config.Bor = borConfig
	}
	return &config, nil
}

// WriteChainConfig writes the chain config settings to the database.
func WriteChainConfig(db kv.Putter, hash common.Hash, cfg *chain.Config) error {
	if cfg == nil {
		return nil
	}

	if cfg.Bor != nil {
		borJSON, err := json.Marshal(cfg.Bor)
		if err != nil {
			return fmt.Errorf("failed to JSON encode chain config 'bor': %w", err)
		}
		cfg.BorJSON = borJSON
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to JSON encode chain config: %w", err)
	}

	if err := db.Put(kv.ConfigTable, hash[:], data); err != nil {
		return fmt.Errorf("failed to store chain config: %w", err)
	}
	return nil
}

func WriteGenesisIfNotExist(db kv.RwTx, g *types.Genesis) error {
	has, err := db.Has(kv.ConfigTable, kv.GenesisKey)
	if err != nil {
		return err
	}
	if has {
		return nil
	}

	// Marshal json g
	val, err := json.Marshal(g)
	if err != nil {
		return err
	}
	return db.Put(kv.ConfigTable, kv.GenesisKey, val)
}

func ReadGenesis(db kv.Getter) (*types.Genesis, error) {
	val, err := db.GetOne(kv.ConfigTable, kv.GenesisKey)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 || bytes.Equal(val, []byte("null")) {
		return nil, nil
	}
	var g types.Genesis
	if err := json.Unmarshal(val, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

// GenesisBundle is the set of keys that collectively make a chain DB valid at block 0.
// It is the persistence-layer contract for "a written genesis": the raw genesis JSON,
// the resolved chain config, the block, and its total difficulty. Layer-2 policy
// (genesiswrite) is responsible for computing Block via GenesisToBlock and for deciding
// which Config to store; this layer only moves bytes.
//
// Deliberately excluded from the bundle (owned by other subsystems):
//   - rawdbv3.TxNums[0] — appended by the genesiswrite layer alongside WriteGenesisBundle
//   - DatabaseInfo version stamping
//   - stages progress seeding
//   - snapshot download marker
//
// Every field may be nil when returned by ReadGenesisBundle, in which case that
// particular key was not present in the DB.
type GenesisBundle struct {
	Genesis *types.Genesis
	Config  *chain.Config
	Block   *types.Block
	TD      *big.Int
}

// WriteGenesisBundleOpts selects between the two write modes of WriteGenesisBundle.
// FreshDB == true writes every field of the bundle (used when initializing an empty DB).
// FreshDB == false only rewrites the chain config for an existing stored block — used
// by the policy layer for the config-upgrade and "repair missing chain config" paths.
type WriteGenesisBundleOpts struct {
	FreshDB bool
}

// WriteGenesisBundle persists a GenesisBundle to the given transaction. See
// WriteGenesisBundleOpts for the two modes. The caller's transaction is expected
// to handle rollback on error — this function is fail-fast on the first error.
func WriteGenesisBundle(tx kv.RwTx, b *GenesisBundle, opts WriteGenesisBundleOpts) error {
	if b == nil {
		return fmt.Errorf("WriteGenesisBundle: nil bundle")
	}
	if b.Block == nil {
		return fmt.Errorf("WriteGenesisBundle: bundle.Block is nil")
	}
	if b.Config == nil {
		return fmt.Errorf("WriteGenesisBundle: bundle.Config is nil")
	}
	hash := b.Block.Hash()
	if !opts.FreshDB {
		return WriteChainConfig(tx, hash, b.Config)
	}
	if b.Genesis == nil {
		return fmt.Errorf("WriteGenesisBundle: bundle.Genesis is nil in fresh-DB mode")
	}
	if b.TD == nil {
		return fmt.Errorf("WriteGenesisBundle: bundle.TD is nil in fresh-DB mode")
	}
	if err := WriteGenesisIfNotExist(tx, b.Genesis); err != nil {
		return err
	}
	if err := WriteBlock(tx, b.Block); err != nil {
		return err
	}
	if err := WriteTd(tx, hash, b.Block.NumberU64(), b.TD); err != nil {
		return err
	}
	if err := WriteCanonicalHash(tx, hash, b.Block.NumberU64()); err != nil {
		return err
	}
	WriteHeadBlockHash(tx, hash)
	if err := WriteHeadHeaderHash(tx, hash); err != nil {
		return err
	}
	return WriteChainConfig(tx, hash, b.Config)
}

// ReadGenesisBundle reads back a GenesisBundle. Any field may be nil if the
// corresponding key is not present in the DB — for example, on a freshly
// initialized chaindata directory every field is nil.
func ReadGenesisBundle(tx kv.Getter) (*GenesisBundle, error) {
	b := &GenesisBundle{}

	g, err := ReadGenesis(tx)
	if err != nil {
		return nil, err
	}
	b.Genesis = g

	hash, err := ReadCanonicalHash(tx, 0)
	if err != nil {
		return nil, err
	}
	if hash == (common.Hash{}) {
		return b, nil
	}

	block, _, err := ReadBlockWithSenders(tx, hash, 0)
	if err != nil {
		return nil, err
	}
	b.Block = block

	td, err := ReadTd(tx, hash, 0)
	if err != nil {
		return nil, err
	}
	b.TD = td

	cfg, err := ReadChainConfig(tx, hash)
	if err != nil {
		return nil, err
	}
	b.Config = cfg

	return b, nil
}

func AllSegmentsDownloadComplete(tx kv.Getter) (allSegmentsDownloadComplete bool, err error) {
	snapshotsStageProgress, err := stages.GetStageProgress(tx, stages.Snapshots)
	return snapshotsStageProgress > 0, err
}

func AllSegmentsDownloadCompleteFromDB(db kv.RoDB) (allSegmentsDownloadComplete bool, err error) {
	err = db.View(context.Background(), func(tx kv.Tx) error {
		allSegmentsDownloadComplete, err = AllSegmentsDownloadComplete(tx)
		return err
	})
	return allSegmentsDownloadComplete, err
}
