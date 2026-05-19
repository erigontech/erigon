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
	existing, err := db.GetOne(kv.ConfigTable, kv.GenesisKey)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		// If the stored blob predates the chain.Config JSON-compat fix, the
		// ChainID / TerminalTotalDifficulty fields are quoted strings, which
		// older binaries (still on *big.Int for those fields) reject. Detect
		// and re-marshal in place so subsequent downgrades can read the DB.
		if needsLegacyGenesisRewrite(existing) {
			var stored types.Genesis
			if err := json.Unmarshal(existing, &stored); err != nil {
				return fmt.Errorf("rewrite legacy genesis JSON: unmarshal: %w", err)
			}
			val, err := json.Marshal(&stored)
			if err != nil {
				return fmt.Errorf("rewrite legacy genesis JSON: marshal: %w", err)
			}
			return db.Put(kv.ConfigTable, kv.GenesisKey, val)
		}
		return nil
	}

	// Marshal json g
	val, err := json.Marshal(g)
	if err != nil {
		return err
	}
	return db.Put(kv.ConfigTable, kv.GenesisKey, val)
}

// needsLegacyGenesisRewrite reports whether the stored genesis JSON has
// chain.Config.ChainID or chain.Config.TerminalTotalDifficulty serialised as
// quoted strings — uint256.Int's default form, incompatible with older
// erigon binaries that decode those fields into *big.Int.
func needsLegacyGenesisRewrite(blob []byte) bool {
	return bytes.Contains(blob, []byte(`"chainId":"`)) ||
		bytes.Contains(blob, []byte(`"terminalTotalDifficulty":"`))
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
