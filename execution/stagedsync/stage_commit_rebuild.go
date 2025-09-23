// Copyright 2024 The Erigon Authors
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

package stagedsync

import (
	"context"
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/trie"
	"github.com/erigontech/erigon/turbo/services"
)

type TrieCfg struct {
	db                kv.TemporalRwDB
	checkRoot         bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader

	agg *state.Aggregator
}

func StageTrieCfg(db kv.TemporalRwDB, checkRoot, saveNewHashesToDB bool, tmpDir string, blockReader services.FullBlockReader) TrieCfg {
	return TrieCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		blockReader:       blockReader,
	}
}

var ErrInvalidStateRootHash = errors.New("invalid state root hash")

func RebuildPatriciaTrieBasedOnFiles(ctx context.Context, cfg TrieCfg, squeeze bool) (common.Hash, error) {
	txNumsReader := cfg.blockReader.TxnumReader(ctx)
	rh, err := state.RebuildCommitmentFiles(ctx, cfg.db, &txNumsReader, log.New(), squeeze)
	if err != nil {
		return trie.EmptyRoot, err
	}
	return common.BytesToHash(rh), err
}
