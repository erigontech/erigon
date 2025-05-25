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
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

type TrieCfg struct {
	db                kv.TemporalRwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload

	historyV3 bool
	agg       *state.Aggregator
}

func StageTrieCfg(db kv.TemporalRwDB, checkRoot, saveNewHashesToDB, badBlockHalt bool, tmpDir string, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload, historyV3 bool) TrieCfg {
	return TrieCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,

		historyV3: historyV3,
	}
}

var ErrInvalidStateRootHash = errors.New("invalid state root hash")

func RebuildPatriciaTrieBasedOnFiles(ctx context.Context, cfg TrieCfg) (common.Hash, error) {
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.blockReader))
	rh, err := state.RebuildCommitmentFiles(ctx, cfg.db, &txNumsReader, log.New())
	if err != nil {
		return empty.RootHash, err
	}
	return common.BytesToHash(rh), err
}
