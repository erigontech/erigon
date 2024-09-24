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
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/turbo/services"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/turbo/trie"
)

func collectAndComputeCommitment(ctx context.Context, cfg TrieCfg) ([]byte, error) {
	roTx, err := cfg.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer roTx.Rollback()

	// has to set this value because it will be used during domain.Commit() call.
	// If we do not, txNum of block beginning will be used, which will cause invalid txNum on restart following commitment rebuilding
	//logger := log.New("stage", "patricia_trie", "block", domains.BlockNum())
	logger := log.New()
	domains, err := state.NewSharedDomains(roTx, log.New())
	if err != nil {
		return nil, err
	}
	defer domains.Close()

	rh, err := domains.ComputeCommitment(ctx, false, 0, "")
	if err != nil {
		return nil, err
	}
	logger.Info("Initial commitment", "root", hex.EncodeToString(rh))

	sdCtx := state.NewSharedDomainsCommitmentContext(domains, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)
	fileRanges := sdCtx.Ranges()
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.blockReader))
	for _, r := range fileRanges[kv.AccountsDomain] {
		fromTxNumRange, toTxNumRange := r.FromTo()
		logger.Info("scanning", "range", r.String("", domains.StepSize()))

		ok, blockNum, err := txNumsReader.FindBlockNum(roTx, toTxNumRange-1)
		if err != nil {
			logger.Warn("Failed to find block number for txNum", "txNum", toTxNumRange, "err", err)
			return nil, err
		}
		_ = ok
		domains.SetBlockNum(blockNum)
		domains.SetTxNum(toTxNumRange - 1)

		rh, err := domains.RebuildCommitmentRange(ctx, cfg.db, blockNum, int(fromTxNumRange), int(toTxNumRange))
		if err != nil {
			return nil, err
		}

		domains.Close()
		domains, err = state.NewSharedDomains(roTx, log.New())
		if err != nil {
			return nil, err
		}
		logger.Info("commitment done", "hash", hex.EncodeToString(rh), "range", r.String("", domains.StepSize()), "block", blockNum)
	}

	return rh, nil
}

type blockBorders struct {
	Number    uint64
	FirstTx   uint64
	CurrentTx uint64
	LastTx    uint64
}

func (b blockBorders) Offset() uint64 {
	if b.CurrentTx > b.FirstTx && b.CurrentTx < b.LastTx {
		return b.CurrentTx - b.FirstTx
	}
	return 0
}

func countBlockByTxnum(ctx context.Context, tx kv.Tx, blockReader services.FullBlockReader, txNum uint64) (bb blockBorders, err error) {
	var txCounter uint64 = 0

	for i := uint64(0); i < math.MaxUint64; i++ {
		if i%1000000 == 0 {
			fmt.Printf("\r [%s] Counting block for txn %d: cur block %s cur txn %d\n", "restoreCommit", txNum, libcommon.PrettyCounter(i), txCounter)
		}

		h, err := blockReader.HeaderByNumber(ctx, tx, i)
		if err != nil {
			return blockBorders{}, err
		}

		bb.Number = i
		bb.FirstTx = txCounter
		txCounter++
		b, err := blockReader.BodyWithTransactions(ctx, tx, h.Hash(), i)
		if err != nil {
			return blockBorders{}, err
		}
		txCounter += uint64(len(b.Transactions))
		txCounter++
		bb.LastTx = txCounter

		if txCounter >= txNum {
			bb.CurrentTx = txNum
			return bb, nil
		}
	}
	return blockBorders{}, fmt.Errorf("block with txn %x not found", txNum)
}

type TrieCfg struct {
	db                kv.RwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload

	historyV3 bool
	agg       *state.Aggregator
}

func StageTrieCfg(db kv.RwDB, checkRoot, saveNewHashesToDB, badBlockHalt bool, tmpDir string, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload, historyV3 bool, agg *state.Aggregator) TrieCfg {
	return TrieCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,

		historyV3: historyV3,
		agg:       agg,
	}
}

type HashStateCfg struct {
	db   kv.RwDB
	dirs datadir.Dirs
}

func StageHashStateCfg(db kv.RwDB, dirs datadir.Dirs) HashStateCfg {
	return HashStateCfg{
		db:   db,
		dirs: dirs,
	}
}

var ErrInvalidStateRootHash = errors.New("invalid state root hash")

func RebuildPatriciaTrieBasedOnFiles(cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	//roTx, err := cfg.db.BeginRo(context.Background())
	//if err != nil {
	//	return trie.EmptyRoot, err
	//}
	//defer roTx.Rollback()
	//
	//txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.blockReader))
	//var foundHash bool
	//toTxNum := roTx.(*temporal.Tx).AggTx().(*state.AggregatorRoTx).EndTxNumNoCommitment()
	//ok, blockNum, err := txNumsReader.FindBlockNum(roTx, toTxNum)
	//if err != nil {
	//	return libcommon.Hash{}, err
	//}
	//if !ok {
	//	bb, err := countBlockByTxnum(ctx, roTx, cfg.blockReader, toTxNum)
	//	if err != nil {
	//		return libcommon.Hash{}, err
	//	}
	//	blockNum = bb.Number
	//	foundHash = bb.Offset() != 0
	//} else {
	//	firstTxInBlock, err := txNumsReader.Min(roTx, blockNum)
	//	if err != nil {
	//		return libcommon.Hash{}, fmt.Errorf("failed to find first txNum in block %d : %w", blockNum, err)
	//	}
	//	lastTxInBlock, err := txNumsReader.Max(roTx, blockNum)
	//	if err != nil {
	//		return libcommon.Hash{}, fmt.Errorf("failed to find last txNum in block %d : %w", blockNum, err)
	//	}
	//	if firstTxInBlock == toTxNum || lastTxInBlock == toTxNum {
	//		foundHash = true // state is in the beginning or end of block
	//	}
	//}
	//
	//var expectedRootHash libcommon.Hash
	//var headerHash libcommon.Hash
	//var syncHeadHeader *types.Header
	//if foundHash && cfg.checkRoot {
	//	syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, roTx, blockNum)
	//	if err != nil {
	//		return trie.EmptyRoot, err
	//	}
	//	if syncHeadHeader == nil {
	//		return trie.EmptyRoot, fmt.Errorf("no header found with number %d", blockNum)
	//	}
	//	expectedRootHash = syncHeadHeader.Root
	//	headerHash = syncHeadHeader.Hash()
	//}
	//
	//roTx.Rollback()
	//if err := roTx.Rollback(); err != nil {
	//	return trie.EmptyRoot, err
	//}
	rh, err := collectAndComputeCommitment(ctx, cfg)
	if err != nil {
		return trie.EmptyRoot, err
	}
	_ = rh
	//
	//roTx.(*temporal.Tx).AggTx().(*state.AggregatorRoTx).RebuildCommitmentForEachFileRange()
	//roTx.Rollback()
	//if foundHash && cfg.checkRoot && !bytes.Equal(rh, expectedRootHash[:]) {
	//	logger.Error(fmt.Sprintf("[RebuildCommitment] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", blockNum, rh, expectedRootHash, headerHash))
	//	roTx.Rollback()
	//
	//	return trie.EmptyRoot, errors.New("wrong trie root")
	//}
	//logger.Info(fmt.Sprintf("[RebuildCommitment] Trie root of block %d txNum %d: %x. Could not verify with block hash because txnum of state is in the middle of the block.", blockNum, toTxNum, rh))

	return libcommon.Hash{}, nil
	//return libcommon.BytesToHash(rh), err
}
