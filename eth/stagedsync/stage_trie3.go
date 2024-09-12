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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/turbo/services"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/trie"
)

func collectAndComputeCommitment(ctx context.Context, tx kv.RwTx, tmpDir string, toTxNum uint64) ([]byte, error) {
	domains, err := state.NewSharedDomains(tx, log.New())
	if err != nil {
		return nil, err
	}
	defer domains.Close()
	ac := domains.AggTx().(*state.AggregatorRoTx)

	// has to set this value because it will be used during domain.Commit() call.
	// If we do not, txNum of block beginning will be used, which will cause invalid txNum on restart following commitment rebuilding
	domains.SetTxNum(toTxNum)

	logger := log.New("stage", "patricia_trie", "block", domains.BlockNum())
	logger.Info("Collecting account/storage keys")
	collector := etl.NewCollector("collect_keys", tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize/2), logger)
	defer collector.Close()

	var totalKeys atomic.Uint64
	it, err := ac.DomainRangeLatest(tx, kv.AccountsDomain, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if err := collector.Collect(k, nil); err != nil {
			return nil, err
		}
		totalKeys.Add(1)
	}

	it, err = ac.DomainRangeLatest(tx, kv.CodeDomain, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if err := collector.Collect(k, nil); err != nil {
			return nil, err
		}
		totalKeys.Add(1)
	}

	it, err = ac.DomainRangeLatest(tx, kv.StorageDomain, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if err := collector.Collect(k, nil); err != nil {
			return nil, err
		}
		totalKeys.Add(1)
	}

	var (
		batchSize = uint64(10_000_000)
		processed atomic.Uint64
	)

	sdCtx := state.NewSharedDomainsCommitmentContext(domains, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)

	loadKeys := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if sdCtx.KeysCount() >= batchSize {
			rh, err := sdCtx.ComputeCommitment(ctx, true, domains.BlockNum(), "")
			if err != nil {
				return err
			}
			logger.Info("Committing batch",
				"processed", fmt.Sprintf("%s/%s (%.2f%%)", libcommon.PrettyCounter(processed.Load()), libcommon.PrettyCounter(totalKeys.Load()),
					float64(processed.Load())/float64(totalKeys.Load())*100),
				"intermediate root", hex.EncodeToString(rh))
		}
		processed.Add(1)
		sdCtx.TouchKey(kv.AccountsDomain, string(k), nil)

		return nil
	}
	err = collector.Load(nil, "", loadKeys, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return nil, err
	}
	collector.Close()

	rh, err := sdCtx.ComputeCommitment(ctx, true, domains.BlockNum(), "")
	if err != nil {
		return nil, err
	}
	logger.Info("Commitment has been reevaluated",
		"tx", domains.TxNum(),
		"root", hex.EncodeToString(rh),
		"processed", processed.Load(),
		"total", totalKeys.Load())

	if err := domains.Flush(ctx, tx); err != nil {
		return nil, err
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

func RebuildPatriciaTrieBasedOnFiles(rwTx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	useExternalTx := rwTx != nil
	if !useExternalTx {
		var err error
		rwTx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return trie.EmptyRoot, err
		}
		defer rwTx.Rollback()
	}
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.blockReader))
	var foundHash bool
	toTxNum := rwTx.(*temporal.Tx).AggTx().(*state.AggregatorRoTx).EndTxNumNoCommitment()
	ok, blockNum, err := txNumsReader.FindBlockNum(rwTx, toTxNum)
	if err != nil {
		return libcommon.Hash{}, err
	}
	if !ok {
		bb, err := countBlockByTxnum(ctx, rwTx, cfg.blockReader, toTxNum)
		if err != nil {
			return libcommon.Hash{}, err
		}
		blockNum = bb.Number
		foundHash = bb.Offset() != 0
	} else {
		firstTxInBlock, err := txNumsReader.Min(rwTx, blockNum)
		if err != nil {
			return libcommon.Hash{}, fmt.Errorf("failed to find first txNum in block %d : %w", blockNum, err)
		}
		lastTxInBlock, err := txNumsReader.Max(rwTx, blockNum)
		if err != nil {
			return libcommon.Hash{}, fmt.Errorf("failed to find last txNum in block %d : %w", blockNum, err)
		}
		if firstTxInBlock == toTxNum || lastTxInBlock == toTxNum {
			foundHash = true // state is in the beginning or end of block
		}
	}

	var expectedRootHash libcommon.Hash
	var headerHash libcommon.Hash
	var syncHeadHeader *types.Header
	if foundHash && cfg.checkRoot {
		syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, rwTx, blockNum)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", blockNum)
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
	}

	rh, err := collectAndComputeCommitment(ctx, rwTx, cfg.tmpDir, toTxNum)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if foundHash && cfg.checkRoot && !bytes.Equal(rh, expectedRootHash[:]) {
		logger.Error(fmt.Sprintf("[RebuildCommitment] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", blockNum, rh, expectedRootHash, headerHash))
		rwTx.Rollback()

		return trie.EmptyRoot, errors.New("wrong trie root")
	}
	logger.Info(fmt.Sprintf("[RebuildCommitment] Trie root of block %d txNum %d: %x. Could not verify with block hash because txnum of state is in the middle of the block.", blockNum, toTxNum, rh))

	if !useExternalTx {
		if err := rwTx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}
	return libcommon.BytesToHash(rh), err
}
