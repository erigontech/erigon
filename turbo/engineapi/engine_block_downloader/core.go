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

package engine_block_downloader

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/membatchwithdb"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/erigon-db/rawdb"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

// download is the process that reverse download a specific block hash.
func (e *EngineBlockDownloader) download(ctx context.Context, hashToDownload common.Hash, requestId int, block *types.Block) {
	/* Start download process*/
	// First we schedule the headers download process
	if !e.scheduleHeadersDownload(requestId, hashToDownload, block.NumberU64()) {
		e.logger.Warn("[EngineBlockDownloader] could not begin header download")
		// could it be scheduled? if not nevermind.
		// e.status.Store(headerdownload.Idle)
		return
	}
	// see the outcome of header download
	headersStatus, err := e.waitForEndOfHeadersDownload(ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not finish headers download", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}

	if headersStatus != headerdownload.Synced {
		// Could not sync. Set to idle
		e.logger.Warn("[EngineBlockDownloader] Header download did not yield success")
		e.status.Store(headerdownload.Idle)
		return
	}
	e.hd.SetPosStatus(headerdownload.Idle)

	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not begin tx", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	defer tx.Rollback()

	tmpDb, err := mdbx.NewTemporaryMdbx(ctx, e.tmpdir)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	defer tmpDb.Close()
	tmpTx, err := tmpDb.BeginRw(ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	defer tmpTx.Rollback()

	memoryMutation := membatchwithdb.NewMemoryBatchWithCustomDB(tx, tmpDb, tmpTx)
	defer memoryMutation.Rollback()

	if block != nil {
		err = rawdb.WriteCanonicalHash(memoryMutation, block.Hash(), block.NumberU64())
		if err != nil {
			e.logger.Warn("[EngineBlockDownloader] Could not make leading header canonical", "err", err)
			e.status.Store(headerdownload.Idle)
			return
		}
	}
	_, endBlock, err := e.loadDownloadedHeaders(memoryMutation)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not load headers", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}

	if err := stages.SaveStageProgress(memoryMutation, stages.Headers, endBlock); err != nil {
		e.logger.Error("[EngineBlockDownloader] Failed to save headers progress", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	headHash, _, _, err := e.chainRW.GetForkChoice(ctx)
	if err != nil {
		panic(err)
	}
	latestHeader, err := e.blockReader.HeaderByHash(ctx, memoryMutation, headHash)
	if err != nil {
		panic(err)
	}
	latestBlockNr := uint64(0)
	if latestHeader != nil {
		latestBlockNr = latestHeader.Number.Uint64()
	}
	e.logger.Info("[EngineBlockDownloader] currentForkChoice:  ", "latestBlockNr", latestBlockNr)
	targetBlockNr := endBlock
	increment := uint64(2_000)
	currentBlockNr := latestBlockNr // start from latest fork choice
	for currentBlockNr < targetBlockNr {
		if currentBlockNr+increment >= targetBlockNr { // adjust increment to remainder towards the end
			increment = targetBlockNr - currentBlockNr
		}
		err = e.downloadAndExecLoopIteration(ctx, memoryMutation, currentBlockNr, currentBlockNr+increment)
		if err != nil {
			e.logger.Error("[EngineBlockDownloader] failed to execute blocks ", "startBlock", currentBlockNr, "endBlock", currentBlockNr+increment, "err", err)
			e.status.Store(headerdownload.Idle)
			return
		}
		currentBlockNr += increment

	}
	// Can fail, not an issue in this case.
	e.chainRW.InsertBlockAndWait(ctx, block)
	e.logger.Info("[EngineBlockDownloader] Sync completed")
	e.status.Store(headerdownload.Synced)

}

func (e *EngineBlockDownloader) downloadAndExecLoopIteration(ctx context.Context, memoryMutation kv.RwTx, startBlockNr, endBlockNr uint64) error {
	e.logger.Info("[EngineBlockDownloader] LOOP ITERATION downloading and executing ", "startBlock", startBlockNr, "endBlock", endBlockNr)

	// if latestBlockNr > startBlockNr {
	// 	e.logger.Info("[EngineBlockDownloader] Early exit: not downloading bodies or executing because startBlockNr < latestBlockNr ", "startBlockNr", startBlockNr, "latestBlockNr", latestBlockNr)
	// 	return nil
	// }

	// if latestBlockNr > endBlockNr { // this should also not happen
	// 	e.logger.Error("[EngineBlockDownloader] This should not happen ", "latestBlockNr", latestBlockNr, "endBlockNr", endBlockNr)
	// 	return errors.New("latestBlockNr>endBlockNr")
	// }

	if err := e.downloadAndLoadBodiesSyncronously(ctx, memoryMutation, uint64(startBlockNr), uint64(endBlockNr)); err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not download bodies", "err", err)
		return err
	}
	e.logger.Info("[ITER] Before UpdateFork", "startBlock", startBlockNr, "endBlock", endBlockNr)
	endBlock, err := rawdb.ReadHeadersByNumber(memoryMutation, uint64(endBlockNr))
	if err != nil {
		panic(err)
	}
	var endBlockHash common.Hash
	if len(endBlock) == 0 {
		roTx, err := e.db.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer roTx.Rollback()
		endBlockFromReader, err := e.blockReader.HeaderByNumber(ctx, roTx, endBlockNr) // look into snapshots
		if err != nil {
			return err
		}
		if endBlockFromReader == nil {
			panic("endBlock = nil")
		}
		endBlockHash = endBlockFromReader.Hash()
	} else {
		endBlockHash = endBlock[0].Hash()
	}
	// Can fail, not an issue in this case.
	// e.chainRW.InsertBlockAndWait(ctx, )
	status, _, latestValidHash, err := e.chainRW.UpdateForkChoice(ctx, endBlockHash, endBlockHash, endBlockHash)
	e.logger.Info("[ITER] After UpdateFork", "endBlockNr", endBlockNr)

	if err != nil {
		return err
	}

	if status == execution.ExecutionStatus_TooFarAway || status == execution.ExecutionStatus_Busy {
		e.logger.Info("[EngineBlockDownloader] updateForkChoice skipped")
		return fmt.Errorf("chainRW.UpdateForkChoice() failed. Execution status=%v", status)
	}
	if status == execution.ExecutionStatus_BadBlock {
		e.logger.Warn("[EngineBlockDownloader] block segments downloaded are invalid")
		e.hd.ReportBadHeaderPoS(endBlockHash, latestValidHash)
		return err
	}

	e.logger.Info("[EngineBlockDownloader] fork choice update successful")
	e.status.Store(headerdownload.Idle)
	return nil
}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
// blockTip is optional and should be the block tip of the download request. which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) StartDownloading(ctx context.Context, requestId int, hashToDownload common.Hash, blockTip *types.Block) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.status.Load() == headerdownload.Syncing {
		return false
	}
	e.status.Store(headerdownload.Syncing)
	go e.download(e.bacgroundCtx, hashToDownload, requestId, blockTip)
	return true
}

func (e *EngineBlockDownloader) Status() headerdownload.SyncStatus {
	return headerdownload.SyncStatus(e.status.Load().(int))
}
