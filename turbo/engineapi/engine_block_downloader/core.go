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

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/membatchwithdb"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"
)

// download is the process that reverse download a specific block hash.
// chainTip is optional and should be the block tip of the download request, which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) download(
	ctx context.Context,
	hashToDownload common.Hash,
	heightToDownload uint64,
	requestId int,
	newPayloadBlock *types.Block,
) {
	forkChoiceUpdate := newPayloadBlock == nil // means we are asked to download blocks backwards by a fcu
	/* Start download process*/
	// First we schedule the headers download process
	if !e.scheduleHeadersDownload(requestId, hashToDownload, heightToDownload) {
		e.logger.Warn("[EngineBlockDownloader] could not begin header download")
		// could it be scheduled? if not nevermind.
		e.status.Store(headerdownload.Idle)
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

	if !forkChoiceUpdate {
		err = rawdb.WriteCanonicalHash(memoryMutation, newPayloadBlock.Hash(), newPayloadBlock.NumberU64())
		if err != nil {
			e.logger.Warn("[EngineBlockDownloader] Could not make leading header canonical", "err", err)
			e.status.Store(headerdownload.Idle)
			return
		}
	}
	startBlock, endBlock, err := e.loadDownloadedHeaders(memoryMutation)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not load headers", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}

	currentHeader := e.chainRW.CurrentHeader(ctx)
	if currentHeader == nil {
		e.logger.Warn("[EngineBlockDownloader] Could not load current header", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}

	currentHeaderNum := currentHeader.Number.Uint64()
	if startBlock+1000 > currentHeaderNum {
		e.logger.Warn(
			"[EngineBlockDownloader] Can not download sidechain deeper back than allowed unwind distance",
			"startBlock", startBlock,
			"currentHeaderNum", currentHeaderNum,
		)
		e.status.Store(headerdownload.Idle)
		return
	}

	if !forkChoiceUpdate && endBlock-startBlock > uint64(e.syncCfg.LoopBlockLimit) {
		e.logger.Warn(
			"[EngineBlockDownloader] Can not download new payload backward chain longer than sync loop block limit - waiting for fork choice update",
			"startBlock", startBlock,
			"endBlock", endBlock,
			"currentHeaderNum", currentHeaderNum,
		)
		e.status.Store(headerdownload.Idle)
		return
	}

	// bodiesCollector := etl.NewCollector("EngineBlockDownloader", e.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), e.logger)
	lastValidAncestor, err := e.downloadAndLoadBodiesSyncronously(ctx, memoryMutation, startBlock, endBlock, forkChoiceUpdate)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not download bodies", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}

	tx.Rollback() // Discard the original db tx
	e.logger.Info("[EngineBlockDownloader] Finished downloading blocks", "from", startBlock-1, "to", endBlock)

	if forkChoiceUpdate {
		// means we're called within a fork choice update!
		head := e.chainRW.GetBlockByHash(ctx, hashToDownload)
		if head == nil {
			e.logger.Warn("[EngineBlockDownloader] Could not read hash head after block download during a fcu", "hash", hashToDownload)
			e.status.Store(headerdownload.Idle)
			return
		}

		err := e.forkChoiceUpdate(ctx, head.Hash(), head.ParentHash(), lastValidAncestor)
		if err != nil {
			e.logger.Warn("[EngineBlockDownloader] fork choice update failed", "err", err)
			e.status.Store(headerdownload.Idle)
			return
		}
	}

	// Can fail, not an issue in this case.
	e.chainRW.InsertBlockAndWait(ctx, newPayloadBlock)
	// Lastly attempt verification
	status, _, latestValidHash, err := e.chainRW.ValidateChain(ctx, newPayloadBlock.Hash(), newPayloadBlock.NumberU64())
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] block verification failed", "reason", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	if status == execution.ExecutionStatus_TooFarAway || status == execution.ExecutionStatus_Busy {
		e.logger.Info("[EngineBlockDownloader] block verification skipped")
		e.status.Store(headerdownload.Synced)
		return
	}
	if status == execution.ExecutionStatus_BadBlock {
		e.logger.Warn("[EngineBlockDownloader] block segments downloaded are invalid")
		e.status.Store(headerdownload.Idle)
		e.hd.ReportBadHeaderPoS(newPayloadBlock.Hash(), latestValidHash)
		return
	}
	e.logger.Info("[EngineBlockDownloader] blocks verification successful")
	e.status.Store(headerdownload.Synced)

}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
// chainTip is optional and should be the block tip of the download request, which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) StartDownloading(requestId int, hashToDownload common.Hash, heightToDownload uint64, chainTip *types.Block) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.status.Load() == headerdownload.Syncing {
		return false
	}
	e.status.Store(headerdownload.Syncing)
	go e.download(e.bacgroundCtx, hashToDownload, heightToDownload, requestId, chainTip)
	return true
}

func (e *EngineBlockDownloader) Status() headerdownload.SyncStatus {
	return headerdownload.SyncStatus(e.status.Load().(int))
}
