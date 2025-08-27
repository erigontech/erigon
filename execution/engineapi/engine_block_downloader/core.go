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
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/bbd"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/types"
)

// download is the process that reverse download a specific block hash.
// chainTip is optional and should be the block tip of the download request, which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) download(
	ctx context.Context,
	hashToDownload common.Hash,
	heightToDownload uint64,
	requestId int,
	chainTip *types.Block,
	trigger Trigger,
) {
	if e.v2 {
		req := BackwardDownloadRequest{MissingHash: hashToDownload, Trigger: trigger, ValidateChainTip: chainTip}
		err := e.downloadV2(ctx, req)
		if err != nil {
			args := append(req.LogArgs(), "err", err)
			e.logger.Warn("[EngineBlockDownloader] could not process backward download request", args...)
			e.status.Store(Idle)
			return
		}
		e.logger.Info("[EngineBlockDownloader] backward download request successfully processed", req.LogArgs()...)
		e.status.Store(Synced)
		return
	}
	/* Start download process*/
	// First we schedule the headers download process
	if !e.scheduleHeadersDownload(requestId, hashToDownload, heightToDownload) {
		e.logger.Warn("[EngineBlockDownloader] could not begin header download")
		// could it be scheduled? if not nevermind.
		e.status.Store(Idle)
		return
	}
	// see the outcome of header download
	headersStatus, err := e.waitForEndOfHeadersDownload(ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not finish headers download", "err", err)
		e.status.Store(Idle)
		return
	}

	if headersStatus != headerdownload.Synced {
		// Could not sync. Set to idle
		e.logger.Warn("[EngineBlockDownloader] Header download did not yield success")
		e.status.Store(Idle)
		return
	}
	e.hd.SetPosStatus(headerdownload.Idle)

	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not begin tx", "err", err)
		e.status.Store(Idle)
		return
	}
	defer tx.Rollback()

	tmpDb, err := mdbx.NewUnboundedTemporaryMdbx(ctx, e.tmpdir)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
		e.status.Store(Idle)
		return
	}
	defer tmpDb.Close()
	tmpTx, err := tmpDb.BeginRw(ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
		e.status.Store(Idle)
		return
	}
	defer tmpTx.Rollback()

	memoryMutation := membatchwithdb.NewMemoryBatchWithCustomDB(tx, tmpDb, tmpTx)
	defer memoryMutation.Rollback()

	if chainTip != nil {
		err = rawdb.WriteCanonicalHash(memoryMutation, chainTip.Hash(), chainTip.NumberU64())
		if err != nil {
			e.logger.Warn("[EngineBlockDownloader] Could not make leading header canonical", "err", err)
			e.status.Store(Idle)
			return
		}
	}
	startBlock, endBlock, err := e.loadDownloadedHeaders(memoryMutation)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not load headers", "err", err)
		e.status.Store(Idle)
		return
	}

	// bodiesCollector := etl.NewCollector("EngineBlockDownloader", e.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), e.logger)
	if err := e.downloadAndLoadBodiesSyncronously(ctx, memoryMutation, startBlock, endBlock); err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not download bodies", "err", err)
		e.status.Store(Idle)
		return
	}
	tx.Rollback() // Discard the original db tx
	e.logger.Info("[EngineBlockDownloader] Finished downloading blocks", "from", startBlock-1, "to", endBlock)
	if chainTip == nil {
		e.status.Store(Synced)
		return
	}
	// Can fail, not an issue in this case.
	e.chainRW.InsertBlockAndWait(ctx, chainTip)
	// Lastly attempt verification
	status, _, latestValidHash, err := e.chainRW.ValidateChain(ctx, chainTip.Hash(), chainTip.NumberU64())
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] block verification failed", "reason", err)
		e.status.Store(Idle)
		return
	}
	if status == execution.ExecutionStatus_TooFarAway || status == execution.ExecutionStatus_Busy {
		e.logger.Info("[EngineBlockDownloader] block verification skipped")
		e.status.Store(Idle)
		return
	}
	if status == execution.ExecutionStatus_BadBlock {
		e.logger.Warn("[EngineBlockDownloader] block segments downloaded are invalid")
		e.hd.ReportBadHeaderPoS(chainTip.Hash(), latestValidHash)
		e.status.Store(Idle)
		return
	}
	e.logger.Info("[EngineBlockDownloader] blocks verification successful")
	e.status.Store(Synced)
}

func (e *EngineBlockDownloader) downloadV2(ctx context.Context, req BackwardDownloadRequest) error {
	err := e.downloadBlocksV2(ctx, req)
	if err != nil {
		return fmt.Errorf("could not process backward download of blocks: %w", err)
	}
	e.logger.Info("[EngineBlockDownloader] backward download of blocks finished successfully", req.LogArgs()...)
	if req.ValidateChainTip == nil {
		return nil
	}
	tip := req.ValidateChainTip
	err = e.chainRW.InsertBlockAndWait(ctx, tip)
	if err != nil {
		return fmt.Errorf("could not insert request chain tip for validation: %w", err)
	}
	status, _, latestValidHash, err := e.chainRW.ValidateChain(ctx, tip.Hash(), tip.NumberU64())
	if err != nil {
		return fmt.Errorf("request chain tip validation failed: %w", err)
	}
	if status == execution.ExecutionStatus_TooFarAway || status == execution.ExecutionStatus_Busy {
		e.logger.Info("[EngineBlockDownloader] block verification skipped")
		return nil
	}
	if status == execution.ExecutionStatus_BadBlock {
		e.hd.ReportBadHeaderPoS(tip.Hash(), latestValidHash)
		return errors.New("block segments downloaded are invalid")
	}
	e.logger.Info("[EngineBlockDownloader] blocks verification successful")
	return nil
}

func (e *EngineBlockDownloader) downloadBlocksV2(ctx context.Context, req BackwardDownloadRequest) error {
	e.logger.Info("[EngineBlockDownloader] processing backward download of blocks", req.LogArgs()...)
	blocksBatchSize := min(500, uint64(e.syncCfg.LoopBlockLimit))
	opts := []bbd.Option{bbd.WithBlocksBatchSize(blocksBatchSize)}
	if req.Trigger == NewPayloadTrigger {
		opts = append(opts, bbd.WithChainLengthLimit(uint64(dbg.MaxReorgDepth)))
		currentHeader := e.chainRW.CurrentHeader(ctx)
		if currentHeader != nil {
			opts = append(opts, bbd.WithChainLengthCurrentHead(currentHeader.Number.Uint64()))
		}
	}
	if req.Trigger == SegmentRecoveryTrigger {
		opts = append(opts, bbd.WithChainLengthLimit(uint64(e.syncCfg.LoopBlockLimit)))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // need to cancel the ctx so that we cancel the download request processing if we err out prematurely
	feed, err := e.bbdV2.DownloadBlocksBackwards(ctx, req.MissingHash, opts...)
	if err != nil {
		return err
	}

	logProgressTicker := time.NewTicker(30 * time.Second)
	defer logProgressTicker.Stop()

	var blocks []*types.Block
	var insertedBlocksWithoutExec int
	for blocks, err = feed.Next(ctx); err == nil && len(blocks) > 0; blocks, err = feed.Next(ctx) {
		progressLogArgs := []interface{}{
			"from", blocks[0].NumberU64(),
			"fromHash", blocks[0].Hash(),
			"to", blocks[len(blocks)-1].NumberU64(),
			"toHash", blocks[len(blocks)-1].Hash(),
		}
		select {
		case <-logProgressTicker.C:
			e.logger.Info("[EngineBlockDownloader] processing downloaded blocks periodic progress", progressLogArgs...)
		default:
			e.logger.Trace("[EngineBlockDownloader] processing downloaded blocks", progressLogArgs...)
		}
		err := e.chainRW.InsertBlocksAndWait(ctx, blocks)
		if err != nil {
			return err
		}
		insertedBlocksWithoutExec += len(blocks)
		if req.Trigger == FcuTrigger && uint(insertedBlocksWithoutExec) >= e.syncCfg.LoopBlockLimit {
			tip := blocks[len(blocks)-1]
			e.logger.Info(
				"[EngineBlockDownloader] executing downloaded batch as it reached sync loop block limit",
				"to", tip.NumberU64(),
				"toHash", tip.Hash(),
			)
			err = e.execDownloadedBatch(ctx, tip, req.MissingHash)
			if err != nil {
				return err
			}
			insertedBlocksWithoutExec = 0
		}
	}
	return err
}

func (e *EngineBlockDownloader) execDownloadedBatch(ctx context.Context, block *types.Block, requested common.Hash) error {
	status, _, lastValidHash, err := e.chainRW.ValidateChain(ctx, block.Hash(), block.NumberU64())
	if err != nil {
		return err
	}
	switch status {
	case execution.ExecutionStatus_BadBlock:
		e.hd.ReportBadHeaderPoS(block.Hash(), lastValidHash)
		e.hd.ReportBadHeaderPoS(requested, lastValidHash)
		return fmt.Errorf("bad block when validating batch download: tip=%s, latestValidHash=%s", block.Hash(), lastValidHash)
	case execution.ExecutionStatus_TooFarAway:
		e.logger.Debug(
			"[EngineBlockDownloader] skipping validation of block batch download due to exec status too far away",
			"tip", block.Hash(),
			"latestValidHash", lastValidHash,
		)
	case execution.ExecutionStatus_Success: // proceed to UpdateForkChoice
	default:
		return fmt.Errorf(
			"unsuccessful status when validating batch download: status=%s, tip=%s, latestValidHash=%s",
			status,
			block.Hash(),
			lastValidHash,
		)
	}
	fcuStatus, _, lastValidHash, err := e.chainRW.UpdateForkChoice(ctx, block.Hash(), common.Hash{}, common.Hash{}, 0)
	if err != nil {
		return err
	}
	if fcuStatus != execution.ExecutionStatus_Success {
		return fmt.Errorf(
			"unsuccessful status when updating fork choice for batch download: status=%s, tip=%s, latestValidHash=%s",
			fcuStatus,
			block.Hash(),
			lastValidHash,
		)
	}
	return nil
}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
// chainTip is optional and should be the block tip of the download request, which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) StartDownloading(requestId int, hashToDownload common.Hash, heightToDownload uint64, chainTip *types.Block, trigger Trigger) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.status.Load() == Syncing {
		return false
	}
	e.status.Store(Syncing)
	go e.download(e.bacgroundCtx, hashToDownload, heightToDownload, requestId, chainTip, trigger)
	return true
}

func (e *EngineBlockDownloader) Status() Status {
	return e.status.Load().(Status)
}

type Status int

const (
	Idle Status = iota
	Syncing
	Synced
)
