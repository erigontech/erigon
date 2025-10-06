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
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/eth1/eth1_chain_reader"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/turbo/services"
)

const (
	forkchoiceTimeoutMillis = 5000
)

type Status int

const (
	Idle Status = iota
	Syncing
	Synced
)

// EngineBlockDownloader is responsible to download blocks in reverse, and then insert them in the database.
type EngineBlockDownloader struct {
	backgroundCtx   context.Context
	status          atomic.Value // current Status of the downloading process, aka: is it doing anything
	blockReader     services.FullBlockReader
	db              kv.RoDB
	chainRW         eth1_chain_reader.ChainReaderWriterEth1
	syncCfg         ethconfig.Sync
	lock            sync.Mutex
	logger          log.Logger
	bbd             *p2p.BackwardBlockDownloader
	badHeaders      *lru.Cache[common.Hash, common.Hash]
	messageListener *p2p.MessageListener
	peerTracker     *p2p.PeerTracker
	stopped         atomic.Bool
}

func NewEngineBlockDownloader(
	ctx context.Context,
	logger log.Logger,
	executionClient executionproto.ExecutionClient,
	blockReader services.FullBlockReader,
	db kv.RoDB,
	config *chain.Config,
	tmpdir string,
	syncCfg ethconfig.Sync,
	sentryClient sentryproto.SentryClient,
	statusDataProvider *sentry.StatusDataProvider,
) *EngineBlockDownloader {
	var s atomic.Value
	s.Store(Idle)
	peerPenalizer := p2p.NewPeerPenalizer(sentryClient)
	messageListener := p2p.NewMessageListener(logger, sentryClient, statusDataProvider.GetStatusData, peerPenalizer)
	messageSender := p2p.NewMessageSender(sentryClient)
	peerTracker := p2p.NewPeerTracker(logger, messageListener)
	var fetcher p2p.Fetcher
	fetcher = p2p.NewFetcher(logger, messageListener, messageSender)
	fetcher = p2p.NewPenalizingFetcher(logger, fetcher, peerPenalizer)
	fetcher = p2p.NewTrackingFetcher(fetcher, peerTracker)
	bbd := p2p.NewBackwardBlockDownloader(logger, fetcher, peerPenalizer, peerTracker, tmpdir)
	badHeaders, err := lru.New[common.Hash, common.Hash](1_000_000) // 64mb
	if err != nil {
		panic(fmt.Errorf("failed to create badHeaders cache: %w", err))
	}
	return &EngineBlockDownloader{
		backgroundCtx:   ctx,
		db:              db,
		status:          s,
		syncCfg:         syncCfg,
		logger:          logger,
		blockReader:     blockReader,
		chainRW:         eth1_chain_reader.NewChainReaderEth1(config, executionClient, forkchoiceTimeoutMillis),
		bbd:             bbd,
		badHeaders:      badHeaders,
		messageListener: messageListener,
		peerTracker:     peerTracker,
	}
}

func (e *EngineBlockDownloader) Run(ctx context.Context) error {
	e.logger.Info("[EngineBlockDownloader] running")
	defer func() {
		e.logger.Info("[EngineBlockDownloader] stopped")
		e.stopped.Store(true)
	}()
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := e.peerTracker.Run(ctx)
		if err != nil {
			return fmt.Errorf("engine block downloader peer tracker failed: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := e.messageListener.Run(ctx)
		if err != nil {
			return fmt.Errorf("engine block downloader message listener failed: %w", err)
		}
		return nil
	})
	return eg.Wait()
}

func (e *EngineBlockDownloader) ReportBadHeader(badHeader, lastValidAncestor common.Hash) {
	e.badHeaders.Add(badHeader, lastValidAncestor)
}

func (e *EngineBlockDownloader) IsBadHeader(h common.Hash) (bad bool, lastValidAncestor common.Hash) {
	lastValidAncestor, bad = e.badHeaders.Get(h)
	return bad, lastValidAncestor
}

func (e *EngineBlockDownloader) Status() Status {
	return e.status.Load().(Status)
}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
// chainTip is optional and should be the block tip of the download request, which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) StartDownloading(hashToDownload common.Hash, chainTip *types.Block, trigger Trigger) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.status.Load() == Syncing {
		return false
	}
	e.status.Store(Syncing)
	go e.download(e.backgroundCtx, BackwardDownloadRequest{
		MissingHash:      hashToDownload,
		Trigger:          trigger,
		ValidateChainTip: chainTip,
	})
	return true
}

// download is the process that reverse download a specific block hash.
// chainTip is optional and should be the block tip of the download request, which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) download(ctx context.Context, req BackwardDownloadRequest) {
	err := e.processReq(ctx, req)
	if err != nil {
		args := append(req.LogArgs(), "err", err)
		e.logger.Warn("[EngineBlockDownloader] could not process backward download request", args...)
		e.status.Store(Idle)
		return
	}
	e.logger.Info("[EngineBlockDownloader] backward download request successfully processed", req.LogArgs()...)
	e.status.Store(Synced)
}

func (e *EngineBlockDownloader) processReq(ctx context.Context, req BackwardDownloadRequest) error {
	err := e.downloadBlocks(ctx, req)
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
	if status == executionproto.ExecutionStatus_TooFarAway || status == executionproto.ExecutionStatus_Busy {
		e.logger.Info("[EngineBlockDownloader] block verification skipped")
		return nil
	}
	if status == executionproto.ExecutionStatus_BadBlock {
		e.ReportBadHeader(tip.Hash(), latestValidHash)
		return errors.New("block segments downloaded are invalid")
	}
	e.logger.Info("[EngineBlockDownloader] blocks verification successful")
	return nil
}

func (e *EngineBlockDownloader) downloadBlocks(ctx context.Context, req BackwardDownloadRequest) error {
	e.logger.Info("[EngineBlockDownloader] processing backward download of blocks", req.LogArgs()...)
	if e.stopped.Load() {
		return errors.New("engine block downloader is stopped")
	}
	blocksBatchSize := min(500, uint64(e.syncCfg.LoopBlockLimit))
	opts := []p2p.BbdOption{p2p.WithBlocksBatchSize(blocksBatchSize)}
	if req.Trigger == NewPayloadTrigger {
		opts = append(opts, p2p.WithChainLengthLimit(e.syncCfg.MaxReorgDepth))
		currentHeader := e.chainRW.CurrentHeader(ctx)
		if currentHeader != nil {
			opts = append(opts, p2p.WithChainLengthCurrentHead(currentHeader.Number.Uint64()))
		}
	}
	if req.Trigger == SegmentRecoveryTrigger {
		opts = append(opts, p2p.WithChainLengthLimit(uint64(e.syncCfg.LoopBlockLimit)))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // need to cancel the ctx so that we cancel the download request processing if we err out prematurely
	hr := headerReader{db: e.db, blockReader: e.blockReader}
	feed, err := e.bbd.DownloadBlocksBackwards(ctx, req.MissingHash, hr, opts...)
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
	case executionproto.ExecutionStatus_BadBlock:
		e.ReportBadHeader(block.Hash(), lastValidHash)
		e.ReportBadHeader(requested, lastValidHash)
		return fmt.Errorf("bad block when validating batch download: tip=%s, latestValidHash=%s", block.Hash(), lastValidHash)
	case executionproto.ExecutionStatus_TooFarAway:
		e.logger.Debug(
			"[EngineBlockDownloader] skipping validation of block batch download due to exec status too far away",
			"tip", block.Hash(),
			"latestValidHash", lastValidHash,
		)
	case executionproto.ExecutionStatus_Success: // proceed to UpdateForkChoice
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
	if fcuStatus != executionproto.ExecutionStatus_Success {
		return fmt.Errorf(
			"unsuccessful status when updating fork choice for batch download: status=%s, tip=%s, latestValidHash=%s",
			fcuStatus,
			block.Hash(),
			lastValidHash,
		)
	}
	return nil
}
