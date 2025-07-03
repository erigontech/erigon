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

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/membatchwithdb"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/polygon/p2p"
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
		e.downloadV2(ctx, hashToDownload, trigger)
		return
	}
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

	tmpDb, err := mdbx.NewUnboundedTemporaryMdbx(ctx, e.tmpdir)
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

	if chainTip != nil {
		err = rawdb.WriteCanonicalHash(memoryMutation, chainTip.Hash(), chainTip.NumberU64())
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

	// bodiesCollector := etl.NewCollector("EngineBlockDownloader", e.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), e.logger)
	if err := e.downloadAndLoadBodiesSyncronously(ctx, memoryMutation, startBlock, endBlock); err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not download bodies", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	tx.Rollback() // Discard the original db tx
	e.logger.Info("[EngineBlockDownloader] Finished downloading blocks", "from", startBlock-1, "to", endBlock)
	if chainTip == nil {
		e.status.Store(headerdownload.Idle)
		return
	}
	// Can fail, not an issue in this case.
	e.chainRW.InsertBlockAndWait(ctx, chainTip)
	// Lastly attempt verification
	status, _, latestValidHash, err := e.chainRW.ValidateChain(ctx, chainTip.Hash(), chainTip.NumberU64())
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
		e.hd.ReportBadHeaderPoS(chainTip.Hash(), latestValidHash)
		return
	}
	e.logger.Info("[EngineBlockDownloader] blocks verification successful")
	e.status.Store(headerdownload.Synced)

}

func (e *EngineBlockDownloader) downloadV2(ctx context.Context, req BackwardDownloadRequest) {
	err := e.processDownloadV2(ctx, req)
	if err != nil {
		args := []interface{}{"hash", req.MissingHash, "trigger", req.Trigger}
		if req.ValidateChainTip != nil {
			args = append(args, "chainTip", req.ValidateChainTip)
		}
		args = append(args, "err", err)
		e.logger.Warn("[EngineBlockDownloader] could not process backward download request", args)
		e.status.Store(headerdownload.Idle)
		return
	}
}

func (e *EngineBlockDownloader) processDownloadV2(ctx context.Context, req BackwardDownloadRequest) error {
	// 1. Get all peers
	peers := e.p2pGateway.ListPeers()

	// 2. Check which peers have the header and terminate if none have seen it
	type headerKey struct {
		hash   common.Hash
		height uint64
	}
	type headerAvailability struct {
		from headerKey
		to   headerKey
	}

	hash := req.MissingHash
	peerAvailability := make(map[p2p.PeerId]*headerAvailability, len(peers))
	var eg errgroup.Group
	for _, peer := range peers {
		eg.Go(func() error {
			resp, err := e.p2pGateway.FetchHeadersBackwards(ctx, hash, 1, peer)
			if err != nil {
				e.logger.Trace("[EngineBlockDownloader] peer does not have header", "peer", peer, "hash", hash, "err", err)
				return nil
			}

			key := headerKey{hash: hash, height: resp.Data[0].Number.Uint64()}
			peerAvailability[*peer] = &headerAvailability{from: key, to: key}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		panic(err) // err not expected since we don't return any errors in the above goroutines
	}
	if len(peerAvailability) == 0 {
		return fmt.Errorf("no peers have block hash for backward download: %s", hash)
	}

	// 3. Download the header chain backwards in an etl collector until we connect it to a known local header.
	//    Note we do this for all peers to so that we can build an availability map that can be later on
	//    used for downloading bodies (i.e., to know which peers may have our bodies).

	//
	// TODO
	//

	return nil
}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
// chainTip is optional and should be the block tip of the download request, which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) StartDownloading(requestId int, hashToDownload common.Hash, heightToDownload uint64, chainTip *types.Block, trigger Trigger) bool {
	if !e.status.CompareAndSwap(headerdownload.Idle, headerdownload.Syncing) {
		return false
	}
	go e.download(e.bacgroundCtx, hashToDownload, heightToDownload, requestId, chainTip, trigger)
	return true
}

func (e *EngineBlockDownloader) Status() headerdownload.SyncStatus {
	return headerdownload.SyncStatus(e.status.Load().(int))
}
