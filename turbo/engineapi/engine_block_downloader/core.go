package engine_block_downloader

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

// download is the process that reverse download a specific block hash.
func (e *EngineBlockDownloader) download(hashToDownload libcommon.Hash, downloaderTip libcommon.Hash, requestId int, block *types.Block) {
	/* Start download process*/
	// First we schedule the headers download process
	if !e.scheduleHeadersDownload(requestId, hashToDownload, 0, downloaderTip) {
		e.logger.Warn("[EngineBlockDownloader] could not begin header download")
		// could it be scheduled? if not nevermind.
		e.status.Store(headerdownload.Idle)
		return
	}
	// see the outcome of header download
	headersStatus := e.waitForEndOfHeadersDownload()

	if headersStatus != headerdownload.Synced {
		// Could not sync. Set to idle
		e.logger.Warn("[EngineBlockDownloader] Header download did not yield success")
		e.status.Store(headerdownload.Idle)
		return
	}
	e.hd.SetPosStatus(headerdownload.Idle)

	tx, err := e.db.BeginRo(e.ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not begin tx: %s", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	defer tx.Rollback()

	tmpDb, err := mdbx.NewTemporaryMdbx(e.tmpdir)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	defer tmpDb.Close()
	tmpTx, err := tmpDb.BeginRw(e.ctx)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	defer tmpTx.Rollback()

	memoryMutation := memdb.NewMemoryBatchWithCustomDB(tx, tmpDb, tmpTx, e.tmpdir)
	defer memoryMutation.Rollback()

	startBlock, endBlock, startHash, err := e.loadDownloadedHeaders(memoryMutation)
	if err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could load headers", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}

	// bodiesCollector := etl.NewCollector("EngineBlockDownloader", e.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), e.logger)
	if err := e.downloadAndLoadBodiesSyncronously(memoryMutation, startBlock, endBlock); err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not download bodies", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	tx.Rollback() // Discard the original db tx
	if err := e.insertHeadersAndBodies(tmpTx, startBlock, startHash, endBlock); err != nil {
		e.logger.Warn("[EngineBlockDownloader] Could not insert headers and bodies", "err", err)
		e.status.Store(headerdownload.Idle)
		return
	}
	e.logger.Info("[EngineBlockDownloader] Finished downloading blocks", "from", startBlock-1, "to", endBlock)
	if block == nil {
		e.status.Store(headerdownload.Idle)
		return
	}
	// Can fail, not an issue in this case.
	e.chainRW.InsertBlockAndWait(block)
	// Lastly attempt verification
	status, latestValidHash, err := e.chainRW.ValidateChain(block.Hash(), block.NumberU64())
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
		e.hd.ReportBadHeaderPoS(block.Hash(), latestValidHash)
		return
	}
	e.logger.Info("[EngineBlockDownloader] blocks verification successful")
	e.status.Store(headerdownload.Synced)

}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
// blockTip is optional and should be the block tip of the download request. which will be inserted at the end of the procedure if specified.
func (e *EngineBlockDownloader) StartDownloading(requestId int, hashToDownload libcommon.Hash, downloaderTip libcommon.Hash, blockTip *types.Block) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.status.Load() == headerdownload.Syncing {
		return false
	}
	e.status.Store(headerdownload.Syncing)
	go e.download(hashToDownload, downloaderTip, requestId, blockTip)
	return true
}

func (e *EngineBlockDownloader) Status() headerdownload.SyncStatus {
	return headerdownload.SyncStatus(e.status.Load().(int))
}
