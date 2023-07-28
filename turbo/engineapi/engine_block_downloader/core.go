package engine_block_downloader

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

type downloadRequest struct {
	hashToDownload libcommon.Hash
	downloaderTip  libcommon.Hash
	requestId      int
}

func (e *EngineBlockDownloader) Loop() {
	for {
		select {
		case req := <-e.startDownloadCh:
			/* Start download process*/
			// First we schedule the headers download process
			if !e.scheduleHeadersDownload(req.requestId, req.hashToDownload, 0, req.downloaderTip) {
				e.logger.Warn("[EngineBlockDownloader] could not begin header download")
				// could it be scheduled? if not nevermind.
				e.status.Store(headerdownload.Idle)
				continue
			}
			// see the outcome of header download
			headersStatus := e.waitForEndOfHeadersDownload()
			if headersStatus != engine_helpers.Synced {
				// Could not sync. Set to idle
				e.status.Store(headerdownload.Idle)
				e.logger.Warn("[EngineBlockDownloader] Header download did not yield success")
				continue
			}
			tx, err := e.db.BeginRo(e.ctx)
			if err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could not begin tx: %s", err)
				e.status.Store(headerdownload.Idle)
				continue
			}

			memoryMutation := memdb.NewMemoryBatch(tx, e.tmpdir)
			defer memoryMutation.Rollback()

			startBlock, endBlock, startHash, err := e.loadDownloadedHeaders(memoryMutation)
			if err != nil {
				e.status.Store(headerdownload.Idle)
				continue
			}
			if err := e.downloadAndLoadBodiesSyncronously(memoryMutation, startBlock, endBlock); err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could not download bodies: %s", err)
				e.status.Store(headerdownload.Idle)
				continue
			}
			tx.Rollback() // Discard the original db tx
			if err := e.insertHeadersAndBodies(memoryMutation.MemTx(), startBlock, startHash); err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could not insert headers and bodies: %s", err)
				e.status.Store(headerdownload.Idle)
				continue
			}
			e.status.Store(headerdownload.Idle)
		case <-e.ctx.Done():
			return
		}

	}
}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
func (e *EngineBlockDownloader) StartDownloading(requestId int, hashToDownload libcommon.Hash, downloaderTip libcommon.Hash) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.status.Load() != headerdownload.Idle {
		return false
	}
	e.status.Store(headerdownload.Syncing)
	e.startDownloadCh <- downloadRequest{
		requestId:      requestId,
		hashToDownload: hashToDownload,
		downloaderTip:  downloaderTip,
	}
	return true
}

func (e *EngineBlockDownloader) Status() headerdownload.SyncStatus {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.status.Load().(headerdownload.SyncStatus)
}
