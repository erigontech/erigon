package engine_block_downloader

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
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

			if headersStatus != headerdownload.Synced {
				// Could not sync. Set to idle
				e.status.Store(headerdownload.Idle)
				e.logger.Warn("[EngineBlockDownloader] Header download did not yield success")
				continue
			}
			e.hd.SetPosStatus(headerdownload.Idle)

			tx, err := e.db.BeginRo(e.ctx)
			if err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could not begin tx: %s", err)
				e.status.Store(headerdownload.Idle)
				continue
			}

			tmpDb, err := mdbx.NewTemporaryMdbx()
			if err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
				e.status.Store(headerdownload.Idle)
				continue
			}
			defer tmpDb.Close()
			tmpTx, err := tmpDb.BeginRw(e.ctx)
			if err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could create temporary mdbx", "err", err)
				e.status.Store(headerdownload.Idle)
				continue
			}
			defer tmpTx.Rollback()

			memoryMutation := memdb.NewMemoryBatchWithCustomDB(tx, tmpDb, tmpTx, e.tmpdir)
			defer memoryMutation.Rollback()

			startBlock, endBlock, startHash, err := e.loadDownloadedHeaders(memoryMutation)
			if err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could load headers", "err", err)
				e.status.Store(headerdownload.Idle)
				continue
			}

			// bodiesCollector := etl.NewCollector("EngineBlockDownloader", e.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), e.logger)
			if err := e.downloadAndLoadBodiesSyncronously(memoryMutation, startBlock, endBlock); err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could not download bodies", "err", err)
				e.status.Store(headerdownload.Idle)
				continue
			}
			tx.Rollback() // Discard the original db tx
			if err := e.insertHeadersAndBodies(tmpTx, startBlock, startHash); err != nil {
				e.logger.Warn("[EngineBlockDownloader] Could not insert headers and bodies", "err", err)
				e.status.Store(headerdownload.Idle)
				continue
			}
			e.status.Store(headerdownload.Synced)
			e.logger.Info("[EngineBlockDownloader] Finished downloading blocks", "from", startBlock-1, "to", endBlock)
		case <-e.ctx.Done():
			return
		}

	}
}

// StartDownloading triggers the download process and returns true if the process started or false if it could not.
func (e *EngineBlockDownloader) StartDownloading(requestId int, hashToDownload libcommon.Hash, downloaderTip libcommon.Hash) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.status.Load() == headerdownload.Syncing {
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
	return headerdownload.SyncStatus(e.status.Load().(int))
}
