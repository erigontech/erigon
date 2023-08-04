package engine_block_downloader

import (
	"fmt"
	"runtime"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/dataflow"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/log/v3"
)

// downloadBodies executes bodies download.
func (e *EngineBlockDownloader) downloadAndLoadBodiesSyncronously(tx kv.RwTx, fromBlock, toBlock uint64) (err error) {
	headerProgress := toBlock
	bodyProgress := fromBlock - 1

	if err := stages.SaveStageProgress(tx, stages.Bodies, bodyProgress); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, headerProgress); err != nil {
		return err
	}

	var d1, d2, d3, d4, d5, d6 time.Duration

	timeout := e.timeout

	// This will update bd.maxProgress
	if _, _, _, _, err = e.bd.UpdateFromDb(tx); err != nil {
		return
	}
	defer e.bd.ClearBodyCache()

	if bodyProgress >= headerProgress {
		return nil
	}

	logPrefix := "EngineBlockDownloader"
	if headerProgress <= bodyProgress+16 {
		// When processing small number of blocks, we can afford wasting more bandwidth but get blocks quicker
		timeout = 1
	} else {
		// Do not print logs for short periods
		e.logger.Info(fmt.Sprintf("[%s] Processing bodies...", logPrefix), "from", bodyProgress, "to", headerProgress)
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	var prevDeliveredCount float64 = 0
	var prevWastedCount float64 = 0
	timer := time.NewTimer(1 * time.Second) // Check periodically even in the abseence of incoming messages
	var req *bodydownload.BodyRequest
	var peer [64]byte
	var sentToPeer bool
	stopped := false
	prevProgress := bodyProgress
	var noProgressCount uint = 0 // How many time the progress was printed without actual progress
	var totalDelivered uint64 = 0

	loopBody := func() (bool, error) {
		// loopCount is used here to ensure we don't get caught in a constant loop of making requests
		// having some time out so requesting again and cycling like that forever.  We'll cap it
		// and break the loop so we can see if there are any records to actually process further down
		// then come back here again in the next cycle
		for loopCount := 0; loopCount == 0 || (req != nil && sentToPeer && loopCount < requestLoopCutOff); loopCount++ {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			req, err = e.bd.RequestMoreBodies(tx, e.blockReader, currentTime, e.blockPropagator)
			if err != nil {
				return false, fmt.Errorf("request more bodies: %w", err)
			}
			d1 += time.Since(start)
			peer = [64]byte{}
			sentToPeer = false
			if req != nil {
				start = time.Now()
				peer, sentToPeer = e.bodyReqSend(e.ctx, req)
				d2 += time.Since(start)
			}
			if req != nil && sentToPeer {
				start = time.Now()
				e.bd.RequestSent(req, currentTime+uint64(timeout), peer)
				d3 += time.Since(start)
			}
		}

		start := time.Now()
		requestedLow, delivered, err := e.bd.GetDeliveries(tx)
		if err != nil {
			return false, err
		}
		totalDelivered += delivered
		d4 += time.Since(start)
		start = time.Now()

		toProcess := e.bd.NextProcessingCount()

		write := true
		for i := uint64(0); i < toProcess; i++ {
			select {
			case <-logEvery.C:
				logWritingBodies(logPrefix, bodyProgress, headerProgress, e.logger)
			default:
			}
			nextBlock := requestedLow + i
			rawBody := e.bd.GetBodyFromCache(nextBlock, write)
			if rawBody == nil {
				e.bd.NotDelivered(nextBlock)
				write = false
			}
			if !write {
				continue
			}

			e.bd.NotDelivered(nextBlock)
			header, _, err := e.bd.GetHeader(nextBlock, e.blockReader, tx)
			if err != nil {
				return false, err
			}
			blockHeight := header.Number.Uint64()
			if blockHeight != nextBlock {
				return false, fmt.Errorf("[%s] Header block unexpected when matching body, got %v, expected %v", logPrefix, blockHeight, nextBlock)
			}

			// Check existence before write - because WriteRawBody isn't idempotent (it allocates new sequence range for transactions on every call)
			ok, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), blockHeight, rawBody)
			if err != nil {
				return false, fmt.Errorf("WriteRawBodyIfNotExists: %w", err)
			}
			if ok {
				dataflow.BlockBodyDownloadStates.AddChange(blockHeight, dataflow.BlockBodyCleared)
			}

			if blockHeight > bodyProgress {
				bodyProgress = blockHeight
			}
			e.bd.AdvanceLow()
		}

		d5 += time.Since(start)
		start = time.Now()
		if bodyProgress == headerProgress {
			return true, nil
		}

		timer.Stop()
		timer = time.NewTimer(1 * time.Second)
		select {
		case <-e.ctx.Done():
			stopped = true
		case <-logEvery.C:
			deliveredCount, wastedCount := e.bd.DeliveryCounts()
			if prevProgress == bodyProgress {
				noProgressCount++
			} else {
				noProgressCount = 0 // Reset, there was progress
			}
			logDownloadingBodies(logPrefix, bodyProgress, headerProgress-requestedLow, totalDelivered, prevDeliveredCount, deliveredCount,
				prevWastedCount, wastedCount, e.bd.BodyCacheSize(), e.logger)
			prevProgress = bodyProgress
			prevDeliveredCount = deliveredCount
			prevWastedCount = wastedCount
			//logger.Info("Timings", "d1", d1, "d2", d2, "d3", d3, "d4", d4, "d5", d5, "d6", d6)
		case <-timer.C:
			e.logger.Trace("RequestQueueTime (bodies) ticked")
		case <-e.bd.DeliveryNotify:
			e.logger.Trace("bodyLoop woken up by the incoming request")
		}
		d6 += time.Since(start)

		return false, nil
	}

	// kick off the loop and check for any reason to stop and break early
	var shouldBreak bool
	for !stopped && !shouldBreak {
		if shouldBreak, err = loopBody(); err != nil {
			return err
		}
	}

	if stopped {
		return libcommon.ErrStopped
	}
	e.logger.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest", bodyProgress)

	return nil
}

func logDownloadingBodies(logPrefix string, committed, remaining uint64, totalDelivered uint64, prevDeliveredCount, deliveredCount,
	prevWastedCount, wastedCount float64, bodyCacheSize int, logger log.Logger) {
	speed := (deliveredCount - prevDeliveredCount) / float64(logInterval/time.Second)
	wastedSpeed := (wastedCount - prevWastedCount) / float64(logInterval/time.Second)
	if speed == 0 && wastedSpeed == 0 {
		logger.Info(fmt.Sprintf("[%s] No block bodies to write in this log period", logPrefix), "block number", committed)
		return
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info(fmt.Sprintf("[%s] Downloading block bodies", logPrefix),
		"block_num", committed,
		"delivery/sec", libcommon.ByteCount(uint64(speed)),
		"wasted/sec", libcommon.ByteCount(uint64(wastedSpeed)),
		"remaining", remaining,
		"delivered", totalDelivered,
		"cache", libcommon.ByteCount(uint64(bodyCacheSize)),
		"alloc", libcommon.ByteCount(m.Alloc),
		"sys", libcommon.ByteCount(m.Sys),
	)
}

func logWritingBodies(logPrefix string, committed, headerProgress uint64, logger log.Logger) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	remaining := headerProgress - committed
	logger.Info(fmt.Sprintf("[%s] Writing block bodies", logPrefix),
		"block_num", committed,
		"remaining", remaining,
		"alloc", libcommon.ByteCount(m.Alloc),
		"sys", libcommon.ByteCount(m.Sys),
	)
}
