package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/dataflow"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

const requestLoopCutOff int = 1

type BodiesCfg struct {
	db              kv.RwDB
	bd              *bodydownload.BodyDownload
	bodyReqSend     func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool)
	penalise        func(context.Context, []headerdownload.PenaltyItem)
	blockPropagator adapter.BlockPropagator
	timeout         int
	chanConfig      chain.Config
	blockReader     services.FullBlockReader
	blockWriter     *blockio.BlockWriter
	loopBreakCheck  func(int) bool
}

func StageBodiesCfg(db kv.RwDB, bd *bodydownload.BodyDownload,
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool), penalise func(context.Context, []headerdownload.PenaltyItem),
	blockPropagator adapter.BlockPropagator, timeout int,
	chanConfig chain.Config,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	loopBreakCheck func(int) bool) BodiesCfg {
	return BodiesCfg{
		db: db, bd: bd, bodyReqSend: bodyReqSend, penalise: penalise, blockPropagator: blockPropagator,
		timeout: timeout, chanConfig: chanConfig, blockReader: blockReader,
		blockWriter: blockWriter, loopBreakCheck: loopBreakCheck}
}

// BodiesForward progresses Bodies stage in the forward direction
func BodiesForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg BodiesCfg,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
	firstCycle bool,
	logger log.Logger,
) error {
	var doUpdate bool

	startTime := time.Now()

	if s.BlockNumber < cfg.blockReader.FrozenBlocks() {
		s.BlockNumber = cfg.blockReader.FrozenBlocks()
		doUpdate = true
	}

	var d1, d2, d3, d4, d5, d6 time.Duration
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	timeout := cfg.timeout

	// this update is required, because cfg.bd.UpdateFromDb(tx) below reads it and initialises requestedLow accordingly
	// if not done, it will cause downloading from block 1
	if doUpdate {
		if err := s.Update(tx, s.BlockNumber); err != nil {
			return err
		}
	}
	// This will update bd.maxProgress
	if _, _, _, _, err = cfg.bd.UpdateFromDb(tx); err != nil {
		return err
	}
	defer cfg.bd.ClearBodyCache()
	var headerProgress, bodyProgress uint64

	if cfg.chanConfig.Bor != nil {
		headerProgress, err = stages.GetStageProgress(tx, stages.BorHeimdall)
		if err != nil {
			return err
		}
	}

	if headerProgress == 0 {
		headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return err
		}
	}

	bodyProgress = s.BlockNumber
	if bodyProgress >= headerProgress {
		return nil
	}

	logPrefix := s.LogPrefix()
	if headerProgress <= bodyProgress+16 {
		// When processing small number of blocks, we can afford wasting more bandwidth but get blocks quicker
		timeout = 1
	} else {
		// Do not print logs for short periods
		diagnostics.Send(diagnostics.BodiesProcessingUpdate{
			From: bodyProgress,
			To:   headerProgress,
		})

		logger.Info(fmt.Sprintf("[%s] Processing bodies...", logPrefix), "from", bodyProgress, "to", headerProgress)
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// Property of blockchain: same block in different forks will have different hashes.
	// Means - can mark all canonical blocks as non-canonical on unwind, and
	// do opposite here - without storing any meta-info.
	if err := cfg.blockWriter.MakeBodiesCanonical(tx, s.BlockNumber+1); err != nil {
		return fmt.Errorf("make block canonical: %w", err)
	}

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
	cr := ChainReader{Cfg: cfg.chanConfig, Db: tx, BlockReader: cfg.blockReader, Logger: logger}

	loopBody := func() (bool, error) {
		// loopCount is used here to ensure we don't get caught in a constant loop of making requests
		// having some time out so requesting again and cycling like that forever.  We'll cap it
		// and break the loop so we can see if there are any records to actually process further down
		// then come back here again in the next cycle
		for loopCount := 0; loopCount == 0 || (req != nil && sentToPeer && loopCount < requestLoopCutOff); loopCount++ {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			req, err = cfg.bd.RequestMoreBodies(tx, cfg.blockReader, currentTime, cfg.blockPropagator)
			if err != nil {
				return false, fmt.Errorf("request more bodies: %w", err)
			}
			d1 += time.Since(start)
			peer = [64]byte{}
			sentToPeer = false
			if req != nil {
				start = time.Now()
				peer, sentToPeer = cfg.bodyReqSend(ctx, req)
				d2 += time.Since(start)
			}
			if req != nil && sentToPeer {
				start = time.Now()
				cfg.bd.RequestSent(req, currentTime+uint64(timeout), peer)
				d3 += time.Since(start)
			}
		}

		start := time.Now()
		requestedLow, delivered, err := cfg.bd.GetDeliveries(tx)
		if err != nil {
			return false, err
		}

		// this can happen if we have bor heimdall processing
		// as the body downloader only has access to headers which
		// may be higher than the current bor processing stage
		if requestedLow > headerProgress {
			requestedLow = headerProgress
		}

		totalDelivered += delivered
		d4 += time.Since(start)
		start = time.Now()

		toProcess := cfg.bd.NextProcessingCount()

		write := true
		for i := uint64(0); i < toProcess; i++ {
			select {
			case <-logEvery.C:
				logWritingBodies(logPrefix, bodyProgress, headerProgress, logger)
			default:
			}
			nextBlock := requestedLow + i
			rawBody := cfg.bd.GetBodyFromCache(nextBlock, write /* delete */)
			if rawBody == nil {
				cfg.bd.NotDelivered(nextBlock)
				write = false
			}
			if !write {
				continue
			}
			cfg.bd.NotDelivered(nextBlock)
			header, _, err := cfg.bd.GetHeader(nextBlock, cfg.blockReader, tx)
			if err != nil {
				return false, err
			}

			// this check is necessary if we have bor heimdall processing as the body downloader only has
			// access to headers which may be higher than the current bor processing stage
			if headerNumber := header.Number.Uint64(); headerNumber <= headerProgress {
				blockHeight := headerNumber
				if blockHeight != nextBlock {
					return false, fmt.Errorf("[%s] Header block unexpected when matching body, got %v, expected %v", logPrefix, blockHeight, nextBlock)
				}

				// Txn & uncle roots are verified via bd.requestedMap
				err = cfg.bd.Engine.VerifyUncles(cr, header, rawBody.Uncles)
				if err != nil {
					logger.Error(fmt.Sprintf("[%s] Uncle verification failed", logPrefix), "number", blockHeight, "hash", header.Hash().String(), "err", err)
					if err := u.UnwindTo(blockHeight-1, BadBlock(header.Hash(), fmt.Errorf("Uncle verification failed: %w", err)), tx); err != nil {
						return false, err
					}
					return true, nil
				}

				metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, header.Number.Uint64(), logger)

				// Check existence before write - because WriteRawBody isn't idempotent (it allocates new sequence range for transactions on every call)
				ok, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), blockHeight, rawBody)
				if err != nil {
					return false, fmt.Errorf("WriteRawBodyIfNotExists: %w", err)
				}
				if ok {
					if err := rawdb.AppendCanonicalTxNums(tx, blockHeight); err != nil {
						return false, err
					}
				}
				if ok {
					dataflow.BlockBodyDownloadStates.AddChange(blockHeight, dataflow.BlockBodyCleared)
				}

				if blockHeight > bodyProgress {
					bodyProgress = blockHeight
					if err = s.Update(tx, blockHeight); err != nil {
						return false, fmt.Errorf("saving Bodies progress: %w", err)
					}
				}
				cfg.bd.AdvanceLow()
			}

			if cfg.loopBreakCheck != nil && cfg.loopBreakCheck(int(i)) {
				return true, nil
			}
		}

		d5 += time.Since(start)
		start = time.Now()
		if bodyProgress == headerProgress {
			return true, nil
		}
		if test {
			stopped = true
			return true, nil
		}
		if !firstCycle && s.BlockNumber > 0 && noProgressCount >= 5 {
			return true, nil
		}
		timer.Stop()
		timer = time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			deliveredCount, wastedCount := cfg.bd.DeliveryCounts()
			if prevProgress == bodyProgress {
				noProgressCount++
			} else {
				noProgressCount = 0 // Reset, there was progress
			}
			logDownloadingBodies(logPrefix, bodyProgress, headerProgress-requestedLow, totalDelivered, prevDeliveredCount, deliveredCount,
				prevWastedCount, wastedCount, cfg.bd.BodyCacheSize(), logger)
			prevProgress = bodyProgress
			prevDeliveredCount = deliveredCount
			prevWastedCount = wastedCount
			//logger.Info("Timings", "d1", d1, "d2", d2, "d3", d3, "d4", d4, "d5", d5, "d6", d6)
		case <-timer.C:
			logger.Trace("RequestQueueTime (bodies) ticked")
		case <-cfg.bd.DeliveryNotify:
			logger.Trace("bodyLoop woken up by the incoming request")
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

	// remove the temporary bucket for bodies stage
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	if stopped {
		return libcommon.ErrStopped
	}
	if bodyProgress > s.BlockNumber+16 {
		blocks := bodyProgress - s.BlockNumber
		secs := time.Since(startTime).Seconds()

		diagnostics.Send(diagnostics.BodiesProcessedUpdate{
			HighestBlock: bodyProgress,
			Blocks:       blocks,
			TimeElapsed:  secs,
			BlkPerSec:    float64(blocks) / secs,
		})

		logger.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest", bodyProgress,
			"blocks", blocks, "in", secs, "blk/sec", uint64(float64(blocks)/secs))
	}
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

	diagnostics.Send(diagnostics.BodiesDownloadBlockUpdate{
		BlockNumber:    committed,
		DeliveryPerSec: uint64(speed),
		WastedPerSec:   uint64(wastedSpeed),
		Remaining:      remaining,
		Delivered:      totalDelivered,
		BlockPerSec:    totalDelivered / uint64(logInterval/time.Second),
		Cache:          uint64(bodyCacheSize),
		Alloc:          m.Alloc,
		Sys:            m.Sys,
	})

	logger.Info(fmt.Sprintf("[%s] Downloading block bodies", logPrefix),
		"block_num", committed,
		"delivery/sec", libcommon.ByteCount(uint64(speed)),
		"wasted/sec", libcommon.ByteCount(uint64(wastedSpeed)),
		"remaining", remaining,
		"delivered", totalDelivered,
		"blk/sec", totalDelivered/uint64(logInterval/time.Second),
		"cache", libcommon.ByteCount(uint64(bodyCacheSize)),
		"alloc", libcommon.ByteCount(m.Alloc),
		"sys", libcommon.ByteCount(m.Sys),
	)
}

func logWritingBodies(logPrefix string, committed, headerProgress uint64, logger log.Logger) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	remaining := headerProgress - committed

	diagnostics.Send(diagnostics.BodiesWriteBlockUpdate{
		BlockNumber: committed,
		Remaining:   remaining,
		Alloc:       m.Alloc,
		Sys:         m.Sys,
	})

	logger.Info(fmt.Sprintf("[%s] Writing block bodies", logPrefix),
		"block_num", committed,
		"remaining", remaining,
		"alloc", libcommon.ByteCount(m.Alloc),
		"sys", libcommon.ByteCount(m.Sys),
	)
}

func UnwindBodiesStage(u *UnwindState, tx kv.RwTx, cfg BodiesCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if err := cfg.blockWriter.MakeBodiesNonCanonical(tx, u.UnwindPoint+1); err != nil {
		return err
	}

	if err = u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
