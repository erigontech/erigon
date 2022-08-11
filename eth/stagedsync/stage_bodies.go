package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
)

type BodiesCfg struct {
	db              kv.RwDB
	bd              *bodydownload.BodyDownload
	bodyReqSend     func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool)
	penalise        func(context.Context, []headerdownload.PenaltyItem)
	blockPropagator adapter.BlockPropagator
	timeout         int
	chanConfig      params.ChainConfig
	batchSize       datasize.ByteSize
	snapshots       *snapshotsync.RoSnapshots
	blockReader     services.FullBlockReader
}

func StageBodiesCfg(
	db kv.RwDB,
	bd *bodydownload.BodyDownload,
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool),
	penalise func(context.Context, []headerdownload.PenaltyItem),
	blockPropagator adapter.BlockPropagator,
	timeout int,
	chanConfig params.ChainConfig,
	batchSize datasize.ByteSize,
	snapshots *snapshotsync.RoSnapshots,
	blockReader services.FullBlockReader,
) BodiesCfg {
	return BodiesCfg{db: db, bd: bd, bodyReqSend: bodyReqSend, penalise: penalise, blockPropagator: blockPropagator, timeout: timeout, chanConfig: chanConfig, batchSize: batchSize, snapshots: snapshots, blockReader: blockReader}
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
) error {
	var doUpdate bool
	if cfg.snapshots != nil && s.BlockNumber < cfg.snapshots.BlocksAvailable() {
		s.BlockNumber = cfg.snapshots.BlocksAvailable()
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
	if _, _, _, err = cfg.bd.UpdateFromDb(tx); err != nil {
		return err
	}
	var headerProgress, bodyProgress uint64
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
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
		log.Info(fmt.Sprintf("[%s] Processing bodies...", logPrefix), "from", bodyProgress, "to", headerProgress)
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// Property of blockchain: same block in different forks will have different hashes.
	// Means - can mark all canonical blocks as non-canonical on unwind, and
	// do opposite here - without storing any meta-info.
	if err := rawdb.MakeBodiesCanonical(tx, s.BlockNumber+1, ctx, logPrefix, logEvery); err != nil {
		return fmt.Errorf("make block canonical: %w", err)
	}

	var prevDeliveredCount float64 = 0
	var prevWastedCount float64 = 0
	timer := time.NewTimer(1 * time.Second) // Check periodically even in the abseence of incoming messages
	var blockNum uint64
	var req *bodydownload.BodyRequest
	var peer [64]byte
	var sentToPeer bool
	stopped := false
	prevProgress := bodyProgress
	noProgressCount := 0 // How many time the progress was printed without actual progress
Loop:
	for !stopped {
		// TODO: this is incorrect use
		if req == nil {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			req, blockNum, err = cfg.bd.RequestMoreBodies(tx, cfg.blockReader, blockNum, currentTime, cfg.blockPropagator)
			if err != nil {
				return fmt.Errorf("request more bodies: %w", err)
			}
			d1 += time.Since(start)
		}
		peer = [64]byte{}
		sentToPeer = false
		if req != nil {
			start := time.Now()
			peer, sentToPeer = cfg.bodyReqSend(ctx, req)
			d2 += time.Since(start)
		}
		if req != nil && sentToPeer {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			cfg.bd.RequestSent(req, currentTime+uint64(timeout), peer)
			d3 += time.Since(start)
		}
		for req != nil && sentToPeer {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			req, blockNum, err = cfg.bd.RequestMoreBodies(tx, cfg.blockReader, blockNum, currentTime, cfg.blockPropagator)
			if err != nil {
				return fmt.Errorf("request more bodies: %w", err)
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
		headers, rawBodies, err := cfg.bd.GetDeliveries()
		if err != nil {
			return err
		}
		d4 += time.Since(start)
		start = time.Now()
		cr := ChainReader{Cfg: cfg.chanConfig, Db: tx}
		for i, header := range headers {
			rawBody := rawBodies[i]
			blockHeight := header.Number.Uint64()
			// Txn & uncle roots are verified via bd.requestedMap
			err := cfg.bd.Engine.VerifyUncles(cr, header, rawBody.Uncles)
			if err != nil {
				log.Error(fmt.Sprintf("[%s] Uncle verification failed", logPrefix), "number", blockHeight, "hash", header.Hash().String(), "err", err)
				u.UnwindTo(blockHeight-1, header.Hash())
				break Loop
			}

			// Check existence before write - because WriteRawBody isn't idempotent (it allocates new sequence range for transactions on every call)
			if err = rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), blockHeight, rawBody); err != nil {
				return fmt.Errorf("WriteRawBodyIfNotExists: %w", err)
			}

			if blockHeight > bodyProgress {
				bodyProgress = blockHeight
				if err = s.Update(tx, blockHeight); err != nil {
					return fmt.Errorf("saving Bodies progress: %w", err)
				}
			}
		}
		d5 += time.Since(start)
		start = time.Now()
		if bodyProgress == headerProgress {
			break
		}
		if test {
			stopped = true
			break
		}
		if !firstCycle && s.BlockNumber > 0 && noProgressCount >= 5 {
			break
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
			logProgressBodies(logPrefix, bodyProgress, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount)
			prevProgress = bodyProgress
			prevDeliveredCount = deliveredCount
			prevWastedCount = wastedCount
			//log.Info("Timings", "d1", d1, "d2", d2, "d3", d3, "d4", d4, "d5", d5, "d6", d6)
		case <-timer.C:
			log.Trace("RequestQueueTime (bodies) ticked")
		case <-cfg.bd.DeliveryNotify:
			log.Trace("bodyLoop woken up by the incoming request")
		}
		d6 += time.Since(start)
	}
	if err := s.Update(tx, bodyProgress); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	if stopped {
		return libcommon.ErrStopped
	}
	if bodyProgress > s.BlockNumber+16 {
		log.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest", bodyProgress)
	}
	return nil
}

func logProgressBodies(logPrefix string, committed uint64, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount float64) {
	speed := (deliveredCount - prevDeliveredCount) / float64(logInterval/time.Second)
	wastedSpeed := (wastedCount - prevWastedCount) / float64(logInterval/time.Second)
	var m runtime.MemStats
	libcommon.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block bodies", logPrefix),
		"block_num", committed,
		"delivery/sec", libcommon.ByteCount(uint64(speed)),
		"wasted/sec", libcommon.ByteCount(uint64(wastedSpeed)),
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

	badBlock := u.BadBlock != (common.Hash{})
	if err := rawdb.MakeBodiesNonCanonical(tx, u.UnwindPoint+1, badBlock /* deleteBodies */, ctx, u.LogPrefix(), logEvery); err != nil {
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

func PruneBodiesStage(s *PruneState, tx kv.RwTx, cfg BodiesCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
