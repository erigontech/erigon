package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

var stageBodiesGauge = metrics.NewRegisteredGauge("stage/bodies", nil)

type BodiesCfg struct {
	db              ethdb.RwKV
	bd              *bodydownload.BodyDownload
	bodyReqSend     func(context.Context, *bodydownload.BodyRequest) []byte
	penalise        func(context.Context, []headerdownload.PenaltyItem)
	blockPropagator adapter.BlockPropagator
	timeout         int
	chanConfig      params.ChainConfig
	batchSize       datasize.ByteSize
}

func StageBodiesCfg(
	db ethdb.RwKV,
	bd *bodydownload.BodyDownload,
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) []byte,
	penalise func(context.Context, []headerdownload.PenaltyItem),
	blockPropagator adapter.BlockPropagator,
	timeout int,
	chanConfig params.ChainConfig,
	batchSize datasize.ByteSize,
) BodiesCfg {
	return BodiesCfg{db: db, bd: bd, bodyReqSend: bodyReqSend, penalise: penalise, blockPropagator: blockPropagator, timeout: timeout, chanConfig: chanConfig, batchSize: batchSize}
}

// BodiesForward progresses Bodies stage in the forward direction
func BodiesForward(
	s *StageState,
	ctx context.Context,
	tx ethdb.RwTx,
	cfg BodiesCfg) error {

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
	// This will update bd.maxProgress
	if _, _, _, err = cfg.bd.UpdateFromDb(tx); err != nil {
		return err
	}
	var headerProgress, bodyProgress uint64
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	bodyProgress, err = stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return err
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
	var prevDeliveredCount float64 = 0
	var prevWastedCount float64 = 0
	timer := time.NewTimer(1 * time.Second) // Check periodically even in the abseence of incoming messages
	var blockNum uint64
	var req *bodydownload.BodyRequest
	var peer []byte
	stopped := false
	var headHash common.Hash
	for !stopped {
		// TODO: this is incorrect use
		if req == nil {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			req, blockNum, err = cfg.bd.RequestMoreBodies(tx, blockNum, currentTime, cfg.blockPropagator)
			if err != nil {
				return fmt.Errorf("[%s] request more bodies: %w", logPrefix, err)
			}
			d1 += time.Since(start)
		}
		peer = nil
		if req != nil {
			start := time.Now()
			peer = cfg.bodyReqSend(ctx, req)
			d2 += time.Since(start)
		}
		if req != nil && peer != nil {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			cfg.bd.RequestSent(req, currentTime+uint64(timeout), peer)
			d3 += time.Since(start)
		}
		for req != nil && peer != nil {
			start := time.Now()
			currentTime := uint64(time.Now().Unix())
			req, blockNum, err = cfg.bd.RequestMoreBodies(tx, blockNum, currentTime, cfg.blockPropagator)
			if err != nil {
				return fmt.Errorf("[%s] request more bodies: %w", logPrefix, err)
			}
			d1 += time.Since(start)
			peer = nil
			if req != nil {
				start = time.Now()
				peer = cfg.bodyReqSend(ctx, req)
				d2 += time.Since(start)
			}
			if req != nil && peer != nil {
				start = time.Now()
				cfg.bd.RequestSent(req, currentTime+uint64(timeout), peer)
				d3 += time.Since(start)
			}
		}
		start := time.Now()
		// can't use same transaction from another goroutine
		var verifyUncles = func(peerID string, header *types.Header, uncles []*types.Header) error {
			/* TODO: it always fail now, because parent block is not in db yet
			cr := ChainReader{Cfg: cfg.chanConfig, Db: batch}
			penalty, err := cfg.bd.VerifyUncles(header, uncles, cr)
			if err != nil {
				if penalty != headerdownload.NoPenalty {
					log.Debug("penalize", "peer", peerID, "reason", err)
					cfg.penalise(ctx, []headerdownload.PenaltyItem{{PeerID: peerID, Penalty: penalty}})
				}
				return err
			}
			*/
			return nil
		}
		headers, rawBodies, err := cfg.bd.GetDeliveries(verifyUncles)
		if err != nil {
			return err
		}
		d4 += time.Since(start)
		start = time.Now()
		for i, header := range headers {
			rawBody := rawBodies[i]
			blockHeight := header.Number.Uint64()
			if err = rawdb.WriteRawBody(tx, header.Hash(), blockHeight, rawBody); err != nil {
				return fmt.Errorf("[%s] writing block body: %w", logPrefix, err)
			}
			if blockHeight > bodyProgress {
				bodyProgress = blockHeight
				if err = stages.SaveStageProgress(tx, stages.Bodies, blockHeight); err != nil {
					return fmt.Errorf("[%s] saving Bodies progress: %w", logPrefix, err)
				}
				headHash = header.Hash()
				rawdb.WriteHeadBlockHash(tx, headHash)
			}
		}
		d5 += time.Since(start)
		start = time.Now()
		if bodyProgress == headerProgress {
			break
		}
		timer.Stop()
		timer = time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			deliveredCount, wastedCount := cfg.bd.DeliveryCounts()
			logProgressBodies(logPrefix, bodyProgress, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount)
			prevDeliveredCount = deliveredCount
			prevWastedCount = wastedCount
			//log.Info("Timings", "d1", d1, "d2", d2, "d3", d3, "d4", d4, "d5", d5, "d6", d6)
		case <-timer.C:
			log.Trace("RequestQueueTime (bodies) ticked")
		case <-cfg.bd.DeliveryNotify:
			log.Debug("bodyLoop woken up by the incoming request")
		}
		d6 += time.Since(start)
		stageBodiesGauge.Update(int64(bodyProgress))
	}
	if err := s.DoneAndUpdate(tx, bodyProgress); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	log.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest", bodyProgress)
	return nil
}

func logProgressBodies(logPrefix string, committed uint64, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount float64) {
	speed := (deliveredCount - prevDeliveredCount) / float64(logInterval/time.Second)
	wastedSpeed := (wastedCount - prevWastedCount) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block bodies", logPrefix),
		"number committed", committed,
		"delivery /second", common.StorageSize(speed),
		"wasted /second", common.StorageSize(wastedSpeed),
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys),
		"numGC", int(m.NumGC))
}

func UnwindBodiesStage(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg BodiesCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	err := u.Done(tx)
	logPrefix := s.state.LogPrefix()
	if err != nil {
		return fmt.Errorf("%s: reset: %v", logPrefix, err)
	}
	if !useExternalTx {
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("%s: failed to write db commit: %v", logPrefix, err)
		}
	}
	return nil
}
