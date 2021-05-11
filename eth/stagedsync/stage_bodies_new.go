package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

var stageBodiesGauge = metrics.NewRegisteredGauge("stage/bodies", nil)

type BodiesCfg struct {
	db              ethdb.RwKV
	bd              *bodydownload.BodyDownload
	bodyReqSend     func(context.Context, *bodydownload.BodyRequest) []byte
	penalise        func(context.Context, []headerdownload.PenaltyItem)
	updateHead      func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int)
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
	updateHead func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int),
	blockPropagator adapter.BlockPropagator,
	timeout int,
	chanConfig params.ChainConfig,
	batchSize datasize.ByteSize,
) BodiesCfg {
	return BodiesCfg{db: db, bd: bd, bodyReqSend: bodyReqSend, penalise: penalise, updateHead: updateHead, blockPropagator: blockPropagator, timeout: timeout, chanConfig: chanConfig, batchSize: batchSize}
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
	penalties := true
	if headerProgress-bodyProgress <= 16 {
		// When processing small number of blocks, we can afford wasting more bandwidth but get blocks quicker
		timeout = 1
		penalties = false
	}
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Processing bodies...", logPrefix), "from", bodyProgress, "to", headerProgress)
	batch := ethdb.NewBatch(tx)
	defer batch.Rollback()
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
	var headSet bool
	for !stopped {
		/*
			if penalties {
				penaltyPeers := bd.GetPenaltyPeers()
				for _, penaltyPeer := range penaltyPeers {
					penalise(ctx, penaltyPeer)
				}
			}
		*/
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
			if !penalties {
				log.Info("Sent", "req", fmt.Sprintf("[%d-%d]", req.BlockNums[0], req.BlockNums[len(req.BlockNums)-1]), "peer", string(peer))
			}
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
				if !penalties {
					log.Info("Sent", "req", fmt.Sprintf("[%d-%d]", req.BlockNums[0], req.BlockNums[len(req.BlockNums)-1]), "peer", string(peer))
				}
				cfg.bd.RequestSent(req, currentTime+uint64(timeout), peer)
				d3 += time.Since(start)
			}
		}
		start := time.Now()
		cr := ChainReader{Cfg: cfg.chanConfig, Db: batch}
		d, penalties, err := cfg.bd.GetDeliveries(func(block *types.Block) (headerdownload.Penalty, error, error) {
			return cfg.bd.ValidateBody(block, cr)
		})
		if err != nil {
			return err
		}
		cfg.penalise(ctx, penalties)
		d4 += time.Since(start)
		start = time.Now()
		for _, block := range d {
			if err = rawdb.WriteBodyDeprecated(batch, block.Hash(), block.NumberU64(), block.Body()); err != nil {
				return fmt.Errorf("[%s] writing block body: %w", logPrefix, err)
			}
			blockHeight := block.NumberU64()
			if blockHeight > bodyProgress {
				bodyProgress = blockHeight
				if err = stages.SaveStageProgress(batch, stages.Bodies, blockHeight); err != nil {
					return fmt.Errorf("[%s] saving Bodies progress: %w", logPrefix, err)
				}
				headHash = block.Header().Hash()
				rawdb.WriteHeadBlockHash(batch, headHash)
				headSet = true
			}

			if batch.BatchSize() >= int(cfg.batchSize) {
				if err = batch.Commit(); err != nil {
					return err
				}
				if !useExternalTx {
					if err := s.Update(tx, bodyProgress); err != nil {
						return err
					}
					if err = tx.Commit(); err != nil {
						return err
					}
					tx, err = cfg.db.BeginRw(ctx)
					if err != nil {
						return err
					}
				}
				batch = ethdb.NewBatch(tx)
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
			logProgressBodies(logPrefix, bodyProgress, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount, batch)
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
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if headSet {
		if headTd, err := rawdb.ReadTd(tx, headHash, bodyProgress); err == nil {
			headTd256 := new(uint256.Int)
			headTd256.SetFromBig(headTd)
			cfg.updateHead(ctx, bodyProgress, headHash, headTd256)
		} else {
			log.Error("Failed to get total difficulty", "hash", headHash, "height", bodyProgress, "error", err)
		}
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

func logProgressBodies(logPrefix string, committed uint64, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount float64, batch ethdb.DbWithPendingMutations) {
	speed := (deliveredCount - prevDeliveredCount) / float64(logInterval/time.Second)
	wastedSpeed := (wastedCount - prevWastedCount) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block bodies", logPrefix),
		"number committed", committed,
		"delivery /second", common.StorageSize(speed),
		"wasted /second", common.StorageSize(wastedSpeed),
		"batch", common.StorageSize(batch.BatchSize()),
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys),
		"numGC", int(m.NumGC))
}
