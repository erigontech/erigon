package bodydownload

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	logInterval = 30 * time.Second
)

// Forward progresses Bodies stage in the forward direction
func Forward(
	logPrefix string,
	ctx context.Context,
	db ethdb.Database,
	bd *BodyDownload,
	bodyReqSend func(context.Context, *BodyRequest) []byte,
	penalise func(context.Context, []byte),
	updateHead func(head uint64, hash common.Hash, td *big.Int),
	wakeUpChan chan struct{}, timeout int) error {
	var headerProgress, bodyProgress uint64
	var err error
	headerProgress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return err
	}
	bodyProgress, err = stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("[%s] Processing bodies...", logPrefix), "from", bodyProgress, "to", headerProgress)
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	batch := tx.NewBatch()
	defer batch.Rollback()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var prevDeliveredCount float64 = 0
	var prevWastedCount float64 = 0
	timer := time.NewTimer(1 * time.Second) // Check periodically even in the abseence of incoming messages
	var blockNum uint64
	var req *BodyRequest
	var peer []byte
	stopped := false
	var headHash common.Hash
	var headSet bool
	for !stopped {
		penaltyPeers := bd.GetPenaltyPeers()
		for _, penaltyPeer := range penaltyPeers {
			penalise(ctx, penaltyPeer)
		}
		if req == nil {
			currentTime := uint64(time.Now().Unix())
			req, blockNum = bd.RequestMoreBodies(db, blockNum, currentTime)
		}
		peer = nil
		if req != nil {
			peer = bodyReqSend(ctx, req)
		}
		for req != nil && peer != nil {
			currentTime := uint64(time.Now().Unix())
			bd.RequestSent(req, currentTime+uint64(timeout), peer)
			log.Info(fmt.Sprintf("Generated request for %d bodies [%d-%d] sent to %s", len(req.BlockNums), req.BlockNums[0], req.BlockNums[len(req.BlockNums)-1], peer))
			req, blockNum = bd.RequestMoreBodies(db, blockNum, currentTime)
			peer = nil
			if req != nil {
				peer = bodyReqSend(ctx, req)
			}
		}
		d := bd.GetDeliveries()
		for _, block := range d {
			if err = rawdb.WriteBody(batch, block.Hash(), block.NumberU64(), block.Body()); err != nil {
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
			if batch.BatchSize() >= batch.IdealBatchSize() {
				if err = batch.CommitAndBegin(context.Background()); err != nil {
					return err
				}
				if !useExternalTx {
					if err = tx.CommitAndBegin(context.Background()); err != nil {
						return err
					}
				}
			}
		}
		log.Info("Body progress", "block number", bodyProgress, "header progress", headerProgress)
		if bodyProgress == headerProgress {
			break
		}
		timer.Stop()
		timer = time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			deliveredCount, wastedCount := bd.DeliveryCounts()
			logProgress(logPrefix, bodyProgress, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount, batch)
			prevDeliveredCount = deliveredCount
			prevWastedCount = wastedCount
			bd.PrintPeerMap()
		case <-timer.C:
		//log.Info("RequestQueueTime (bodies) ticked")
		case <-wakeUpChan:
			//log.Info("bodyLoop woken up by the incoming request")
		}
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	if headSet {
		if headTd, err := rawdb.ReadTd(db, headHash, bodyProgress); err == nil {
			updateHead(bodyProgress, headHash, headTd)
		} else {
			log.Error("Failed to get total difficulty", "hash", headHash, "height", bodyProgress, "error", err)
		}
	}
	log.Info("Processed", "highest", bodyProgress)
	return nil
}

func logProgress(logPrefix string, committed uint64, prevDeliveredCount, deliveredCount, prevWastedCount, wastedCount float64, batch ethdb.DbWithPendingMutations) {
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
