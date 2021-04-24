package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/txpropagate"
)

type TxPropagateCfg struct {
	tp         *txpropagate.TxPropagate
	txsReqSend func(context.Context, *txpropagate.TxsRequest) []byte
}

func StageTxPropagateCfg(tp *txpropagate.TxPropagate, txsReqSend func(context.Context, *txpropagate.TxsRequest) []byte) TxPropagateCfg {
	return TxPropagateCfg{tp: tp, txsReqSend: txsReqSend}
}

func TxPropagateForward(
	s *StageState,
	ctx context.Context,
	db ethdb.Database,
	cfg TxPropagateCfg) error {
	var tx ethdb.DbWithPendingMutations
	var err error
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
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// TODO: similar to bodies_new for loop
	/*
			logPrefix := s.LogPrefix()
		timer := time.NewTimer(1 * time.Second) // Check periodically even in the abseence of incoming messages
			stopped := false
		for !stopped {
			if req == nil {
				currentTime := uint64(time.Now().Unix())
				req, blockNum = cfg.bd.RequestMoreBodies(db, blockNum, currentTime, cfg.blockPropagator)
			}
			peer = nil
			if req != nil {
				peer = cfg.txsReqSend(ctx, req)
			}
			if req != nil && peer != nil {
				if !penalties {
					log.Info("Sent", "req", fmt.Sprintf("[%d-%d]", req.BlockNums[0], req.BlockNums[len(req.BlockNums)-1]), "peer", string(peer))
				}
				currentTime := uint64(time.Now().Unix())
				cfg.bd.RequestSent(req, currentTime+uint64(timeout), peer)
			}
			for req != nil && peer != nil {
				currentTime := uint64(time.Now().Unix())
				req, blockNum = cfg.bd.RequestMoreBodies(db, blockNum, currentTime, cfg.blockPropagator)
				peer = nil
				if req != nil {
					peer = cfg.txsReqSend(ctx, req)
				}
				if req != nil && peer != nil {
					if !penalties {
						log.Info("Sent", "req", fmt.Sprintf("[%d-%d]", req.BlockNums[0], req.BlockNums[len(req.BlockNums)-1]), "peer", string(peer))
					}
					cfg.bd.RequestSent(req, currentTime+uint64(timeout), peer)
				}
			}
			d := cfg.bd.GetDeliveries()
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

				if batch.BatchSize() >= int(cfg.batchSize) {
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
			//log.Info("Body progress", "block number", bodyProgress, "header progress", headerProgress)
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
			case <-timer.C:
			//log.Info("RequestQueueTime (bodies) ticked")
			case <-cfg.wakeUpChan:
				//log.Info("bodyLoop woken up by the incoming request")
			}
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
			log.Info("Processed", "highest", bodyProgress)
	*/
	if err := s.DoneAndUpdate(tx, bodyProgress); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
