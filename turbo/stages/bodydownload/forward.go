package bodydownload

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	logInterval = 30 * time.Second
)

// Forward progresses Bodies stage in the forward direction
func Forward(logPrefix string, db ethdb.Database, blockChannel chan *types.Block) error {
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
	var logBlock uint64
	var count int
	for block := range blockChannel {
		if err = rawdb.WriteBody(batch, block.Hash(), block.NumberU64(), block.Body()); err != nil {
			return fmt.Errorf("[%s] writing block body: %w", logPrefix, err)
		}
		blockHeight := block.NumberU64()
		if blockHeight > bodyProgress {
			bodyProgress = blockHeight
			if err = stages.SaveStageProgress(batch, stages.Bodies, blockHeight); err != nil {
				return fmt.Errorf("[%s] saving Bodies progress: %w", logPrefix, err)
			}
			rawdb.WriteHeadBlockHash(batch, block.Header().Hash())
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
		select {
		default:
		case <-logEvery.C:
			logBlock = logProgress(logPrefix, logBlock, blockHeight, batch)
		}
		count++
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	log.Info("Processed", "block bodies", count, "highest", bodyProgress)
	return nil
}

func logProgress(logPrefix string, prev, now uint64, batch ethdb.DbWithPendingMutations) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block bodies", logPrefix),
		"number", now,
		"blk/second", speed,
		"batch", common.StorageSize(batch.BatchSize()),
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys),
		"numGC", int(m.NumGC))
	return now
}
