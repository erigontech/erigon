package stagedsync

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(s *StageState, u Unwinder, ctx context.Context, db ethdb.Database, hd *headerdownload.HeaderDownload) error {
	files, buffer := hd.PrepareStageData()
	if len(files) == 0 && (buffer == nil || buffer.IsEmpty()) {
		return nil
	}

	logPrefix := s.LogPrefix()

	var headerProgress uint64
	var err error
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("[%s] Processing headers...", logPrefix), "from", headerProgress)
	batch := tx.NewBatch()
	defer batch.Rollback()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var logBlock uint64

	headHash := rawdb.ReadHeadHeaderHash(tx)
	headNumber := rawdb.ReadHeaderNumber(tx, headHash)
	localTd, err1 := rawdb.ReadTd(tx, headHash, *headNumber)
	if err1 != nil {
		return err1
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, tx, batch, localTd, headerProgress)
	if err1 = headerdownload.ReadFilesAndBuffer(files, buffer, func(header *types.Header, blockHeight uint64) error {
		if err = headerInserter.FeedHeader(header, blockHeight); err != nil {
			return err
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
			logBlock = logProgressHeaders(logPrefix, logBlock, blockHeight, batch)
		}
		return nil
	}); err1 != nil {
		return err1
	}
	if headerInserter.UnwindPoint() < headerProgress {
		if err := u.UnwindTo(headerInserter.UnwindPoint(), tx); err != nil {
			return fmt.Errorf("%s: failed to unwind to %d: %v", logPrefix, headerInserter.UnwindPoint(), err)
		}
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if err := s.DoneAndUpdate(tx, headerInserter.GetHighest()); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	log.Info("Processed", "highest", headerInserter.GetHighest())
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			log.Error("Could not remove", "file", file, "error", err)
		}
	}
	return nil
}

func logProgressHeaders(logPrefix string, prev, now uint64, batch ethdb.DbWithPendingMutations) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block headers", logPrefix),
		"number", now,
		"blk/second", speed,
		"batch", common.StorageSize(batch.BatchSize()),
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys),
		"numGC", int(m.NumGC))

	return now
}
