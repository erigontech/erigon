package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	db ethdb.Database,
	hd *headerdownload.HeaderDownload,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) []byte,
	wakeUpChan chan struct{},
) error {
	var headerProgress uint64
	var err error
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
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	logPrefix := s.LogPrefix()
	// Check if this is called straight after the unwinds, which means we need to create new canonical markings
	hash, err1 := rawdb.ReadCanonicalHash(tx, headerProgress)
	if err1 != nil {
		return err1
	}
	if hash == (common.Hash{}) {
		if err = fixCanonicalChain(logPrefix, headerProgress, tx); err != nil {
			return err
		}
		if !useExternalTx {
			if _, err = tx.Commit(); err != nil {
				return err
			}
		}
		s.Done()
		return nil
	}

	log.Info(fmt.Sprintf("[%s] Processing headers...", logPrefix), "from", headerProgress)
	batch := tx.NewBatch()
	defer batch.Rollback()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	headHash := rawdb.ReadHeadHeaderHash(tx)
	headNumber := rawdb.ReadHeaderNumber(tx, headHash)
	localTd, err1 := rawdb.ReadTd(tx, headHash, *headNumber)
	if err1 != nil {
		return err1
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, tx, batch, localTd, headerProgress)

	var req *headerdownload.HeaderRequest
	var peer []byte
	stopped := false
	timer := time.NewTimer(1 * time.Second) // Check periodically even in the abseence of incoming messages
	prevProgress := headerProgress
	for !stopped {
		currentTime := uint64(time.Now().Unix())
		req = hd.RequestMoreHeaders(currentTime, 5 /*timeout */)
		if req != nil {
			peer = headerReqSend(ctx, req)
		}
		for req != nil && peer != nil {
			req = hd.RequestMoreHeaders(currentTime, 5 /*timeout */)
			if req != nil {
				peer = headerReqSend(ctx, req)
			}
		}
		// Send skeleton request if required
		req = hd.RequestSkeleton()
		if req != nil {
			peer = headerReqSend(ctx, req)
		}
		// Load headers into the database
		if err = hd.InsertHeaders(headerInserter.FeedHeader); err != nil {
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
		timer.Stop()
		timer = time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			progress := hd.Progress()
			logProgressHeaders(logPrefix, prevProgress, progress, batch)
			prevProgress = progress
		case <-timer.C:
			log.Debug("RequestQueueTime (header) ticked")
		case <-wakeUpChan:
			log.Debug("headerLoop woken up by the incoming request")
		}
	}
	if err := s.Update(tx, headerInserter.GetHighest()); err != nil {
		return err
	}
	if headerInserter.UnwindPoint() < headerProgress {
		if err := u.UnwindTo(headerInserter.UnwindPoint(), batch); err != nil {
			return fmt.Errorf("%s: failed to unwind to %d: %w", logPrefix, headerInserter.UnwindPoint(), err)
		}
	} else {
		if err := fixCanonicalChain(logPrefix, headerInserter.GetHighest(), batch); err != nil {
			return fmt.Errorf("%s: failed to fix canonical chain: %w", logPrefix, err)
		}
		s.Done()
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
		}
	}
	log.Info("Processed", "highest", headerInserter.GetHighest())
	return nil
}

func fixCanonicalChain(logPrefix string, height uint64, tx ethdb.DbWithPendingMutations) error {
	ancestorHash := rawdb.ReadHeadHeaderHash(tx)
	ancestorHeight := height
	var ch common.Hash
	var err error
	for ch, err = rawdb.ReadCanonicalHash(tx, ancestorHeight); err == nil && ch != ancestorHash; ch, err = rawdb.ReadCanonicalHash(tx, ancestorHeight) {
		if err = rawdb.WriteCanonicalHash(tx, ancestorHash, ancestorHeight); err != nil {
			return fmt.Errorf("[%s] marking canonical header %d %x: %w", logPrefix, ancestorHeight, ancestorHash, err)
		}
		ancestor := rawdb.ReadHeader(tx, ancestorHash, ancestorHeight)
		ancestorHash = ancestor.ParentHash
		ancestorHeight--
	}
	if err != nil {
		return fmt.Errorf("[%s] reading canonical hash for %d: %w", logPrefix, ancestorHeight, err)
	}
	return nil
}

func HeadersUnwind(u *UnwindState, s *StageState, db ethdb.Database) error {
	var err error
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
	// Delete canonical hashes that are being unwound
	var headerProgress uint64
	headerProgress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return err
	}
	for blockHeight := headerProgress; blockHeight > u.UnwindPoint; blockHeight-- {
		if err = rawdb.DeleteCanonicalHash(tx, blockHeight); err != nil {
			return err
		}
	}
	if err = u.Skip(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if _, err := tx.Commit(); err != nil {
			return err
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
