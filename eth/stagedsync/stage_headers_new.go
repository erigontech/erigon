package stagedsync

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
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	db ethdb.Database,
	hd *headerdownload.HeaderDownload,
	chainConfig *params.ChainConfig,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) []byte,
	initialCycle bool,
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
	headHash := rawdb.ReadHeadHeaderHash(tx)
	if hash == (common.Hash{}) {
		if err = fixCanonicalChain(logPrefix, headerProgress, headHash, tx); err != nil {
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

	localTd, err1 := rawdb.ReadTd(tx, headHash, headerProgress)
	if err1 != nil {
		return err1
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, batch, localTd, headerProgress)
	hd.SetHeaderReader(&chainReader{config: chainConfig, batch: batch})

	var req *headerdownload.HeaderRequest
	var peer []byte
	stopped := false
	timer := time.NewTimer(1 * time.Second) // Check periodically even in the abseence of incoming messages
	prevProgress := headerProgress
	for !stopped {
		currentTime := uint64(time.Now().Unix())
		req = hd.RequestMoreHeaders(currentTime)
		if req != nil {
			peer = headerReqSend(ctx, req)
			if peer != nil {
				hd.SentRequest(req, currentTime, 5 /* timeout */)
				//log.Info("Sent request", "height", req.Number)
			}
		}
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && peer != nil && maxRequests > 0 {
			req = hd.RequestMoreHeaders(currentTime)
			if req != nil {
				peer = headerReqSend(ctx, req)
				if peer != nil {
					hd.SentRequest(req, currentTime, 5 /*timeout */)
					//log.Info("Sent request", "height", req.Number)
				}
			}
			maxRequests--
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
		if !initialCycle && headerInserter.AnythingDone() {
			// if this is not an initial cycle, we need to react quickly when new headers are coming in
			break
		}
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
		if initialCycle && hd.InSync() {
			fmt.Printf("Top seen height: %d\n", hd.TopSeenHeight())
			break
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
		if err := fixCanonicalChain(logPrefix, headerInserter.GetHighest(), headerInserter.GetHighestHash(), batch); err != nil {
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
	if stopped {
		return fmt.Errorf("interrupted")
	}
	return nil
}

func fixCanonicalChain(logPrefix string, height uint64, hash common.Hash, tx ethdb.DbWithPendingMutations) error {
	if height == 0 {
		return nil
	}
	ancestorHash := hash
	ancestorHeight := height
	var ch common.Hash
	var err error
	for ch, err = rawdb.ReadCanonicalHash(tx, ancestorHeight); err == nil && ch != ancestorHash; ch, err = rawdb.ReadCanonicalHash(tx, ancestorHeight) {
		if err = rawdb.WriteCanonicalHash(tx, ancestorHash, ancestorHeight); err != nil {
			return fmt.Errorf("[%s] marking canonical header %d %x: %w", logPrefix, ancestorHeight, ancestorHash, err)
		}
		ancestor := rawdb.ReadHeader(tx, ancestorHash, ancestorHeight)
		if ancestor == nil {
			fmt.Printf("ancestor nil for %d %x\n", ancestorHeight, ancestorHash)
		}
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

type chainReader struct {
	config *params.ChainConfig
	batch  ethdb.DbWithPendingMutations
}

func (cr chainReader) Config() *params.ChainConfig  { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(cr.batch, hash, number)
}
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header {
	return rawdb.ReadHeaderByNumber(cr.batch, number)
}
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header { panic("") }
