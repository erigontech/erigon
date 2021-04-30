package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

type HeadersCfg struct {
	hd                *headerdownload.HeaderDownload
	chainConfig       params.ChainConfig
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) []byte
	announceNewHashes func(context.Context, eth.NewBlockHashesPacket)
	wakeUpChan        chan struct{}
	batchSize         datasize.ByteSize
}

func StageHeadersCfg(
	headerDownload *headerdownload.HeaderDownload,
	chainConfig params.ChainConfig,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) []byte,
	announceNewHashes func(context.Context, eth.NewBlockHashesPacket),
	wakeUpChan chan struct{},
	batchSize datasize.ByteSize,
) HeadersCfg {
	return HeadersCfg{
		hd:                headerDownload,
		chainConfig:       chainConfig,
		headerReqSend:     headerReqSend,
		announceNewHashes: announceNewHashes,
		wakeUpChan:        wakeUpChan,
		batchSize:         batchSize,
	}
}

// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	db ethdb.Database,
	cfg HeadersCfg,
	initialCycle bool,
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
			if err = tx.Commit(); err != nil {
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

	localTd, err1 := rawdb.ReadTd(tx, hash, headerProgress)
	if err1 != nil {
		return err1
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, batch, localTd, headerProgress)
	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, batch: batch})

	var req *headerdownload.HeaderRequest
	var peer []byte
	stopped := false
	timer := time.NewTimer(1 * time.Second) // Check periodically even in the absence of incoming messages
	prevProgress := headerProgress
	for !stopped {
		currentTime := uint64(time.Now().Unix())
		req = cfg.hd.RequestMoreHeaders(currentTime)
		if req != nil {
			peer = cfg.headerReqSend(ctx, req)
			if peer != nil {
				cfg.hd.SentRequest(req, currentTime, 5 /* timeout */)
				log.Debug("Sent request", "height", req.Number)
			}
		}
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && peer != nil && maxRequests > 0 {
			req = cfg.hd.RequestMoreHeaders(currentTime)
			if req != nil {
				peer = cfg.headerReqSend(ctx, req)
				if peer != nil {
					cfg.hd.SentRequest(req, currentTime, 5 /*timeout */)
					log.Debug("Sent request", "height", req.Number)
				}
			}
			maxRequests--
		}

		// Send skeleton request if required
		req = cfg.hd.RequestSkeleton()
		if req != nil {
			peer = cfg.headerReqSend(ctx, req)
			if peer != nil {
				log.Debug("Sent skeleton request", "height", req.Number)
			}
		}
		// Load headers into the database
		if err = cfg.hd.InsertHeaders(headerInserter.FeedHeader); err != nil {
			return err
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
		timer.Stop()
		announces := cfg.hd.GrabAnnounces()
		if len(announces) > 0 {
			cfg.announceNewHashes(ctx, announces)
		}
		if !initialCycle && headerInserter.AnythingDone() {
			// if this is not an initial cycle, we need to react quickly when new headers are coming in
			break
		}
		timer = time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			progress := cfg.hd.Progress()
			logProgressHeaders(logPrefix, prevProgress, progress, batch)
			prevProgress = progress
		case <-timer.C:
			log.Debug("RequestQueueTime (header) ticked")
		case <-cfg.wakeUpChan:
			log.Debug("headerLoop woken up by the incoming request")
		}
		if initialCycle && cfg.hd.InSync() {
			log.Debug("Top seen", "height", cfg.hd.TopSeenHeight())
			break
		}
	}
	if headerInserter.AnythingDone() {
		if err := s.Update(batch, headerInserter.GetHighest()); err != nil {
			return err
		}
	}
	if headerInserter.UnwindPoint() < headerProgress {
		if err := u.UnwindTo(headerInserter.UnwindPoint(), batch, batch); err != nil {
			return fmt.Errorf("%s: failed to unwind to %d: %w", logPrefix, headerInserter.UnwindPoint(), err)
		}
	} else {
		if err := fixCanonicalChain(logPrefix, headerInserter.GetHighest(), headerInserter.GetHighestHash(), batch); err != nil {
			return fmt.Errorf("%s: failed to fix canonical chain: %w", logPrefix, err)
		}
		if !stopped {
			s.Done()
		}
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	log.Info("Processed", "highest", headerInserter.GetHighest())
	if stopped {
		return fmt.Errorf("interrupted")
	}
	stageHeadersGauge.Update(int64(headerInserter.GetHighest()))
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
			log.Error("ancestor nil", "height", ancestorHeight, "hash", ancestorHash)
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
		if err := tx.Commit(); err != nil {
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
