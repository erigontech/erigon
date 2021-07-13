package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

var stageHeadersGauge = metrics.NewRegisteredGauge("stage/headers", nil)

type HeadersCfg struct {
	db                ethdb.RwKV
	hd                *headerdownload.HeaderDownload
	chainConfig       params.ChainConfig
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) []byte
	announceNewHashes func(context.Context, []headerdownload.Announce)
	penalize          func(context.Context, []headerdownload.PenaltyItem)
	batchSize         datasize.ByteSize
}

func StageHeadersCfg(
	db ethdb.RwKV,
	headerDownload *headerdownload.HeaderDownload,
	chainConfig params.ChainConfig,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) []byte,
	announceNewHashes func(context.Context, []headerdownload.Announce),
	penalize func(context.Context, []headerdownload.PenaltyItem),
	batchSize datasize.ByteSize,
) HeadersCfg {
	return HeadersCfg{
		db:                db,
		hd:                headerDownload,
		chainConfig:       chainConfig,
		headerReqSend:     headerReqSend,
		announceNewHashes: announceNewHashes,
		penalize:          penalize,
		batchSize:         batchSize,
	}
}

// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx ethdb.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
) error {
	var headerProgress uint64
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if err = cfg.hd.ReadProgressFromDb(tx); err != nil {
		return err
	}
	cfg.hd.SetFetching(true)
	defer cfg.hd.SetFetching(false)
	headerProgress = cfg.hd.Progress()
	logPrefix := s.LogPrefix()
	// Check if this is called straight after the unwinds, which means we need to create new canonical markings
	hash, err := rawdb.ReadCanonicalHash(tx, headerProgress)
	if err != nil {
		return err
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	if hash == (common.Hash{}) {
		headHash := rawdb.ReadHeadHeaderHash(tx)
		if err = fixCanonicalChain(logPrefix, logEvery, headerProgress, headHash, tx); err != nil {
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

	log.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", headerProgress)

	localTd, err := rawdb.ReadTd(tx, hash, headerProgress)
	if err != nil {
		return err
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, headerProgress)
	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx})

	var peer []byte
	stopped := false
	prevProgress := headerProgress
	for !stopped {
		currentTime := uint64(time.Now().Unix())
		req, penalties := cfg.hd.RequestMoreHeaders(currentTime)
		if req != nil {
			peer = cfg.headerReqSend(ctx, req)
			if peer != nil {
				cfg.hd.SentRequest(req, currentTime, 5 /* timeout */)
				log.Debug("Sent request", "height", req.Number)
			}
		}
		cfg.penalize(ctx, penalties)
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && peer != nil && maxRequests > 0 {
			req, penalties = cfg.hd.RequestMoreHeaders(currentTime)
			if req != nil {
				peer = cfg.headerReqSend(ctx, req)
				if peer != nil {
					cfg.hd.SentRequest(req, currentTime, 5 /*timeout */)
					log.Debug("Sent request", "height", req.Number)
				}
			}
			cfg.penalize(ctx, penalties)
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
		var inSync bool
		if inSync, err = cfg.hd.InsertHeaders(headerInserter.FeedHeaderFunc(tx), logPrefix, logEvery.C); err != nil {
			return err
		}
		announces := cfg.hd.GrabAnnounces()
		if len(announces) > 0 {
			cfg.announceNewHashes(ctx, announces)
		}
		if headerInserter.BestHeaderChanged() { // We do not break unless there best header changed
			if !initialCycle {
				// if this is not an initial cycle, we need to react quickly when new headers are coming in
				break
			}
			// if this is initial cycle, we want to make sure we insert all known headers (inSync)
			if inSync {
				break
			}
		}
		if test {
			break
		}
		timer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			progress := cfg.hd.Progress()
			logProgressHeaders(logPrefix, prevProgress, progress)
			prevProgress = progress
		case <-timer.C:
			log.Trace("RequestQueueTime (header) ticked")
		case <-cfg.hd.DeliveryNotify:
			log.Debug("headerLoop woken up by the incoming request")
		}
		timer.Stop()
	}
	if headerInserter.Unwind() {
		if err := u.UnwindTo(headerInserter.UnwindPoint(), tx, common.Hash{}); err != nil {
			return fmt.Errorf("%s: failed to unwind to %d: %w", logPrefix, headerInserter.UnwindPoint(), err)
		}
	} else if headerInserter.GetHighest() != 0 {
		if err := fixCanonicalChain(logPrefix, logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx); err != nil {
			return fmt.Errorf("%s: failed to fix canonical chain: %w", logPrefix, err)
		}
	}
	s.Done()
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	if stopped {
		return common.ErrStopped
	}
	// We do not print the followin line if the stage was interrupted
	log.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest inserted", headerInserter.GetHighest(), "age", common.PrettyAge(time.Unix(int64(headerInserter.GetHighestTimestamp()), 0)))
	stageHeadersGauge.Update(int64(cfg.hd.Progress()))
	return nil
}

func fixCanonicalChain(logPrefix string, logEvery *time.Ticker, height uint64, hash common.Hash, tx ethdb.StatelessRwTx) error {
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
			return fmt.Errorf("ancestor is nil. height %d, hash %x", ancestorHeight, ancestorHash)
		}

		select {
		case <-logEvery.C:
			log.Info("fix canonical", "ancestor", ancestorHeight, "hash", ancestorHash)
		default:
		}
		ancestorHash = ancestor.ParentHash
		ancestorHeight--
	}
	if err != nil {
		return fmt.Errorf("[%s] reading canonical hash for %d: %w", logPrefix, ancestorHeight, err)
	}
	return nil
}

func HeadersUnwind(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg HeadersCfg) error {
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	// Delete canonical hashes that are being unwound
	var headerProgress uint64
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	badBlock := u.BadBlock != (common.Hash{})
	for blockHeight := headerProgress; blockHeight > u.UnwindPoint; blockHeight-- {
		if badBlock {
			var hash common.Hash
			if hash, err = rawdb.ReadCanonicalHash(tx, blockHeight); err != nil {
				return err
			}
			cfg.hd.ReportBadHeader(hash)
		}
		if err = rawdb.DeleteCanonicalHash(tx, blockHeight); err != nil {
			return err
		}
	}
	if u.BadBlock != (common.Hash{}) {
		cfg.hd.ReportBadHeader(u.BadBlock)
		// Find header with biggest TD
		tdCursor, cErr := tx.Cursor(dbutils.HeaderTDBucket)
		if cErr != nil {
			return cErr
		}
		defer tdCursor.Close()
		var k, v []byte
		k, v, err = tdCursor.Last()
		if err != nil {
			return err
		}
		var maxTd big.Int
		var maxHash common.Hash
		var maxNum uint64 = 0
		for ; err == nil && k != nil; k, v, err = tdCursor.Prev() {
			if len(k) != 40 {
				return fmt.Errorf("key in TD table has to be 40 bytes long: %x", k)
			}
			var hash common.Hash
			copy(hash[:], k[8:])
			if cfg.hd.IsBadHeader(hash) {
				continue
			}
			var td big.Int
			if err = rlp.DecodeBytes(v, &td); err != nil {
				return err
			}
			if td.Cmp(&maxTd) > 0 {
				maxTd.Set(&td)
				copy(maxHash[:], k[8:])
				maxNum = binary.BigEndian.Uint64(k[:8])
			}
		}
		if err != nil {
			return err
		}
		if maxNum == 0 {
			// Read genesis hash
			if maxHash, err = rawdb.ReadCanonicalHash(tx, 0); err != nil {
				return err
			}
		}
		if err = rawdb.WriteHeadHeaderHash(tx, maxHash); err != nil {
			return err
		}
		if err = s.DoneAndUpdate(tx, maxNum); err != nil {
			return err
		}
	} else if err = u.Skip(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func logProgressHeaders(logPrefix string, prev, now uint64) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info(fmt.Sprintf("[%s] Wrote block headers", logPrefix),
		"number", now,
		"blk/second", speed,
		"alloc", common.StorageSize(m.Alloc),
		"sys", common.StorageSize(m.Sys))

	return now
}

type chainReader struct {
	config *params.ChainConfig
	tx     ethdb.RwTx
}

func (cr chainReader) Config() *params.ChainConfig  { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header {
	return rawdb.ReadHeaderByNumber(cr.tx, number)
}
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header {
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}

type epochReader struct {
	tx ethdb.RwTx
}

func (cr epochReader) GetEpoch(hash common.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadEpoch(cr.tx, number, hash)
}
func (cr epochReader) PutEpoch(hash common.Hash, number uint64, proof []byte) error {
	return rawdb.WriteEpoch(cr.tx, number, hash, proof)
}
