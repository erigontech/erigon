package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

// The number of blocks we should be able to re-org sub-second on commodity hardware.
// See https://hackmd.io/TdJtNs0dS56q-In8h-ShSg
const ShortPoSReorgThresholdBlocks = 10

type HeadersCfg struct {
	db                kv.RwDB
	hd                *headerdownload.HeaderDownload
	bodyDownload      *bodydownload.BodyDownload
	chainConfig       chain.Config
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) ([64]byte, bool)
	announceNewHashes func(context.Context, []headerdownload.Announce)
	penalize          func(context.Context, []headerdownload.PenaltyItem)
	batchSize         datasize.ByteSize
	noP2PDiscovery    bool
	tmpdir            string

	blockReader   services.FullBlockReader
	blockWriter   *blockio.BlockWriter
	forkValidator *engine_helpers.ForkValidator
	notifications *shards.Notifications

	syncConfig     ethconfig.Sync
	loopBreakCheck func(int) bool
}

func StageHeadersCfg(
	db kv.RwDB,
	headerDownload *headerdownload.HeaderDownload,
	bodyDownload *bodydownload.BodyDownload,
	chainConfig chain.Config,
	syncConfig ethconfig.Sync,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) ([64]byte, bool),
	announceNewHashes func(context.Context, []headerdownload.Announce),
	penalize func(context.Context, []headerdownload.PenaltyItem),
	batchSize datasize.ByteSize,
	noP2PDiscovery bool,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	tmpdir string,
	notifications *shards.Notifications,
	forkValidator *engine_helpers.ForkValidator,
	loopBreakCheck func(int) bool) HeadersCfg {
	return HeadersCfg{
		db:                db,
		hd:                headerDownload,
		bodyDownload:      bodyDownload,
		chainConfig:       chainConfig,
		syncConfig:        syncConfig,
		headerReqSend:     headerReqSend,
		announceNewHashes: announceNewHashes,
		penalize:          penalize,
		batchSize:         batchSize,
		tmpdir:            tmpdir,
		noP2PDiscovery:    noP2PDiscovery,
		blockReader:       blockReader,
		blockWriter:       blockWriter,
		forkValidator:     forkValidator,
		notifications:     notifications,
		loopBreakCheck:    loopBreakCheck,
	}
}

func SpawnStageHeaders(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Returns true to allow the stage to stop rather than wait indefinitely
	logger log.Logger,
) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if initialCycle && cfg.blockReader.FreezingCfg().Enabled {
		if err := cfg.hd.AddHeadersFromSnapshot(tx, cfg.blockReader); err != nil {
			return err
		}
	}

	return HeadersPOW(s, u, ctx, tx, cfg, initialCycle, test, useExternalTx, logger)

}

// HeadersPOW progresses Headers stage for Proof-of-Work headers
func HeadersPOW(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Returns true to allow the stage to stop rather than wait indefinitely
	useExternalTx bool,
	logger log.Logger,
) error {
	var err error

	startTime := time.Now()

	if err = cfg.hd.ReadProgressFromDb(tx); err != nil {
		return err
	}
	cfg.hd.SetPOSSync(false)
	cfg.hd.SetFetchingNew(true)
	defer cfg.hd.SetFetchingNew(false)
	startProgress := cfg.hd.Progress()
	logPrefix := s.LogPrefix()

	// Check if this is called straight after the unwinds, which means we need to create new canonical markings
	hash, err := cfg.blockReader.CanonicalHash(ctx, tx, startProgress)
	if err != nil {
		return err
	}
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	if hash == (libcommon.Hash{}) {
		headHash := rawdb.ReadHeadHeaderHash(tx)
		if err = fixCanonicalChain(logPrefix, logEvery, startProgress, headHash, tx, cfg.blockReader, logger); err != nil {
			return err
		}
		if !useExternalTx {
			if err = tx.Commit(); err != nil {
				return err
			}
		}
		return nil
	}

	// Allow other stages to run 1 cycle if no network available
	if initialCycle && cfg.noP2PDiscovery {
		return nil
	}

	logger.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", startProgress)

	localTd, err := rawdb.ReadTd(tx, hash, startProgress)
	if err != nil {
		return err
	}
	/* TEMP TESTING
	if localTd == nil {
		return fmt.Errorf("localTD is nil: %d, %x", startProgress, hash)
	}
	TEMP TESTING */
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, startProgress, cfg.blockReader)
	cfg.hd.SetHeaderReader(&ChainReaderImpl{
		config:      &cfg.chainConfig,
		tx:          tx,
		blockReader: cfg.blockReader,
		logger:      logger,
	})

	stopped := false
	var noProgressCounter uint = 0
	prevProgress := startProgress
	var wasProgress bool
	var lastSkeletonTime time.Time
	var peer [64]byte
	var sentToPeer bool
Loop:
	for !stopped {

		transitionedToPoS, err := rawdb.Transitioned(tx, startProgress, cfg.chainConfig.TerminalTotalDifficulty)
		if err != nil {
			return err
		}
		if transitionedToPoS {
			if err := s.Update(tx, startProgress); err != nil {
				return err
			}
			s.state.posTransition = &startProgress
			break
		}

		sentToPeer = false
		currentTime := time.Now()
		req, penalties := cfg.hd.RequestMoreHeaders(currentTime)
		if req != nil {
			peer, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				logger.Debug(fmt.Sprintf("[%s] Requested header", logPrefix), "from", req.Number, "length", req.Length)
				cfg.hd.UpdateStats(req, false /* skeleton */, peer)
				cfg.hd.UpdateRetryTime(req, currentTime, 5*time.Second /* timeout */)
			}
		}
		if len(penalties) > 0 {
			cfg.penalize(ctx, penalties)
		}
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && sentToPeer && maxRequests > 0 {
			req, penalties = cfg.hd.RequestMoreHeaders(currentTime)
			if req != nil {
				peer, sentToPeer = cfg.headerReqSend(ctx, req)
				if sentToPeer {
					cfg.hd.UpdateStats(req, false /* skeleton */, peer)
					cfg.hd.UpdateRetryTime(req, currentTime, 5*time.Second /* timeout */)
				}
			}
			if len(penalties) > 0 {
				cfg.penalize(ctx, penalties)
			}
			maxRequests--
		}

		// Send skeleton request if required
		if time.Since(lastSkeletonTime) > 1*time.Second {
			req = cfg.hd.RequestSkeleton()
			if req != nil {
				peer, sentToPeer = cfg.headerReqSend(ctx, req)
				if sentToPeer {
					logger.Debug(fmt.Sprintf("[%s] Requested skeleton", logPrefix), "from", req.Number, "length", req.Length)
					cfg.hd.UpdateStats(req, true /* skeleton */, peer)
					lastSkeletonTime = time.Now()
				}
			}
		}
		// Load headers into the database
		inSync, err := cfg.hd.InsertHeaders(headerInserter.NewFeedHeaderFunc(tx, cfg.blockReader), cfg.syncConfig.LoopBlockLimit, cfg.chainConfig.TerminalTotalDifficulty, logPrefix, logEvery.C, uint64(currentTime.Unix()))

		if err != nil {
			return err
		}

		if headerInserter.BestHeaderChanged() { // We do not break unless there best header changed
			noProgressCounter = 0
			wasProgress = true
			// if this is initial cycle, we want to make sure we insert all known headers (inSync)
			if inSync {
				break
			}
		}

		if cfg.syncConfig.LoopBlockLimit > 0 {
			if bodyProgress, err := stages.GetStageProgress(tx, stages.Bodies); err == nil {
				if cfg.hd.Progress() > bodyProgress && cfg.hd.Progress()-bodyProgress > uint64(cfg.syncConfig.LoopBlockLimit*2) {
					break
				}
			}
		}

		if cfg.loopBreakCheck != nil && cfg.loopBreakCheck(int(cfg.hd.Progress()-startProgress)) {
			break
		}

		if test {
			announces := cfg.hd.GrabAnnounces()
			if len(announces) > 0 {
				cfg.announceNewHashes(ctx, announces)
			}

			break
		}

		timer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			progress := cfg.hd.Progress()
			stats := cfg.hd.ExtractStats()
			logProgressHeaders(logPrefix, prevProgress, progress, stats, logger)
			if prevProgress == progress {
				noProgressCounter++
			} else {
				noProgressCounter = 0 // Reset, there was progress
			}
			if noProgressCounter >= 5 {
				logger.Info("Req/resp stats", "req", stats.Requests, "reqMin", stats.ReqMinBlock, "reqMax", stats.ReqMaxBlock,
					"skel", stats.SkeletonRequests, "skelMin", stats.SkeletonReqMinBlock, "skelMax", stats.SkeletonReqMaxBlock,
					"resp", stats.Responses, "respMin", stats.RespMinBlock, "respMax", stats.RespMaxBlock, "dups", stats.Duplicates)
				cfg.hd.LogAnchorState()
				if wasProgress {
					logger.Warn("Looks like chain is not progressing, moving to the next stage")
					break Loop
				}
			}
			prevProgress = progress
		case <-timer.C:
			logger.Trace("RequestQueueTime (header) ticked")
		case <-cfg.hd.DeliveryNotify:
			logger.Trace("headerLoop woken up by the incoming request")
		}
		timer.Stop()
	}
	if headerInserter.Unwind() {
		u.UnwindTo(headerInserter.UnwindPoint(), StagedUnwind)
	}
	if headerInserter.GetHighest() != 0 {
		if !headerInserter.Unwind() {
			if err := fixCanonicalChain(logPrefix, logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx, cfg.blockReader, logger); err != nil {
				return fmt.Errorf("fix canonical chain: %w", err)
			}
		}
		if err = rawdb.WriteHeadHeaderHash(tx, headerInserter.GetHighestHash()); err != nil {
			return fmt.Errorf("[%s] marking head header hash as %x: %w", logPrefix, headerInserter.GetHighestHash(), err)
		}
		if err = s.Update(tx, headerInserter.GetHighest()); err != nil {
			return fmt.Errorf("[%s] saving Headers progress: %w", logPrefix, err)
		}
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	if stopped {
		return libcommon.ErrStopped
	}
	// We do not print the following line if the stage was interrupted

	if s.state.posTransition != nil {
		logger.Info(fmt.Sprintf("[%s] Transitioned to POS", logPrefix), "block", *s.state.posTransition)
	} else {
		headers := headerInserter.GetHighest() - startProgress
		secs := time.Since(startTime).Seconds()
		logger.Info(fmt.Sprintf("[%s] Processed", logPrefix),
			"highest", headerInserter.GetHighest(), "age", common.PrettyAge(time.Unix(int64(headerInserter.GetHighestTimestamp()), 0)),
			"headers", headers, "in", secs, "blk/sec", uint64(float64(headers)/secs))
	}

	return nil
}

func fixCanonicalChain(logPrefix string, logEvery *time.Ticker, height uint64, hash libcommon.Hash, tx kv.StatelessRwTx, headerReader services.FullBlockReader, logger log.Logger) error {
	if height == 0 {
		return nil
	}
	ancestorHash := hash
	ancestorHeight := height

	var ch libcommon.Hash
	var err error
	for ch, err = headerReader.CanonicalHash(context.Background(), tx, ancestorHeight); err == nil && ch != ancestorHash; ch, err = headerReader.CanonicalHash(context.Background(), tx, ancestorHeight) {
		if err = rawdb.WriteCanonicalHash(tx, ancestorHash, ancestorHeight); err != nil {
			return fmt.Errorf("marking canonical header %d %x: %w", ancestorHeight, ancestorHash, err)
		}

		ancestor, err := headerReader.Header(context.Background(), tx, ancestorHash, ancestorHeight)
		if err != nil {
			return err
		}
		if ancestor == nil {
			return fmt.Errorf("ancestor is nil. height %d, hash %x", ancestorHeight, ancestorHash)
		}

		select {
		case <-logEvery.C:
			logger.Info(fmt.Sprintf("[%s] write canonical markers", logPrefix), "ancestor", ancestorHeight, "hash", ancestorHash)
		default:
		}
		ancestorHash = ancestor.ParentHash
		ancestorHeight--
	}
	if err != nil {
		return fmt.Errorf("reading canonical hash for %d: %w", ancestorHeight, err)
	}

	return nil
}

func HeadersUnwind(u *UnwindState, s *StageState, tx kv.RwTx, cfg HeadersCfg, test bool) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	// Delete canonical hashes that are being unwound
	unwindBlock := (u.Reason.Block != nil)
	if unwindBlock {
		if u.Reason.IsBadBlock() {
			cfg.hd.ReportBadHeader(*u.Reason.Block)
		}

		cfg.hd.UnlinkHeader(*u.Reason.Block)

		// Mark all descendants of bad block as bad too
		headerCursor, cErr := tx.Cursor(kv.Headers)
		if cErr != nil {
			return cErr
		}
		defer headerCursor.Close()
		var k, v []byte
		for k, v, err = headerCursor.Seek(hexutility.EncodeTs(u.UnwindPoint + 1)); err == nil && k != nil; k, v, err = headerCursor.Next() {
			var h types.Header
			if err = rlp.DecodeBytes(v, &h); err != nil {
				return err
			}
			if cfg.hd.IsBadHeader(h.ParentHash) {
				cfg.hd.ReportBadHeader(h.Hash())
			}
		}
		if err != nil {
			return fmt.Errorf("iterate over headers to mark bad headers: %w", err)
		}
	}
	if err := rawdb.TruncateCanonicalHash(tx, u.UnwindPoint+1, unwindBlock); err != nil {
		return err
	}
	if unwindBlock {
		var maxTd big.Int
		var maxHash libcommon.Hash
		var maxNum uint64 = 0

		if test { // If we are not in the test, we can do searching for the heaviest chain in the next cycle
			// Find header with biggest TD
			tdCursor, cErr := tx.Cursor(kv.HeaderTD)
			if cErr != nil {
				return cErr
			}
			defer tdCursor.Close()
			var k, v []byte
			k, v, err = tdCursor.Last()
			if err != nil {
				return err
			}
			for ; err == nil && k != nil; k, v, err = tdCursor.Prev() {
				if len(k) != 40 {
					return fmt.Errorf("key in TD table has to be 40 bytes long: %x", k)
				}
				var hash libcommon.Hash
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
		}
		/* TODO(yperbasis): Is it safe?
		if err := rawdb.TruncateTd(tx, u.UnwindPoint+1); err != nil {
			return err
		}
		*/
		if maxNum == 0 {
			maxNum = u.UnwindPoint
			if maxHash, err = rawdb.ReadCanonicalHash(tx, maxNum); err != nil {
				return err
			}
		}
		if err = rawdb.WriteHeadHeaderHash(tx, maxHash); err != nil {
			return err
		}
		if err = u.Done(tx); err != nil {
			return err
		}
		if err = s.Update(tx, maxNum); err != nil {
			return err
		}
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func logProgressHeaders(
	logPrefix string,
	prev uint64,
	now uint64,
	stats headerdownload.Stats,
	logger log.Logger,
) uint64 {
	speed := float64(now-prev) / float64(logInterval/time.Second)

	var message string
	if speed == 0 {
		message = "No block headers to write in this log period"
	} else {
		message = "Wrote block headers"
	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info(fmt.Sprintf("[%s] %s", logPrefix, message),
		"number", now,
		"blk/second", speed,
		"alloc", libcommon.ByteCount(m.Alloc),
		"sys", libcommon.ByteCount(m.Sys),
		"invalidHeaders", stats.InvalidHeaders,
		"rejectedBadHeaders", stats.RejectedBadHeaders,
	)

	return now
}

type ChainReaderImpl struct {
	config      *chain.Config
	tx          kv.Tx
	blockReader services.FullBlockReader
	logger      log.Logger
}

func NewChainReaderImpl(config *chain.Config, tx kv.Tx, blockReader services.FullBlockReader, logger log.Logger) *ChainReaderImpl {
	return &ChainReaderImpl{config, tx, blockReader, logger}
}

func (cr ChainReaderImpl) Config() *chain.Config        { return cr.config }
func (cr ChainReaderImpl) CurrentHeader() *types.Header { panic("") }
func (cr ChainReaderImpl) GetHeader(hash libcommon.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr ChainReaderImpl) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr ChainReaderImpl) GetHeaderByHash(hash libcommon.Hash) *types.Header {
	if cr.blockReader != nil {
		number := rawdb.ReadHeaderNumber(cr.tx, hash)
		if number == nil {
			return nil
		}
		return cr.GetHeader(hash, *number)
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr ChainReaderImpl) GetTd(hash libcommon.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		cr.logger.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}
func (cr ChainReaderImpl) FrozenBlocks() uint64 {
	return cr.blockReader.FrozenBlocks()
}
func (cr ChainReaderImpl) GetBlock(hash libcommon.Hash, number uint64) *types.Block {
	b, _, _ := cr.blockReader.BlockWithSenders(context.Background(), cr.tx, hash, number)
	return b
}
func (cr ChainReaderImpl) HasBlock(hash libcommon.Hash, number uint64) bool {
	b, _ := cr.blockReader.BodyRlp(context.Background(), cr.tx, hash, number)
	return b != nil
}
func (cr ChainReaderImpl) BorEventsByBlock(hash libcommon.Hash, number uint64) []rlp.RawValue {
	events, err := cr.blockReader.EventsByBlock(context.Background(), cr.tx, hash, number)
	if err != nil {
		cr.logger.Error("BorEventsByBlock failed", "err", err)
		return nil
	}
	return events
}
func (cr ChainReaderImpl) BorStartEventID(hash libcommon.Hash, blockNum uint64) uint64 {
	id, err := cr.blockReader.BorStartEventID(context.Background(), cr.tx, hash, blockNum)
	if err != nil {
		cr.logger.Error("BorEventsByBlock failed", "err", err)
		return 0
	}
	return id
}
func (cr ChainReaderImpl) BorSpan(spanId uint64) []byte {
	span, err := cr.blockReader.Span(context.Background(), cr.tx, spanId)
	if err != nil {
		cr.logger.Error("[staged sync] BorSpan failed", "err", err)
		return nil
	}
	return span
}
