// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages/bodydownload"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

// The number of blocks we should be able to re-org sub-second on commodity hardware.
// See https://hackmd.io/TdJtNs0dS56q-In8h-ShSg
const ShortPoSReorgThresholdBlocks = 10

type HeadersCfg struct {
	db                kv.RwDB
	hd                *headerdownload.HeaderDownload
	bodyDownload      *bodydownload.BodyDownload
	chainConfig       *chain.Config
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) ([64]byte, bool)
	announceNewHashes func(context.Context, []headerdownload.Announce)
	penalize          func(context.Context, []headerdownload.PenaltyItem)
	batchSize         datasize.ByteSize
	noP2PDiscovery    bool
	tmpdir            string

	blockReader   services.FullBlockReader
	blockWriter   *blockio.BlockWriter
	notifications *shards.Notifications

	syncConfig ethconfig.Sync
}

func StageHeadersCfg(
	db kv.RwDB,
	headerDownload *headerdownload.HeaderDownload,
	bodyDownload *bodydownload.BodyDownload,
	chainConfig *chain.Config,
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
) HeadersCfg {
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
		notifications:     notifications,
	}
}

func SpawnStageHeaders(s *StageState, u Unwinder, ctx context.Context, tx kv.RwTx, cfg HeadersCfg, test bool, logger log.Logger) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if s.CurrentSyncCycle.IsInitialCycle {
		if err := cfg.hd.AddHeadersFromSnapshot(tx, cfg.blockReader); err != nil {
			return err
		}
	}
	cfg.hd.Progress()
	return HeadersPOW(s, u, ctx, tx, cfg, test, useExternalTx, logger)

}

// HeadersPOW progresses Headers stage for Proof-of-Work headers
func HeadersPOW(s *StageState, u Unwinder, ctx context.Context, tx kv.RwTx, cfg HeadersCfg, test bool, useExternalTx bool, logger log.Logger) error {
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
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// Check if this is called straight after the unwinds, which means we need to create new canonical markings
	hash, ok, err := cfg.blockReader.CanonicalHash(ctx, tx, startProgress)
	if err != nil {
		return err
	}
	if !ok || hash == (common.Hash{}) { // restore canonical markers after unwind
		headHash := rawdb.ReadHeadHeaderHash(tx)
		if err = fixCanonicalChain(logPrefix, logEvery, startProgress, headHash, tx, cfg.blockReader, logger); err != nil {
			return err
		}
		hash, _, err = cfg.blockReader.CanonicalHash(ctx, tx, startProgress)
		if err != nil {
			return err
		}
	}

	// Allow other stages to run 1 cycle if no network available
	if s.CurrentSyncCycle.IsInitialCycle && cfg.noP2PDiscovery {
		return nil
	}

	logger.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", startProgress, "hash", hash.Hex())

	diaglib.Send(diaglib.HeadersWaitingUpdate{From: startProgress})

	localTd, err := rawdb.ReadTd(tx, hash, startProgress)
	if err != nil {
		return err
	}
	/* TEMP TESTING
	if localTd == nil {
		return fmt.Errorf("localTD is nil: %d, %x", startProgress, hash)
	}*/

	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, startProgress, cfg.blockReader)
	cfg.hd.SetHeaderReader(&ChainReaderImpl{
		config:      cfg.chainConfig,
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

		loopBlockLimit := uint64(cfg.syncConfig.LoopBlockLimit)
		if loopBlockLimit > 0 && cfg.hd.Progress() > startProgress+loopBlockLimit {
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
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				logger.Info("Req/resp stats", "req", stats.Requests, "reqMin", stats.ReqMinBlock, "reqMax", stats.ReqMaxBlock,
					"skel", stats.SkeletonRequests, "skelMin", stats.SkeletonReqMinBlock, "skelMax", stats.SkeletonReqMaxBlock,
					"resp", stats.Responses, "respMin", stats.RespMinBlock, "respMax", stats.RespMaxBlock, "dups", stats.Duplicates, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
				dbg.SaveHeapProfileNearOOM(dbg.SaveHeapWithLogger(&logger), dbg.SaveHeapWithMemStats(&m))
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
		unwindTo := headerInserter.UnwindPoint()
		temporalTx, ok := tx.(kv.TemporalTx)
		if !ok {
			return errors.New("tx is not a temporal tx")
		}
		doms, err := state.NewSharedDomains(temporalTx, logger) //TODO: if remove this line TestBlockchainHeaderchainReorgConsistency failing
		if err != nil {
			return err
		}
		defer doms.Close()

		if err := u.UnwindTo(unwindTo, StagedUnwind, tx); err != nil {
			return err
		}

	}
	if headerInserter.GetHighest() != 0 {
		if !headerInserter.Unwind() {
			if err = fixCanonicalChain(logPrefix, logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx, cfg.blockReader, logger); err != nil {
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
		return common.ErrStopped
	}
	// We do not print the following line if the stage was interrupted

	if s.state.posTransition != nil {
		logger.Info(fmt.Sprintf("[%s] Transitioned to POS", logPrefix), "block", *s.state.posTransition)
	} else {
		headers := headerInserter.GetHighest() - startProgress
		secs := time.Since(startTime).Seconds()

		diaglib.Send(diaglib.HeadersProcessedUpdate{
			Highest:   headerInserter.GetHighest(),
			Age:       time.Unix(int64(headerInserter.GetHighestTimestamp()), 0).Second(),
			Headers:   headers,
			In:        secs,
			BlkPerSec: uint64(float64(headers) / secs),
		})

		logger.Info(fmt.Sprintf("[%s] Processed", logPrefix),
			"highest", headerInserter.GetHighest(), "age", common.PrettyAge(time.Unix(int64(headerInserter.GetHighestTimestamp()), 0)),
			"headers", headers, "in", secs, "blk/sec", uint64(float64(headers)/secs))
	}

	return nil
}

func fixCanonicalChain(logPrefix string, logEvery *time.Ticker, height uint64, hash common.Hash, tx kv.StatelessRwTx, headerReader services.FullBlockReader, logger log.Logger) error {
	if height == 0 {
		return nil
	}
	ancestorHash := hash
	ancestorHeight := height

	var ch common.Hash
	var err error
	for ch, _, err = headerReader.CanonicalHash(context.Background(), tx, ancestorHeight); err == nil && ch != ancestorHash; ch, _, err = headerReader.CanonicalHash(context.Background(), tx, ancestorHeight) {
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
			diaglib.Send(diaglib.HeaderCanonicalMarkerUpdate{AncestorHeight: ancestorHeight, AncestorHash: ancestorHash.String()})
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

func HeadersUnwind(ctx context.Context, u *UnwindState, s *StageState, tx kv.RwTx, cfg HeadersCfg, test bool) (err error) {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBlocks()) // protect from unwind behind files

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
	badBlock := false
	if unwindBlock {
		badBlock = u.Reason.IsBadBlock()
		if badBlock {
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
		for k, v, err = headerCursor.Seek(hexutil.EncodeTs(u.UnwindPoint + 1)); err == nil && k != nil; k, v, err = headerCursor.Next() {
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
	if err := rawdb.TruncateCanonicalHash(tx, u.UnwindPoint+1, badBlock); err != nil {
		return err
	}
	if unwindBlock {
		var maxTd big.Int
		var maxHash common.Hash
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
		}
		/* TODO(yperbasis): Is it safe?
		if err := rawdb.TruncateTd(tx, u.UnwindPoint+1); err != nil {
			return err
		}
		*/
		if maxNum == 0 {
			maxNum = u.UnwindPoint
			var ok bool
			if maxHash, ok, err = cfg.blockReader.CanonicalHash(ctx, tx, maxNum); err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("not found canonical marker: %d", maxNum)
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
		"alloc", common.ByteCount(m.Alloc),
		"sys", common.ByteCount(m.Sys),
		"invalidHeaders", stats.InvalidHeaders,
		"rejectedBadHeaders", stats.RejectedBadHeaders,
	)

	diaglib.Send(diaglib.BlockHeadersUpdate{
		CurrentBlockNumber:  now,
		PreviousBlockNumber: prev,
		Speed:               speed,
		Alloc:               m.Alloc,
		Sys:                 m.Sys,
		InvalidHeaders:      stats.InvalidHeaders,
		RejectedBadHeaders:  stats.RejectedBadHeaders,
	})

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
func (cr ChainReaderImpl) CurrentFinalizedHeader() *types.Header {
	hash := rawdb.ReadForkchoiceFinalized(cr.tx)
	if hash == (common.Hash{}) {
		return nil
	}
	return cr.GetHeaderByHash(hash)
}
func (cr ChainReaderImpl) CurrentSafeHeader() *types.Header {
	hash := rawdb.ReadForkchoiceSafe(cr.tx)
	if hash == (common.Hash{}) {
		return nil
	}

	return cr.GetHeaderByHash(hash)
}
func (cr ChainReaderImpl) GetHeader(hash common.Hash, number uint64) *types.Header {
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
func (cr ChainReaderImpl) GetHeaderByHash(hash common.Hash) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByHash(context.Background(), cr.tx, hash)
		return h
	}
	h, _ := rawdb.ReadHeaderByHash(cr.tx, hash)
	return h
}
func (cr ChainReaderImpl) GetTd(hash common.Hash, number uint64) *big.Int {
	td, err := rawdb.ReadTd(cr.tx, hash, number)
	if err != nil {
		cr.logger.Error("ReadTd failed", "err", err)
		return nil
	}
	return td
}
func (cr ChainReaderImpl) FrozenBlocks() uint64 { return cr.blockReader.FrozenBlocks() }
func (cr ChainReaderImpl) FrozenBorBlocks(align bool) uint64 {
	return cr.blockReader.FrozenBorBlocks(align)
}
func (cr ChainReaderImpl) GetBlock(hash common.Hash, number uint64) *types.Block {
	b, _, _ := cr.blockReader.BlockWithSenders(context.Background(), cr.tx, hash, number)
	return b
}
func (cr ChainReaderImpl) HasBlock(hash common.Hash, number uint64) bool {
	b, _ := cr.blockReader.BodyRlp(context.Background(), cr.tx, hash, number)
	return b != nil
}
