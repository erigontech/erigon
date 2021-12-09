package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"

	"github.com/ledgerwatch/log/v3"
)

type HeadersCfg struct {
	db                kv.RwDB
	hd                *headerdownload.HeaderDownload
	statusCh          chan privateapi.ExecutionStatus
	chainConfig       params.ChainConfig
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) (enode.ID, bool)
	announceNewHashes func(context.Context, []headerdownload.Announce)
	penalize          func(context.Context, []headerdownload.PenaltyItem)
	batchSize         datasize.ByteSize
	noP2PDiscovery    bool
	tmpdir            string
	reverseDownloadCh chan types.Header
	waitingPosHeaders *bool
	snapshots         *snapshotsync.AllSnapshots
	blockReader       interfaces.FullBlockReader
}

func StageHeadersCfg(
	db kv.RwDB,
	headerDownload *headerdownload.HeaderDownload,
	statusCh chan privateapi.ExecutionStatus,
	chainConfig params.ChainConfig,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) (enode.ID, bool),
	announceNewHashes func(context.Context, []headerdownload.Announce),
	penalize func(context.Context, []headerdownload.PenaltyItem),
	batchSize datasize.ByteSize,
	noP2PDiscovery bool,
	reverseDownloadCh chan types.Header,
	waitingPosHeaders *bool,
	snapshots *snapshotsync.AllSnapshots,
	blockReader interfaces.FullBlockReader,
	tmpdir string,
) HeadersCfg {
	return HeadersCfg{
		db:                db,
		hd:                headerDownload,
		statusCh:          statusCh,
		chainConfig:       chainConfig,
		headerReqSend:     headerReqSend,
		announceNewHashes: announceNewHashes,
		penalize:          penalize,
		batchSize:         batchSize,
		noP2PDiscovery:    noP2PDiscovery,
		reverseDownloadCh: reverseDownloadCh,
		waitingPosHeaders: waitingPosHeaders,
		snapshots:         snapshots,
		blockReader:       blockReader,
	}
}

func SpawnStageHeaders(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
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

	var blockNumber uint64

	if s == nil {
		blockNumber = 0
	} else {
		blockNumber = s.BlockNumber
	}

	isTrans, err := rawdb.Transitioned(tx, blockNumber, cfg.chainConfig.TerminalTotalDifficulty)

	if err != nil {
		return err
	}

	if isTrans {
		return HeadersDownward(s, u, ctx, tx, cfg, initialCycle, test, useExternalTx)
	} else {
		return HeadersForward(s, u, ctx, tx, cfg, initialCycle, test, useExternalTx)
	}
}

// HeadersDownward progresses Headers stage in the downward direction
func HeadersDownward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
	useExternalTx bool,
) error {
	*cfg.waitingPosHeaders = true
	// Waiting for the beacon chain
	log.Info("Waiting for payloads...")
	header := <-cfg.reverseDownloadCh
	*cfg.waitingPosHeaders = false

	headerNumber := header.Number.Uint64()

	blockHash, err := rawdb.ReadCanonicalHash(tx, headerNumber)
	if err != nil {
		return err
	}

	// Do we need to unwind? (TODO)
	if s.BlockNumber >= headerNumber && header.Hash() != blockHash {
		u.UnwindTo(headerNumber-1, common.Hash{})
		cfg.statusCh <- privateapi.ExecutionStatus{
			HeadHash: header.ParentHash,
			Status:   privateapi.Syncing,
		}
		return nil
	}

	// Write current payload
	rawdb.WriteHeader(tx, &header)
	if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
		return err
	}
	// if we have the parent then we can move on with the stagedsync
	parent, err := rawdb.ReadHeaderByHash(tx, header.ParentHash)
	if err != nil {
		return err
	}
	if parent != nil && parent.Hash() == header.ParentHash {
		if err := s.Update(tx, header.Number.Uint64()); err != nil {
			return err
		}
		// For the sake of simplicity we can just assume it will be valid for now. (TODO: move to execution stage)
		cfg.statusCh <- privateapi.ExecutionStatus{
			Status:   privateapi.Valid,
			HeadHash: header.Hash(),
		}
		return tx.Commit()
	}
	cfg.hd.SetBackwards(true)
	if err = cfg.hd.ReadProgressFromDb(tx); err != nil {
		return err
	}
	cfg.hd.SetProcessed(header.Number.Uint64())
	cfg.hd.SetExpectedHash(header.ParentHash)
	cfg.hd.SetFetching(true)
	defer cfg.hd.SetFetching(false)
	headerProgress := cfg.hd.Progress()
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
		if err = fixCanonicalChain(logPrefix, logEvery, headerProgress, headHash, tx, cfg.blockReader); err != nil {
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

	cfg.statusCh <- privateapi.ExecutionStatus{
		Status:   privateapi.Syncing,
		HeadHash: rawdb.ReadHeadBlockHash(tx),
	}
	log.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", header.Number.Uint64())

	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})

	var sentToPeer bool
	stopped := false
	prevProgress := header.Number.Uint64()
	go func() {
		for {
			timer := time.NewTimer(1 * time.Second)
			select {
			case <-ctx.Done():
				stopped = true
			case <-logEvery.C:
				diff := prevProgress - cfg.hd.Progress()
				if cfg.hd.Progress() <= prevProgress {
					log.Info("Wrote Block Headers backwards", "from", header.Number.Uint64(),
						"now", cfg.hd.Progress(), "blk/sec", float64(diff)/float64(logInterval/time.Second))
					prevProgress = cfg.hd.Progress()
				}
			}
			timer.Stop()
		}
	}()
	headerCollector := etl.NewCollector(logPrefix, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	canonicalHeadersCollector := etl.NewCollector(logPrefix, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	for !stopped {
		currentTime := uint64(time.Now().Unix())
		req := cfg.hd.RequestMoreHeadersForPOS(currentTime)
		if req != nil {
			_, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				// cfg.hd.SentRequest(req, currentTime, 5 /* timeout */)
				log.Trace("Sent request", "height", req.Number)
			}
		}

		req = cfg.hd.RequestMoreHeadersForPOS(currentTime)

		if req != nil {
			_, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				cfg.hd.SentRequest(req, currentTime, 5 /*timeout */)
				log.Trace("Sent request", "height", req.Number)
			}
		}

		// Load headers into the database
		var inSync bool
		if inSync, err = cfg.hd.InsertHeadersBackwards(tx, headerCollector, canonicalHeadersCollector, logPrefix, logEvery.C); err != nil {
			return err
		}
		announces := cfg.hd.GrabAnnounces()
		if len(announces) > 0 {
			cfg.announceNewHashes(ctx, announces)
		}
		if inSync { // We do not break unless there best header changed
			s.Update(tx, header.Number.Uint64())
			break
		}
	}
	if err := headerCollector.Load(tx, kv.Headers, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	if err := canonicalHeadersCollector.Load(tx, kv.Headers, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	// Downward sync if we need to process more (TODO)
	return tx.Commit()
}

// HeadersForward progresses Headers stage in the forward direction
func HeadersForward(
	s *StageState,
	u Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
	test bool, // Set to true in tests, allows the stage to fail rather than wait indefinitely
	useExternalTx bool,
) error {
	var headerProgress uint64

	if cfg.snapshots != nil {
		if !cfg.snapshots.AllSegmentsAvailable() {
			// wait for Downloader service to download all expected snapshots
			logEvery := time.NewTicker(logInterval)
			defer logEvery.Stop()
			for {
				headers, bodies, txs, err := cfg.snapshots.SegmentsAvailability()
				if err != nil {
					return err
				}
				expect := cfg.snapshots.ChainSnapshotConfig().ExpectBlocks
				if headers >= expect && bodies >= expect && txs >= expect {
					if err := cfg.snapshots.ReopenSegments(); err != nil {
						return err
					}
					if expect > cfg.snapshots.BlocksAvailable() {
						return fmt.Errorf("not enough snapshots available: %d > %d", expect, cfg.snapshots.BlocksAvailable())
					}
					cfg.snapshots.SetAllSegmentsAvailable(true)

					break
				}
				log.Info(fmt.Sprintf("[%s] Waiting for snapshots up to block %d...", s.LogPrefix(), expect), "headers", headers, "bodies", bodies, "txs", txs)
				time.Sleep(10 * time.Second)

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[%s] Waiting for snapshots up to block %d...", s.LogPrefix(), expect), "headers", headers, "bodies", bodies, "txs", txs)
				default:
				}
			}
		}

		if !cfg.snapshots.AllIdxAvailable() {
			if !cfg.snapshots.AllSegmentsAvailable() {
				return fmt.Errorf("not all snapshot segments are available")
			}

			// wait for Downloader service to download all expected snapshots
			logEvery := time.NewTicker(logInterval)
			defer logEvery.Stop()
			headers, bodies, txs, err := cfg.snapshots.IdxAvailability()
			if err != nil {
				return err
			}
			expect := cfg.snapshots.ChainSnapshotConfig().ExpectBlocks
			if headers < expect || bodies < expect || txs < expect {
				chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
				if err := cfg.snapshots.BuildIndices(ctx, *chainID); err != nil {
					return err
				}
			}

			if err := cfg.snapshots.ReopenIndices(); err != nil {
				return err
			}
			if expect > cfg.snapshots.IndicesAvailable() {
				return fmt.Errorf("not enough snapshots available: %d > %d", expect, cfg.snapshots.BlocksAvailable())
			}
			cfg.snapshots.SetAllIdxAvailable(true)
		}

		c, _ := tx.Cursor(kv.HeaderTD)
		count, _ := c.Count()
		if count == 0 || count == 1 { // genesis does write 1 record
			logEvery := time.NewTicker(logInterval)
			defer logEvery.Stop()

			tx.ClearBucket(kv.HeaderTD)
			var lastHeader *types.Header
			//total  difficulty write
			td := big.NewInt(0)
			if err := snapshotsync.ForEachHeader(cfg.snapshots, func(header *types.Header) error {
				td.Add(td, header.Difficulty)
				/*
					if header.Eip3675 {
						return nil
					}

					if td.Cmp(cfg.terminalTotalDifficulty) > 0 {
						return rawdb.MarkTransition(tx, blockNum)
					}
				*/
				// TODO: append
				rawdb.WriteTd(tx, header.Hash(), header.Number.Uint64(), td)
				lastHeader = header
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[%s] Writing total difficulty index for snapshots", s.LogPrefix()), "block_num", header.Number.Uint64())
				default:
				}
				return nil
			}); err != nil {
				return err
			}
			tx.ClearBucket(kv.HeaderCanonical)
			if err := fixCanonicalChain(s.LogPrefix(), logEvery, lastHeader.Number.Uint64(), lastHeader.Hash(), tx, cfg.blockReader); err != nil {
				return err
			}
		}

		if s.BlockNumber < cfg.snapshots.BlocksAvailable() {
			if err := cfg.hd.AddHeaderFromSnapshot(cfg.snapshots.BlocksAvailable(), cfg.blockReader); err != nil {
				return err
			}
			if err := s.Update(tx, cfg.snapshots.BlocksAvailable()); err != nil {
				return err
			}
			s.BlockNumber = cfg.snapshots.BlocksAvailable()
		}
	}

	var err error

	if !useExternalTx {
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
		if err = fixCanonicalChain(logPrefix, logEvery, headerProgress, headHash, tx, cfg.blockReader); err != nil {
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

	log.Info(fmt.Sprintf("[%s] Waiting for headers...", logPrefix), "from", headerProgress)

	localTd, err := rawdb.ReadTd(tx, hash, headerProgress)
	if err != nil {
		return err
	}
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, headerProgress)
	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})

	var sentToPeer bool
	stopped := false
	prevProgress := headerProgress
	go func() {
		for {
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
				log.Trace("headerLoop woken up by the incoming request")
			}
			timer.Stop()
		}
	}()
	for !stopped {

		isTrans, err := rawdb.Transitioned(tx, headerProgress, cfg.chainConfig.TerminalTotalDifficulty)
		if err != nil {
			return err
		}

		if isTrans {
			if err := s.Update(tx, headerProgress); err != nil {
				return err
			}
			break
		}
		currentTime := uint64(time.Now().Unix())
		req, penalties := cfg.hd.RequestMoreHeaders(currentTime)
		if req != nil {
			_, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				cfg.hd.SentRequest(req, currentTime, 5 /* timeout */)
				log.Trace("Sent request", "height", req.Number)
			}
		}
		cfg.penalize(ctx, penalties)
		maxRequests := 64 // Limit number of requests sent per round to let some headers to be inserted into the database
		for req != nil && sentToPeer && maxRequests > 0 {
			req, penalties = cfg.hd.RequestMoreHeaders(currentTime)
			if req != nil {
				_, sentToPeer = cfg.headerReqSend(ctx, req)
				if sentToPeer {
					cfg.hd.SentRequest(req, currentTime, 5 /*timeout */)
					log.Trace("Sent request", "height", req.Number)
				}
			}
			cfg.penalize(ctx, penalties)
			maxRequests--
		}

		req = cfg.hd.RequestSkeleton()
		if req != nil {
			_, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				cfg.hd.SentRequest(req, currentTime, 5 /* timeout */)
				log.Trace("Sent request", "height", req.Number)
			}
		}
		// Load headers into the database
		var inSync bool
		if inSync, err = cfg.hd.InsertHeaders(headerInserter.FeedHeaderFunc(tx, cfg.blockReader), cfg.chainConfig.TerminalTotalDifficulty, logPrefix, logEvery.C); err != nil {
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
	}
	if headerInserter.Unwind() {
		u.UnwindTo(headerInserter.UnwindPoint(), common.Hash{})
	} else if headerInserter.GetHighest() != 0 {
		if err := fixCanonicalChain(logPrefix, logEvery, headerInserter.GetHighest(), headerInserter.GetHighestHash(), tx, cfg.blockReader); err != nil {
			return fmt.Errorf("fix canonical chain: %w", err)
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
	log.Info(fmt.Sprintf("[%s] Processed", logPrefix), "highest inserted", headerInserter.GetHighest(), "age", common.PrettyAge(time.Unix(int64(headerInserter.GetHighestTimestamp()), 0)))

	return nil
}

func fixCanonicalChain(logPrefix string, logEvery *time.Ticker, height uint64, hash common.Hash, tx kv.StatelessRwTx, headerReader interfaces.FullBlockReader) error {
	if height == 0 {
		return nil
	}
	ancestorHash := hash
	ancestorHeight := height

	var ch common.Hash
	var err error
	for ch, err = rawdb.ReadCanonicalHash(tx, ancestorHeight); err == nil && ch != ancestorHash; ch, err = rawdb.ReadCanonicalHash(tx, ancestorHeight) {
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
			log.Info(fmt.Sprintf("[%s] write canonical markers", logPrefix), "ancestor", ancestorHeight, "hash", ancestorHash)
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
	var headerProgress uint64
	headerProgress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	badBlock := u.BadBlock != (common.Hash{})
	if badBlock {
		cfg.hd.ReportBadHeader(u.BadBlock)
		// Mark all descendants of bad block as bad too
		headerCursor, cErr := tx.Cursor(kv.Headers)
		if cErr != nil {
			return cErr
		}
		defer headerCursor.Close()
		var k, v []byte
		for k, v, err = headerCursor.Seek(dbutils.EncodeBlockNumber(u.UnwindPoint + 1)); err == nil && k != nil; k, v, err = headerCursor.Next() {
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
	for blockHeight := headerProgress; blockHeight > u.UnwindPoint; blockHeight-- {
		if err = rawdb.DeleteCanonicalHash(tx, blockHeight); err != nil {
			return err
		}
	}
	if badBlock {
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
		// When we forward sync, total difficulty is updated within headers processing
		if err = stages.SaveStageProgress(tx, stages.TotalDifficulty, maxNum); err != nil {
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
	config      *params.ChainConfig
	tx          kv.RwTx
	blockReader interfaces.FullBlockReader
}

func (cr chainReader) Config() *params.ChainConfig  { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.Header(context.Background(), cr.tx, hash, number)
		return h
	}
	return rawdb.ReadHeader(cr.tx, hash, number)
}
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header {
	if cr.blockReader != nil {
		h, _ := cr.blockReader.HeaderByNumber(context.Background(), cr.tx, number)
		return h
	}
	return rawdb.ReadHeaderByNumber(cr.tx, number)

}
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header {
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

type epochReader struct {
	tx kv.RwTx
}

func (cr epochReader) GetEpoch(hash common.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadEpoch(cr.tx, number, hash)
}
func (cr epochReader) PutEpoch(hash common.Hash, number uint64, proof []byte) error {
	return rawdb.WriteEpoch(cr.tx, number, hash, proof)
}
func (cr epochReader) GetPendingEpoch(hash common.Hash, number uint64) ([]byte, error) {
	return rawdb.ReadPendingEpoch(cr.tx, number, hash)
}
func (cr epochReader) PutPendingEpoch(hash common.Hash, number uint64, proof []byte) error {
	return rawdb.WritePendingEpoch(cr.tx, number, hash, proof)
}
func (cr epochReader) FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash common.Hash, transitionProof []byte, err error) {
	return rawdb.FindEpochBeforeOrEqualNumber(cr.tx, number)
}

func HeadersPrune(p *PruneState, tx kv.RwTx, cfg HeadersCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
