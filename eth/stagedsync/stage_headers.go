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
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
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
	chainConfig       params.ChainConfig
	headerReqSend     func(context.Context, *headerdownload.HeaderRequest) (enode.ID, bool)
	announceNewHashes func(context.Context, []headerdownload.Announce)
	penalize          func(context.Context, []headerdownload.PenaltyItem)
	batchSize         datasize.ByteSize
	noP2PDiscovery    bool
	snapshots         *snapshotsync.AllSnapshots
	blockReader       interfaces.FullBlockReader
}

func StageHeadersCfg(
	db kv.RwDB,
	headerDownload *headerdownload.HeaderDownload,
	chainConfig params.ChainConfig,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) (enode.ID, bool),
	announceNewHashes func(context.Context, []headerdownload.Announce),
	penalize func(context.Context, []headerdownload.PenaltyItem),
	batchSize datasize.ByteSize,
	noP2PDiscovery bool,
	snapshots *snapshotsync.AllSnapshots,
	blockReader interfaces.FullBlockReader,
) HeadersCfg {
	return HeadersCfg{
		db:                db,
		hd:                headerDownload,
		chainConfig:       chainConfig,
		headerReqSend:     headerReqSend,
		announceNewHashes: announceNewHashes,
		penalize:          penalize,
		batchSize:         batchSize,
		noP2PDiscovery:    noP2PDiscovery,
		snapshots:         snapshots,
		blockReader:       blockReader,
	}
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
) error {
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if cfg.snapshots != nil && !cfg.snapshots.AllSegmentsAvailable() {
		// wait for Downloader service to download all expected snapshots
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()
		for {
			headers, bodies, txs, err := cfg.snapshots.SegmentsAvailability()
			if err != nil {
				return err
			}
			expect := cfg.snapshots.ChainSnapshotConfig().ExpectBlocks
			if expect <= headers && expect <= bodies && expect <= txs {
				log.Info(fmt.Sprintf("[%s] Waiting for snapshots up to block %d...", s.LogPrefix(), expect), "headers", headers, "bodies", bodies, "txs", txs)
				if err := cfg.snapshots.ReopenSegments(); err != nil {
					return err
				}
				if expect > cfg.snapshots.BlocksAvailable() {
					return fmt.Errorf("not enough snapshots available: %d > %d", expect, cfg.snapshots.BlocksAvailable())
				}
				cfg.snapshots.SetAllSegmentsAvailable(true)

				_ = s.Update(tx, cfg.snapshots.BlocksAvailable())
				break
			}
			time.Sleep(10 * time.Second)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[%s] Waiting for snapshots up to block %d...", s.LogPrefix(), expect), "headers", headers, "bodies", bodies, "txs", txs)
			default:
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
			if expect > headers || expect > bodies || expect > txs {
				chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
				if err := cfg.snapshots.BuildIndices(*chainID); err != nil {
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
		if true || count == 0 || count == 1 { // genesis does write 1 record
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
				if header.Number.Uint64() == 5499999 {
					fmt.Printf("writeTD: %d, %x\n", header.Number.Uint64(), header.Hash())
				}
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
			if err = fixCanonicalChain(s.LogPrefix(), logEvery, lastHeader.Number.Uint64(), lastHeader.Hash(), tx, cfg.blockReader); err != nil {
				return err
			}

		}

	}

	var headerProgress uint64
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

	fmt.Printf("readTD: %d, %x\n", headerProgress, hash)
	localTd, err := rawdb.ReadTd(tx, hash, headerProgress)
	if err != nil {
		return err
	}
	fmt.Printf("localTd %d\n", localTd)
	headerInserter := headerdownload.NewHeaderInserter(logPrefix, localTd, headerProgress)
	cfg.hd.SetHeaderReader(&chainReader{config: &cfg.chainConfig, tx: tx})

	var sentToPeer bool
	stopped := false
	prevProgress := headerProgress
Loop:
	for !stopped {

		isTrans, err := rawdb.Transitioned(tx, headerProgress)
		if err != nil {
			return err
		}

		if isTrans {
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

		// Send skeleton request if required
		req = cfg.hd.RequestSkeleton()
		if req != nil {
			_, sentToPeer = cfg.headerReqSend(ctx, req)
			if sentToPeer {
				log.Trace("Sent skeleton request", "height", req.Number)
			}
		}
		// Load headers into the database
		var inSync bool

		if inSync, err = cfg.hd.InsertHeaders(headerInserter.FeedHeaderFunc(tx), cfg.chainConfig.TerminalTotalDifficulty, logPrefix, logEvery.C); err != nil {
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
			log.Trace("headerLoop woken up by the incoming request")
		case <-cfg.hd.SkipCycleHack:
			break Loop
		}
		timer.Stop()
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
		// unwind the merge
		isTrans, err := rawdb.Transitioned(tx, u.UnwindPoint)
		if err != nil {
			return err
		}

		if cfg.chainConfig.TerminalTotalDifficulty != nil && !isTrans {
			if err := tx.Delete(kv.TransitionBlockKey, []byte(kv.TransitionBlockKey), nil); err != nil {
				return err
			}
		}
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
	config *params.ChainConfig
	tx     kv.RwTx
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
