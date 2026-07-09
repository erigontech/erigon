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

package freezeblocks

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/concurrent"
	"github.com/erigontech/erigon/common/dbg"
	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/polygon/bor/bordb"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
)

var (
	BorDataNotReadyTimeout = 5 * time.Minute
)

// headers
// value: first_byte_of_header_hash + header_rlp
// header_hash       -> headers_segment_offset

// bodies
// value: rlp(types.BodyForStorage)
// block_num_u64     -> bodies_segment_offset

// transactions
// value: first_byte_of_transaction_hash + sender_address + transaction_rlp
// transaction_hash  -> transactions_segment_offset
// transaction_hash  -> block_number

func chooseSegmentEnd(from, to uint64, snapType snaptype.Enum, snCfg *snapcfg.Cfg) uint64 {
	blocksPerFile := snapcfg.MergeLimitFromCfg(snCfg, snapType, from)

	next := (from/blocksPerFile + 1) * blocksPerFile
	to = min(next, to)

	if to < snaptype.Erigon2MinSegmentSize {
		return to
	}

	return to - (to % snaptype.Erigon2MinSegmentSize) // round down to the nearest 1k
}

type BlockRetire struct {
	maxScheduledBlock  atomic.Uint64
	working            atomic.Bool
	lastRetireGapStart atomic.Uint64

	// shared semaphore with AggregatorV3 to allow only one type of snapshot building at a time
	snBuildAllowed *semaphore.Weighted

	workers atomic.Int32
	tmpDir  string
	db      kv.RoDB

	notifier    services.DBEventNotifier
	logger      log.Logger
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
	dirs        datadir.Dirs
	chainConfig *chain.Config
	config      *ethconfig.Config

	snCfg *snapcfg.Cfg

	heimdallStore         heimdall.Store
	bridgeStore           bridge.Store
	borDataNotReadyBefore time.Time

	// Close cancels the in-flight retire (ctx/stopFn) and waits for it (background).
	background concurrent.ClosingWaitGroup
	ctx        context.Context
	stopFn     context.CancelFunc
}

func NewBlockRetire(
	ctx context.Context,
	compressWorkers int,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	db kv.RoDB,
	heimdallStore heimdall.Store,
	bridgeStore bridge.Store,
	chainConfig *chain.Config,
	config *ethconfig.Config,
	notifier services.DBEventNotifier,
	snBuildAllowed *semaphore.Weighted,
	logger log.Logger,
) *BlockRetire {
	var chainName string
	if chainConfig != nil {
		chainName = chainConfig.ChainName
	}
	snCfg := snapcfg.KnownCfgOrDevnet(chainName)
	r := &BlockRetire{
		tmpDir:                dirs.Tmp,
		dirs:                  dirs,
		blockReader:           blockReader,
		blockWriter:           blockWriter,
		db:                    db,
		snBuildAllowed:        snBuildAllowed,
		chainConfig:           chainConfig,
		config:                config,
		snCfg:                 snCfg,
		notifier:              notifier,
		logger:                logger,
		heimdallStore:         heimdallStore,
		bridgeStore:           bridgeStore,
		borDataNotReadyBefore: time.Now(),
	}
	r.ctx, r.stopFn = context.WithCancel(ctx)
	r.workers.Store(int32(compressWorkers))
	return r
}

func (br *BlockRetire) SetWorkers(workers int) { br.workers.Store(int32(workers)) }
func (br *BlockRetire) GetWorkers() int        { return int(br.workers.Load()) }

// SetCommitGate wraps the retirement's chain DB reads with the given gate so
// each db.View acquires RLock, serializing against a writer (Aggregator
// commit+prune path) that briefly holds Lock during MDBX commit. Prevents a
// retirement RO tx from pinning the freelist and blocking page reclamation.
// Safe to call with nil — no-op. Must be called before retirement starts.
func (br *BlockRetire) SetCommitGate(gate *sync.RWMutex) {
	if gate == nil {
		return
	}
	br.db = kv.NewGatedRoDB(br.db, gate)
}

func (br *BlockRetire) IO() (services.FullBlockReader, *blockio.BlockWriter) {
	return br.blockReader, br.blockWriter
}

func (br *BlockRetire) BorStore() (heimdall.Store, bridge.Store) {
	return br.heimdallStore, br.bridgeStore
}

func (br *BlockRetire) Writer() *blocksnapshots.RoSnapshots {
	return br.blockReader.Snapshots().(*blocksnapshots.RoSnapshots)
}

func (br *BlockRetire) snapshots() *blocksnapshots.RoSnapshots {
	return br.blockReader.Snapshots().(*blocksnapshots.RoSnapshots)
}

func (br *BlockRetire) borSnapshots() *heimdall.RoSnapshots {
	return br.blockReader.BorSnapshots().(*heimdall.RoSnapshots)
}

func CanRetire(curBlockNum uint64, blocksInSnapshots uint64, snapType snaptype.Enum, snCfg *snapcfg.Cfg) (blockFrom, blockTo uint64, can bool) {
	var keep uint64 = dbg.MaxReorgDepth
	if curBlockNum <= keep {
		return
	}
	blockFrom = blocksInSnapshots + 1
	return snapshotsync.CanRetire(blockFrom, curBlockNum-keep, snapType, snCfg)
}

func CanDeleteTo(curBlockNum uint64, blocksInSnapshots uint64) (blockTo uint64) {
	if blocksInSnapshots == 0 {
		return 0
	}

	var keep uint64 = 1024 // params.FullImmutabilityThreshold //TODO: we will increase this value after db optimizations - about on-chain-tip prune speed
	if curBlockNum+999 < keep {
		// To prevent overflow of uint64 below
		return blocksInSnapshots + 1
	}
	hardLimit := (curBlockNum/1_000)*1_000 - keep
	return min(hardLimit, blocksInSnapshots+1)
}

func (br *BlockRetire) dbHasEnoughDataForBlocksRetire(ctx context.Context) (bool, error) {
	// pre-check if db has enough data
	var haveGap bool
	if err := br.db.View(ctx, func(tx kv.Tx) error {
		firstInDB, ok, err := rawdb.ReadFirstNonGenesisHeaderNumber(tx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		nextBlockInSnapshots := br.snapshots().SegmentsMax() + 1
		haveGap = nextBlockInSnapshots < firstInDB
		if haveGap {
			// Log once per unique gap: Swap stores firstInDB and returns the previous
			// value. If it matches, we already logged for this exact gap — skip.
			if br.lastRetireGapStart.Swap(firstInDB) != firstInDB {
				br.logger.Debug("skipping block snapshot retirement: db does not contain the next block after snapshots",
					"nextBlockInSnapshots", nextBlockInSnapshots,
					"firstBlockInDB", firstInDB,
					"gapBlocks", firstInDB-nextBlockInSnapshots,
					"recommendation", "retirement will resume once matching snapshots are downloaded or the gap is covered")
			}
		} else {
			br.lastRetireGapStart.Store(0)
		}
		return nil
	}); err != nil {
		return false, err
	}
	return !haveGap, nil
}

func (br *BlockRetire) retireBlocks(
	ctx context.Context,
	minBlockNum uint64,
	maxBlockNum uint64,
	lvl log.Lvl,
	seeder downloader.SeederClient,
) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers.Load()
	snapshots := br.snapshots()

	blockFrom, blockTo, ok := CanRetire(maxBlockNum, minBlockNum, snaptype.Unknown, br.snCfg)

	if ok {
		if has, err := br.dbHasEnoughDataForBlocksRetire(ctx); err != nil {
			return false, err
		} else if !has {
			return false, nil
		}
		logger.Log(lvl, "[snapshots] Retire Blocks", "range",
			fmt.Sprintf("%s-%s", common.PrettyCounter(blockFrom), common.PrettyCounter(blockTo)))
		// in future we will do it in background
		if err := DumpBlocks(ctx, blockFrom, blockTo, br.chainConfig, tmpDir, snapshots.Dir(), db, int(workers), lvl, logger, blockReader, br.snCfg, &snapshots.BaseRoSnapshots); err != nil {
			return ok, fmt.Errorf("DumpBlocks: %w", err)
		}

		if err := snapshots.OpenFolder(); err != nil {
			return ok, fmt.Errorf("open: %w", err)
		}
		snapshots.LogStat("blocks:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merged, err := br.MergeBlocks(ctx, lvl, seeder)
	return ok || merged, err
}

func (br *BlockRetire) MergeBlocks(
	ctx context.Context,
	lvl log.Lvl,
	seeder downloader.SeederClient,
) (merged bool, err error) {
	notifier, logger, _, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers.Load()
	snapshots := br.snapshots()

	merger := snapshotsync.NewMerger(tmpDir, int(workers), lvl, db, br.chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(true), snapshots.BlocksAvailable())
	if len(rangesToMerge) == 0 {
		//TODO: enable, but optimize to reduce chain-tip impact
		//if err := snapshots.RemoveOverlaps(); err != nil {
		//	return false, err
		//}
		return false, nil
	}
	merged = true
	onMerge := func(mergedFileNames []string) error {
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		return seeder.Seed(ctx, mergedFileNames)
	}
	if err = merger.Merge(ctx, &snapshots.BaseRoSnapshots, snapshots.Types(), rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, seeder.Delete); err != nil {
		return false, err
	}

	// remove old garbage files
	if err = snapshots.RemoveOverlaps(func(l []string) error {
		return seeder.Delete(ctx, l)
	}); err != nil {
		return false, err
	}
	return
}

var mxPruneTookBor = metrics.GetOrCreateSummary(`prune_seconds{type="bor"}`)

func (br *BlockRetire) PruneAncientBlocks(tx kv.RwTx, limit int, timeout time.Duration) (deleted int, err error) {
	if br.blockReader.FreezingCfg().KeepBlocks {
		return deleted, nil
	}
	currentProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return deleted, err
	}

	t := time.Now()

	// PruneBlocks/PruneHeimdall delete the whole [from, to) range capped at limit in a
	// single cursor pass; the sync loop re-enters each cycle, so no inner loop is needed.
	if canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBlocks()); canDeleteTo > 0 {
		if deleted, err = br.blockWriter.PruneBlocks(context.Background(), tx, canDeleteTo, limit); err != nil {
			return deleted, err
		}
	}

	var deletedBorBlocks int
	if br.chainConfig.Bor != nil {
		if canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBorBlocks(true)); canDeleteTo > 0 {
			deletedBorBlocks, err = func() (int, error) {
				defer mxPruneTookBor.ObserveDuration(time.Now())

				return bordb.PruneHeimdall(context.Background(),
					br.heimdallStore, br.bridgeStore, nil, canDeleteTo, limit)
			}()
			if err != nil {
				return deleted, err
			}
		}
	}

	if deleted > 0 || deletedBorBlocks > 0 {
		br.logger.Debug("[snapshots] Prune Blocks", "deleted", deleted, "deletedBorBlocks", deletedBorBlocks, "took", time.Since(t))
	}

	return deleted + deletedBorBlocks, nil
}

func (br *BlockRetire) RetireBlocksInBackground(
	ctx context.Context,
	minBlockNum,
	maxBlockNum uint64,
	lvl log.Lvl,
	seeder downloader.SeederClient,
	onFinishRetire func() error,
	onDone func(),
) bool {
	if maxBlockNum > br.maxScheduledBlock.Load() {
		br.maxScheduledBlock.Store(maxBlockNum)
	}

	if !br.working.CompareAndSwap(false, true) {
		return false
	}

	started := br.background.TryGo(func() {
		defer onDone()
		defer br.working.Store(false)

		// Cancel on either the caller's ctx or Close (br.ctx).
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stopOnClose := context.AfterFunc(br.ctx, cancel)
		defer stopOnClose()

		if br.snBuildAllowed != nil {
			//we are inside own goroutine - it's fine to block here
			if err := br.snBuildAllowed.Acquire(ctx, 1); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, common.ErrStopped) {
					br.logger.Warn("[snapshots] retire blocks", "err", err)
				}
				return
			}
			defer br.snBuildAllowed.Release(1)
		}

		err := br.RetireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seeder, onFinishRetire)
		if errors.Is(err, heimdall.ErrHeimdallDataIsNotReady) {
			br.borDataNotReadyBefore = time.Now().Add(BorDataNotReadyTimeout)
			br.logger.Debug("[snapshots] bor data is not ready to be retired", "nextAttemptAt", br.borDataNotReadyBefore)
			return
		}
		if errors.Is(err, snapshotsync.ErrRangeBuildInProgress) {
			br.logger.Debug("[snapshots] retire blocks: deferred to in-flight build", "err", err)
			return
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, common.ErrStopped) {
				br.logger.Debug("[snapshots] retire blocks canceled", "err", err)
				return
			}
			br.logger.Error("[snapshots] retire blocks", "err", err)
			return
		}
	})
	if !started { // Close has latched; no retire was started
		br.working.Store(false)
		return false
	}

	return true
}

// Close cancels the in-flight background retire and waits for it, so the DB and
// snapshots can be torn down safely afterwards. Idempotent.
func (br *BlockRetire) Close() {
	if !br.background.BeginClose() {
		return
	}
	br.stopFn()
	br.background.Wait()
}

func (br *BlockRetire) RetireBlocks(
	ctx context.Context,
	requestedMinBlockNum uint64,
	requestedMaxBlockNum uint64,
	lvl log.Lvl,
	seeder downloader.SeederClient,
	onFinish func() error,
) error {
	if requestedMaxBlockNum > br.maxScheduledBlock.Load() {
		br.maxScheduledBlock.Store(requestedMaxBlockNum)
	}
	includeBor := br.chainConfig.Bor != nil

	if includeBor && time.Now().After(br.borDataNotReadyBefore) {
		return nil
	}

	if err := br.BuildMissedIndicesIfNeed(ctx, "RetireBlocks", br.notifier); err != nil {
		return err
	}

	if includeBor {
		// "bor snaps" can be behind "block snaps", it's ok:
		//      - for example because of `kill -9` in the middle of merge
		//      - or if manually delete bor files (for re-generation)
		var err error
		var okBor bool
		for {
			minBlockNum := max(br.blockReader.FrozenBlocks(), requestedMinBlockNum)
			okBor, err = br.retireBorBlocks(ctx, requestedMinBlockNum, minBlockNum, lvl, seeder)
			if err != nil {
				return err
			}
			if !okBor {
				break
			}
		}
	}

	var err error
	for {
		var ok, okBor bool
		minBlockNum := max(br.blockReader.FrozenBlocks(), requestedMinBlockNum)
		maxBlockNum := br.maxScheduledBlock.Load()
		ok, err = br.retireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seeder)
		if err != nil {
			return err
		}

		if includeBor {
			minBorBlockNum := max(br.blockReader.FrozenBorBlocks(true), requestedMinBlockNum)
			okBor, err = br.retireBorBlocks(ctx, minBorBlockNum, maxBlockNum, lvl, seeder)
			if err != nil {
				return err
			}
		}
		if onFinish != nil {
			if err := onFinish(); err != nil {
				return err
			}
		}

		if !(ok || okBor) {
			break
		}
	}
	return nil
}

func (br *BlockRetire) BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier) error {
	if err := br.snapshots().BuildMissedIndices(ctx, logPrefix, notifier, br.dirs, br.chainConfig, br.logger); err != nil {
		return err
	}

	if br.chainConfig.Bor != nil {
		if err := br.borSnapshots().BaseRoSnapshots.BuildMissedIndices(ctx, logPrefix, notifier, br.dirs, br.chainConfig, br.logger); err != nil {
			return err
		}
	}

	return nil
}
func (br *BlockRetire) RemoveOverlaps(onDelete func(l []string) error) error {
	if err := br.snapshots().RemoveOverlaps(onDelete); err != nil {
		return err
	}

	if br.chainConfig.Bor != nil {
		if err := br.borSnapshots().BaseRoSnapshots.RemoveOverlaps(onDelete); err != nil {
			return err
		}
	}
	return nil
}

func (br *BlockRetire) MadvNormal() *BlockRetire {
	br.snapshots().MadvNormal()
	if br.chainConfig.Bor != nil {
		br.borSnapshots().BaseRoSnapshots.MadvNormal()
	}
	return br
}

func (br *BlockRetire) DisableReadAhead() {
	br.snapshots().DisableReadAhead()
	if br.chainConfig.Bor != nil {
		br.borSnapshots().BaseRoSnapshots.DisableReadAhead()
	}
}

func DumpBlocks(ctx context.Context, blockFrom, blockTo uint64, chainConfig *chain.Config, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader, snCfg *snapcfg.Cfg, inProgress *snapshotsync.BaseRoSnapshots) error {
	firstTxNum := blockReader.FirstTxnNumNotInSnapshots(nil)
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, snaptype2.Enums.Headers, snCfg) {
		lastTxNum, err := dumpBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, snaptype2.Enums.Headers, snCfg), tmpDir, snapDir, firstTxNum, chainDB, chainConfig, workers, lvl, logger, inProgress)
		if err != nil {
			return err
		}
		firstTxNum = lastTxNum + 1
	}
	return nil
}

func dumpBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger, inProgress *snapshotsync.BaseRoSnapshots) (lastTxNum uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	if blockFrom > 0 && firstTxNum == 0 {
		err := fmt.Errorf("firstTxNum is 0 (blocks=%d-%d); must be a mistake, aborting files build", blockFrom, blockTo)
		logger.Error("DumpBodies", "err", err)
		return lastTxNum, err
	}

	if _, err = dumpRange(ctx, snaptype2.Headers.FileInfo(snapDir, blockFrom, blockTo),
		DumpHeaders, nil, chainDB, chainConfig, tmpDir, workers, lvl, logger, inProgress); err != nil {
		return 0, err
	}

	if lastTxNum, err = dumpRange(ctx, snaptype2.Bodies.FileInfo(snapDir, blockFrom, blockTo),
		DumpBodies, func(context.Context) uint64 { return firstTxNum }, chainDB, chainConfig, tmpDir, workers, lvl, logger, inProgress); err != nil {
		return lastTxNum, err
	}
	if _, err = dumpRange(ctx, snaptype2.Transactions.FileInfo(snapDir, blockFrom, blockTo),
		DumpTxs, func(context.Context) uint64 { return firstTxNum }, chainDB, chainConfig, tmpDir, workers, lvl, logger, inProgress); err != nil {
		return lastTxNum, err
	}

	return lastTxNum, nil
}

type firstKeyGetter func(ctx context.Context) uint64
type dumpFunc func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collector func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error)

var BlockCompressCfg = seg.Cfg{
	MinPatternScore: 1_000,
	MinPatternLen:   8, // `5` - reducing ratio because producing too much prefixes
	MaxPatternLen:   128,
	SamplingFactor:  4,         // not 1 - just to save my time
	MaxDictPatterns: 16 * 1024, // the lower RAM used by huffman tree (arrays)

	DictReducerSoftLimit: 1_000_000,
	Workers:              1,
}

func dumpRange(ctx context.Context, f snaptype.FileInfo, dumper dumpFunc, firstKey firstKeyGetter, chainDB kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger, inProgress *snapshotsync.BaseRoSnapshots) (uint64, error) {
	// Claim this (type, range) so a concurrent OpenFolder doesn't pick up the
	// .seg before its index is built and race our BuildIndexes below.
	if inProgress != nil {
		if !inProgress.TryAcquireRange(f.Type.Enum(), f.From, f.To) {
			return 0, fmt.Errorf("dump %s: %w", f.Name(), snapshotsync.ErrRangeBuildInProgress)
		}
		defer inProgress.ReleaseRange(f.Type.Enum(), f.From, f.To)
	}
	var lastKeyValue uint64

	compressCfg := BlockCompressCfg
	compressCfg.Workers = workers
	sn, err := seg.NewCompressor(ctx, "Snapshot "+f.Type.Name(), f.Path, tmpDir, compressCfg, log.LvlTrace, logger)
	if err != nil {
		return lastKeyValue, err
	}
	defer sn.Close()

	// E3 need to keep db smaller: earlier retire -> earlier prune.
	// Means:
	//  - build must be fast
	//  - merge can be slow and expensive
	noCompress := (f.To - f.From) < (snaptype.Erigon2MergeLimit - 1)

	lastKeyValue, err = dumper(ctx, chainDB, chainConfig, f.From, f.To, firstKey, func(v []byte) error {
		if noCompress {
			return sn.AddUncompressedWord(v)
		}
		return sn.AddWord(v)
	}, workers, lvl, logger)

	if err != nil {
		return lastKeyValue, fmt.Errorf("dump %s: %w", f.Name(), err)
	}

	ext := filepath.Ext(f.Name())
	logger.Log(lvl, "[snapshots] Compression start", "file", f.Name()[:len(f.Name())-len(ext)], "workers", sn.WorkersAmount())

	if err := sn.Compress(); err != nil {
		return lastKeyValue, fmt.Errorf("compress: %w", err)
	}

	p := &background.Progress{}

	if err := f.Type.BuildIndexes(ctx, f, nil, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return lastKeyValue, err
	}

	return lastKeyValue, nil
}

var bufPool = sync.Pool{
	New: func() any {
		bytes := [16 * 4096]byte{}
		return &bytes
	},
}

// DumpTxs - [from, to)
// Format: hash[0]_1byte + sender_address_2bytes + txnRlp
func DumpTxs(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (lastTx uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	warmupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	numBuf := make([]byte, 8)
	parse := func(v, valueBuf []byte, senders []common.Address, j int) ([]byte, error) {
		var sender [20]byte
		txn2, err := types.DecodeTransaction(v)
		if err != nil {
			return nil, err
		}
		hash := txn2.Hash()
		hashFirstByte := hash[:1]
		if len(senders) > 0 {
			txn2.SetSender(accounts.InternAddress(senders[j]))
			sender = senders[j]
		} else {
			signer := types.LatestSignerForChainID(chainConfig.ChainID)
			s, err := txn2.Sender(*signer)
			if err != nil {
				return nil, err
			}
			sender = s.Value()
		}

		valueBuf = valueBuf[:0]
		valueBuf = append(valueBuf, hashFirstByte...)
		valueBuf = append(valueBuf, sender[:]...)
		valueBuf = append(valueBuf, v...)
		return valueBuf, nil
	}

	addSystemTx := func(tx kv.Tx, txId types.BaseTxnID) error {
		binary.BigEndian.PutUint64(numBuf, txId.U64())
		tv, err := tx.GetOne(kv.EthTx, numBuf)
		if err != nil {
			return err
		}
		if tv == nil {
			if err := collect(nil); err != nil {
				return err
			}
			return nil
		}

		valueBuf := bufPool.Get().(*[16 * 4096]byte)
		defer bufPool.Put(valueBuf)

		parsed, err := parse(tv, valueBuf[:], nil, 0)
		if err != nil {
			return err
		}
		if err := collect(parsed); err != nil {
			return err
		}
		return nil
	}

	doWarmup, warmupTxs, warmupSenders := blockTo-blockFrom >= 100_000 && workers > 4, &atomic.Bool{}, &atomic.Bool{}
	from := hexutil.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo { // [from, to)
			return false, nil
		}

		h := common.BytesToHash(v)
		dataRLP := rawdb.ReadStorageBodyRLP(tx, h, blockNum)
		if dataRLP == nil {
			return false, fmt.Errorf("body not found: %d, %x", blockNum, h)
		}
		var body types.BodyForStorage
		if e := rlp.DecodeBytes(dataRLP, &body); e != nil {
			return false, e
		}
		if body.TxCount == 0 {
			return true, nil
		}

		if doWarmup && !warmupSenders.Load() && blockNum%1_000 == 0 {
			clean := kv.ReadAheadDeprecated(warmupCtx, db, warmupSenders, kv.Senders, hexutil.EncodeTs(blockNum), 10_000)
			defer clean()
		}
		if doWarmup && !warmupTxs.Load() && blockNum%1_000 == 0 {
			clean := kv.ReadAheadDeprecated(warmupCtx, db, warmupTxs, kv.EthTx, body.BaseTxnID.Bytes(), 100*10_000)
			defer clean()
		}
		senders, err := rawdb.ReadSenders(tx, h, blockNum)
		if err != nil {
			return false, err
		}

		workers := estimate.AlmostAllCPUs()

		if workers > 3 {
			workers = workers / 3 * 2
		}

		if workers > int(body.TxCount-2) {
			workers = max(int(body.TxCount-2), 1)
		}

		parsers := errgroup.Group{}
		parsers.SetLimit(workers)

		valueBufs := make([][]byte, workers)

		for i := 0; i < workers; i++ {
			valueBuf := bufPool.Get().(*[16 * 4096]byte)
			defer bufPool.Put(valueBuf)
			valueBufs[i] = valueBuf[:]
		}

		if err := addSystemTx(tx, body.BaseTxnID); err != nil {
			return false, err
		}

		binary.BigEndian.PutUint64(numBuf, body.BaseTxnID.First())

		collected := -1
		collectorLock := sync.Mutex{}
		collections := sync.NewCond(&collectorLock)
		var j int

		if err := tx.ForAmount(kv.EthTx, numBuf, body.TxCount-2, func(_, tv []byte) error {
			tx := j
			j++

			parsers.Go(func() error {
				valueBuf, err := parse(tv, valueBufs[tx%workers], senders, tx)
				if err != nil {
					collectorLock.Lock()
					defer collectorLock.Unlock()
					collected = tx
					collections.Broadcast() // to fail fast on it.
					return fmt.Errorf("%w, block: %d", err, blockNum)
				}
				collectorLock.Lock()
				defer collectorLock.Unlock()
				for collected < tx-1 {
					collections.Wait()
				}

				// first tx byte => sender address => tx rlp
				if err := collect(valueBuf); err != nil {
					return err
				}
				collected = tx
				collections.Broadcast()

				return nil
			})

			return nil
		}); err != nil {
			return false, fmt.Errorf("ForAmount: %w", err)
		}

		if err := parsers.Wait(); err != nil {
			return false, fmt.Errorf("ForAmount parser: %w", err)
		}

		if err := addSystemTx(tx, types.BaseTxnID(body.BaseTxnID.LastSystemTx(body.TxCount))); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			if lvl >= log.LvlInfo {
				var m runtime.MemStats
				dbg.ReadMemStats(&m)
				logger.Log(lvl, "[snapshots] Dumping txs", "block num", blockNum, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
			} else {
				logger.Log(lvl, "[snapshots] Dumping txs", "block num", blockNum)
			}
		default:
		}
		return true, nil
	}); err != nil {
		return 0, fmt.Errorf("BigChunks: %w", err)
	}
	return 0, nil
}

func DumpHeaders(ctx context.Context, db kv.RoDB, _ *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	return DumpHeadersRaw(ctx, db, nil, blockFrom, blockTo, nil, collect, workers, lvl, logger, false)
}

// DumpHeadersRaw - [from, to)
func DumpHeadersRaw(ctx context.Context, db kv.RoDB, _ *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger, test bool) (uint64, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	// do hash sanity check
	var (
		prevHash  common.Hash
		emptyHash common.Hash
	)

	// Make sure the canonical chain is not broken.
	if blockFrom > 0 && !test {
		if err := db.View(ctx, func(tx kv.Tx) error {
			blockNum := blockFrom - 1
			h, err := rawdb.ReadCanonicalHash(tx, blockNum)
			if err != nil {
				return err
			}
			if h == emptyHash {
				return fmt.Errorf("header not found: %d", blockNum)
			}
			prevHash = h
			return nil
		}); err != nil {
			return 0, err
		}
	}

	key := make([]byte, 8+32)
	from := hexutil.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)
		dataRLP, err := tx.GetOne(kv.Headers, key)
		if err != nil {
			return false, err
		}
		if dataRLP == nil {
			return false, fmt.Errorf("header missed in db: block_num=%d,  hash=%x", blockNum, v)
		}
		h := types.Header{}
		if err := rlp.DecodeBytes(dataRLP, &h); err != nil {
			return false, err
		}
		// Make sure the canonical chain is not broken.
		if prevHash != emptyHash && prevHash != h.ParentHash && !test {
			return false, fmt.Errorf("header hash mismatch: %d, %x != %x", blockNum, prevHash, h.ParentHash)
		}
		prevHash = h.Hash()

		value := make([]byte, len(dataRLP)+1) // first_byte_of_header_hash + header_rlp
		value[0] = h.Hash()[0]
		copy(value[1:], dataRLP)
		if err := collect(value); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Dumping headers", "block num", blockNum,
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return 0, err
	}

	// Make sure the canonical chain is not broken.
	if err := db.View(ctx, func(tx kv.Tx) error {
		if test {
			return nil
		}
		blockNum := blockTo
		h := rawdb.ReadHeaderByNumber(tx, blockNum)
		if h == nil {
			return fmt.Errorf("last header not found: %d", blockNum)
		}
		if prevHash != h.ParentHash {
			return fmt.Errorf("header hash mismatch: %d, %x != %x", blockNum, prevHash, h.ParentHash)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return 0, nil
}

// DumpBodies - [from, to)
func DumpBodies(ctx context.Context, db kv.RoDB, _ *chain.Config, blockFrom, blockTo uint64, firstTxNum firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	blockNumByteLength := 8
	blockHashByteLength := 32
	key := make([]byte, blockNumByteLength+blockHashByteLength)
	from := hexutil.EncodeTs(blockFrom)

	lastTxNum := firstTxNum(ctx)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo {
			return false, nil
		}
		copy(key, k)
		copy(key[8:], v)

		// Important: DB does store canonical and non-canonical txs in same table. And using same body.BaseTxnID
		// But snapshots using canonical TxNum in field body.BaseTxnID
		// So, we manually calc this field here and serialize again.
		//
		// FYI: we also have other table to map canonical BlockNum->TxNum: kv.MaxTxNum
		body, err := rawdb.ReadBodyForStorageByKey(tx, key)
		if err != nil {
			return false, err
		}
		if body == nil {
			logger.Warn("body missed", "block_num", blockNum, "hash", hex.EncodeToString(v))
			return true, nil
		}
		body.BaseTxnID = types.BaseTxnID(lastTxNum)
		lastTxNum = body.BaseTxnID.LastSystemTx(body.TxCount) + 1 // +1 to set it on first systemTxn of next block

		dataRLP, err := rlp.EncodeToBytes(body)
		if err != nil {
			return false, err
		}

		if err := collect(dataRLP); err != nil {
			return false, err
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			if lvl >= log.LvlInfo {
				dbg.ReadMemStats(&m)
			}
			logger.Log(lvl, "[snapshots] Wrote into file", "block num", blockNum,
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return lastTxNum, err
	}

	return lastTxNum, nil
}

func ForEachHeader(ctx context.Context, s *blocksnapshots.RoSnapshots, walker func(header *types.Header) error) error {
	word := make([]byte, 0, 2*4096)

	view := s.View()
	defer view.Close()

	// header is hoisted: walker must not retain &header beyond its callback.
	var header types.Header

	for _, sn := range view.Headers() {
		if err := sn.Src().WithReadAhead(func(g *seg.Getter) error {
			for i := 0; g.HasNext(); i++ {
				word, _ = g.Next(word[:0])
				if err := types.DecodeHeader(word[1:], &header); err != nil {
					return fmt.Errorf("%w, file=%s, record=%d", err, sn.Src().FileName(), i)
				}
				if err := walker(&header); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func RemoveIncompatibleIndices(dirs datadir.Dirs) error {
	l, err := dir2.ListFiles(dirs.Snap, ".idx")
	if err != nil {
		return err
	}
	l1, err := dir2.ListFiles(dirs.SnapAccessors, ".efi")
	if err != nil {
		return err
	}
	l2, err := dir2.ListFiles(dirs.SnapAccessors, ".vi")
	if err != nil {
		return err
	}
	l = append(append(l, l1...), l2...)

	for _, fPath := range l {
		index, err := recsplit.OpenIndex(fPath)
		if err != nil {
			if errors.Is(err, recsplit.IncompatibleErr) {
				_, fName := filepath.Split(fPath)
				if err = dir2.RemoveFile(fPath); err != nil {
					log.Warn("Removing incompatible index", "file", fName, "err", err)
				} else {
					log.Info("Removing incompatible index", "file", fName)
				}
				if err = dir2.RemoveFile(fPath + ".torrent"); err != nil {
					log.Warn("Removing incompatible index", "file", fName, "err", err)
				} else {
					log.Info("Removing incompatible index", "file", fName)
				}
				continue
			}
			return fmt.Errorf("%w, %s", err, fPath)
		}
		index.Close()
	}
	return nil
}
