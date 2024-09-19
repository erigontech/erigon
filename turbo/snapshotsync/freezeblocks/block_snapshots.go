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
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	common2 "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	dir2 "github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/polygon/bor/bordb"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync"
)

type RoSnapshots struct {
	snapshotsync.RoSnapshots
}

// NewRoSnapshots - opens all snapshots. But to simplify everything:
//   - it opens snapshots only on App start and immutable after
//   - all snapshots of given blocks range must exist - to make this blocks range available
//   - gaps are not allowed
//   - segment have [from:to) semantic
func NewRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, segmentsMin uint64, logger log.Logger) *RoSnapshots {
	return &RoSnapshots{*snapshotsync.NewRoSnapshots(cfg, snapDir, coresnaptype.BlockSnapshotTypes, segmentsMin, logger)}
}

func mappedHeaderSnapshot(sn *DirtySegment) *silkworm.MappedHeaderSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.Index().FilePath(), sn.Index().DataHandle(), sn.Index().Size())
	return silkworm.NewMappedHeaderSnapshot(segmentRegion, idxRegion)
}

func mappedBodySnapshot(sn *DirtySegment) *silkworm.MappedBodySnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxRegion := silkworm.NewMemoryMappedRegion(sn.Index().FilePath(), sn.Index().DataHandle(), sn.Index().Size())
	return silkworm.NewMappedBodySnapshot(segmentRegion, idxRegion)
}

func mappedTxnSnapshot(sn *DirtySegment) *silkworm.MappedTxnSnapshot {
	segmentRegion := silkworm.NewMemoryMappedRegion(sn.FilePath(), sn.DataHandle(), sn.Size())
	idxTxnHash := sn.Index(coresnaptype.Indexes.TxnHash)
	idxTxnHashRegion := silkworm.NewMemoryMappedRegion(idxTxnHash.FilePath(), idxTxnHash.DataHandle(), idxTxnHash.Size())
	idxTxnHash2BlockNum := sn.Index(coresnaptype.Indexes.TxnHash2BlockNum)
	idxTxnHash2BlockRegion := silkworm.NewMemoryMappedRegion(idxTxnHash2BlockNum.FilePath(), idxTxnHash2BlockNum.DataHandle(), idxTxnHash2BlockNum.Size())
	return silkworm.NewMappedTxnSnapshot(segmentRegion, idxTxnHashRegion, idxTxnHash2BlockRegion)
}

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

func buildIdx(ctx context.Context, sn snaptype.FileInfo, indexBuilder snaptype.IndexBuilder, chainConfig *chain.Config, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error {
	//log.Info("[snapshots] build idx", "file", sn.Name())
	if err := sn.Type.BuildIndexes(ctx, sn, indexBuilder, chainConfig, tmpDir, p, lvl, logger); err != nil {
		return fmt.Errorf("buildIdx: %s: %s", sn.Type, err)
	}
	//log.Info("[snapshots] finish build idx", "file", fName)
	return nil
}

func Segments(dir string, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []snapshotsync.Range, err error) {
	return snapshotsync.TypedSegments(dir, minBlock, coresnaptype.BlockSnapshotTypes, true)
}

func SegmentsCaplin(dir string, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []Range, err error) {
	list, err := snaptype.Segments(dir)
	if err != nil {
		return nil, missingSnapshots, err
	}

	{
		var l, lSidecars []snaptype.FileInfo
		var m []Range
		for _, f := range list {
			if f.Type.Enum() != snaptype.CaplinEnums.BeaconBlocks && f.Type.Enum() != snaptype.CaplinEnums.BlobSidecars {
				continue
			}
			if f.Type.Enum() == snaptype.CaplinEnums.BlobSidecars {
				lSidecars = append(lSidecars, f) // blobs are an exception
				continue
			}
			l = append(l, f)
		}
		l, m = noGaps(noOverlaps(l))
		if len(m) > 0 {
			lst := m[len(m)-1]
			log.Debug("[snapshots] see gap", "type", snaptype.CaplinEnums.BeaconBlocks, "from", lst.from)
		}
		res = append(res, l...)
		res = append(res, lSidecars...)
		missingSnapshots = append(missingSnapshots, m...)
	}
	return res, missingSnapshots, nil
}

func chooseSegmentEnd(from, to uint64, snapType snaptype.Enum, chainConfig *chain.Config) uint64 {
	var chainName string

	if chainConfig != nil {
		chainName = chainConfig.ChainName
	}
	blocksPerFile := snapcfg.MergeLimitFromCfg(snapcfg.KnownCfg(chainName), snapType, from)

	next := (from/blocksPerFile + 1) * blocksPerFile
	to = min(next, to)

	if to < snaptype.Erigon2MinSegmentSize {
		return to
	}

	return to - (to % snaptype.Erigon2MinSegmentSize) // round down to the nearest 1k
}

type BlockRetire struct {
	maxScheduledBlock atomic.Uint64
	working           atomic.Bool

	// shared semaphore with AggregatorV3 to allow only one type of snapshot building at a time
	snBuildAllowed *semaphore.Weighted

	workers int
	tmpDir  string
	db      kv.RoDB

	notifier    services.DBEventNotifier
	logger      log.Logger
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
	dirs        datadir.Dirs
	chainConfig *chain.Config

	heimdallStore heimdall.Store
	bridgeStore   bridge.Store
}

func NewBlockRetire(
	compressWorkers int,
	dirs datadir.Dirs,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	db kv.RoDB,
	heimdallStore heimdall.Store,
	bridgeStore bridge.Store,
	chainConfig *chain.Config,
	notifier services.DBEventNotifier,
	snBuildAllowed *semaphore.Weighted,
	logger log.Logger,
) *BlockRetire {
	return &BlockRetire{
		workers:        compressWorkers,
		tmpDir:         dirs.Tmp,
		dirs:           dirs,
		blockReader:    blockReader,
		blockWriter:    blockWriter,
		db:             db,
		snBuildAllowed: snBuildAllowed,
		chainConfig:    chainConfig,
		notifier:       notifier,
		logger:         logger,
		heimdallStore:  heimdallStore,
		bridgeStore:    bridgeStore,
	}
}

func (br *BlockRetire) SetWorkers(workers int) { br.workers = workers }
func (br *BlockRetire) GetWorkers() int        { return br.workers }

func (br *BlockRetire) IO() (services.FullBlockReader, *blockio.BlockWriter) {
	return br.blockReader, br.blockWriter
}

func (br *BlockRetire) Writer() *RoSnapshots { return br.blockReader.Snapshots().(*RoSnapshots) }

func (br *BlockRetire) snapshots() *RoSnapshots { return br.blockReader.Snapshots().(*RoSnapshots) }

func (br *BlockRetire) borSnapshots() *heimdall.RoSnapshots {
	return br.blockReader.BorSnapshots().(*heimdall.RoSnapshots)
}

func CanRetire(curBlockNum uint64, blocksInSnapshots uint64, snapType snaptype.Enum, chainConfig *chain.Config) (blockFrom, blockTo uint64, can bool) {
	var keep uint64 = 1024 //TODO: we will increase it to params.FullImmutabilityThreshold after some db optimizations
	if curBlockNum <= keep {
		return
	}
	blockFrom = blocksInSnapshots + 1
	return canRetire(blockFrom, curBlockNum-keep, snapType, chainConfig)
}

func canRetire(from, to uint64, snapType snaptype.Enum, chainConfig *chain.Config) (blockFrom, blockTo uint64, can bool) {
	if to <= from {
		return
	}
	blockFrom = (from / 1_000) * 1_000
	roundedTo1K := (to / 1_000) * 1_000
	var maxJump uint64 = 1_000

	var chainName string

	if chainConfig != nil {
		chainName = chainConfig.ChainName
	}

	mergeLimit := snapcfg.MergeLimitFromCfg(snapcfg.KnownCfg(chainName), snapType, blockFrom)

	if blockFrom%mergeLimit == 0 {
		maxJump = mergeLimit
	} else if blockFrom%100_000 == 0 {
		maxJump = 100_000
	} else if blockFrom%10_000 == 0 {
		maxJump = 10_000
	}
	//roundedTo1K := (to / 1_000) * 1_000
	jump := min(maxJump, roundedTo1K-blockFrom)
	switch { // only next segment sizes are allowed
	case jump >= mergeLimit:
		blockTo = blockFrom + mergeLimit
	case jump >= 100_000:
		blockTo = blockFrom + 100_000
	case jump >= 10_000:
		blockTo = blockFrom + 10_000
	case jump >= 1_000:
		blockTo = blockFrom + 1_000
	default:
		blockTo = blockFrom
	}
	return blockFrom, blockTo, blockTo-blockFrom >= 1_000
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
		lastInFiles := br.snapshots().SegmentsMax() + 1
		haveGap = lastInFiles < firstInDB
		if haveGap {
			log.Debug("[snapshots] not enough blocks in db to create snapshots", "lastInFiles", lastInFiles, " firstBlockInDB", firstInDB, "recommendations", "it's ok to ignore this message. can fix by: downloading more files `rm datadir/snapshots/prohibit_new_downloads.lock datdir/snapshots/snapshots-lock.json`, or downloading old blocks to db `integration stage_headers --reset`")
		}
		return nil
	}); err != nil {
		return false, err
	}
	return !haveGap, nil
}

func (br *BlockRetire) retireBlocks(ctx context.Context, minBlockNum uint64, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDelete func(l []string) error) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	notifier, logger, blockReader, tmpDir, db, workers := br.notifier, br.logger, br.blockReader, br.tmpDir, br.db, br.workers
	snapshots := br.snapshots()

	blockFrom, blockTo, ok := CanRetire(maxBlockNum, minBlockNum, snaptype.Unknown, br.chainConfig)

	if ok {
		if has, err := br.dbHasEnoughDataForBlocksRetire(ctx); err != nil {
			return false, err
		} else if !has {
			return false, nil
		}
		logger.Log(lvl, "[snapshots] Retire Blocks", "range",
			fmt.Sprintf("%s-%s", common2.PrettyCounter(blockFrom), common2.PrettyCounter(blockTo)))
		// in future we will do it in background
		if err := DumpBlocks(ctx, blockFrom, blockTo, br.chainConfig, tmpDir, snapshots.Dir(), db, workers, lvl, logger, blockReader); err != nil {
			return ok, fmt.Errorf("DumpBlocks: %w", err)
		}

		if err := snapshots.ReopenFolder(); err != nil {
			return ok, fmt.Errorf("reopen: %w", err)
		}
		snapshots.LogStat("blocks:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merger := NewMerger(tmpDir, workers, lvl, db, br.chainConfig, logger)
	rangesToMerge := merger.FindMergeRanges(snapshots.Ranges(), snapshots.BlocksAvailable())
	if len(rangesToMerge) == 0 {
		return ok, nil
	}
	ok = true // have something to merge
	onMerge := func(r snapshotsync.Range) error {
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}

		if seedNewSnapshots != nil {
			downloadRequest := []snapshotsync.DownloadRequest{
				snapshotsync.NewDownloadRequest("", ""),
			}
			if err := seedNewSnapshots(downloadRequest); err != nil {
				return err
			}
		}
		return nil
	}
	err := merger.Merge(ctx, &snapshots.RoSnapshots, snapshots.Types(), rangesToMerge, snapshots.Dir(), true /* doIndex */, onMerge, onDelete)
	if err != nil {
		return ok, err
	}

	// remove old garbage files
	if err := snapshots.RemoveOverlaps(); err != nil {
		return false, err
	}
	return ok, nil
}

var ErrNothingToPrune = errors.New("nothing to prune")

var mxPruneTookBor = metrics.GetOrCreateSummary(`prune_seconds{type="bor"}`)

func (br *BlockRetire) PruneAncientBlocks(tx kv.RwTx, limit int) (deleted int, err error) {
	if br.blockReader.FreezingCfg().KeepBlocks {
		return deleted, nil
	}
	currentProgress, err := stages.GetStageProgress(tx, stages.Senders)
	if err != nil {
		return deleted, err
	}
	if canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBlocks()); canDeleteTo > 0 {
		br.logger.Debug("[snapshots] Prune Blocks", "to", canDeleteTo, "limit", limit)
		deletedBlocks, err := br.blockWriter.PruneBlocks(context.Background(), tx, canDeleteTo, limit)
		if err != nil {
			return deleted, err
		}
		deleted += deletedBlocks
	}

	if br.chainConfig.Bor != nil {
		if canDeleteTo := CanDeleteTo(currentProgress, br.blockReader.FrozenBorBlocks()); canDeleteTo > 0 {
			// PruneBorBlocks - [1, to) old blocks after moving it to snapshots.

			deletedBorBlocks, err := func() (deleted int, err error) {
				defer mxPruneTookBor.ObserveDuration(time.Now())
				return bordb.PruneHeimdall(context.Background(),
					br.heimdallStore, br.bridgeStore, tx, canDeleteTo, limit)
			}()
			br.logger.Debug("[snapshots] Prune Bor Blocks", "to", canDeleteTo, "limit", limit, "deleted", deleted, "err", err)
			if err != nil {
				return deleted, err
			}
			deleted += deletedBorBlocks
		}

	}

	return deleted, nil
}

func (br *BlockRetire) RetireBlocksInBackground(ctx context.Context, minBlockNum, maxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDeleteSnapshots func(l []string) error, onFinishRetire func() error) {
	if maxBlockNum > br.maxScheduledBlock.Load() {
		br.maxScheduledBlock.Store(maxBlockNum)
	}

	if !br.working.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer br.working.Store(false)

		if br.snBuildAllowed != nil {
			//we are inside own goroutine - it's fine to block here
			if err := br.snBuildAllowed.Acquire(ctx, 1); err != nil {
				br.logger.Warn("[snapshots] retire blocks", "err", err)
				return
			}
			defer br.snBuildAllowed.Release(1)
		}

		err := br.RetireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots, onFinishRetire)
		if err != nil {
			br.logger.Warn("[snapshots] retire blocks", "err", err)
			return
		}
	}()
}

func (br *BlockRetire) RetireBlocks(ctx context.Context, requestedMinBlockNum uint64, requestedMaxBlockNum uint64, lvl log.Lvl, seedNewSnapshots func(downloadRequest []snapshotsync.DownloadRequest) error, onDeleteSnapshots func(l []string) error, onFinish func() error) error {
	if requestedMaxBlockNum > br.maxScheduledBlock.Load() {
		br.maxScheduledBlock.Store(requestedMaxBlockNum)
	}
	includeBor := br.chainConfig.Bor != nil

	if err := br.BuildMissedIndicesIfNeed(ctx, "RetireBlocks", br.notifier, br.chainConfig); err != nil {
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
			okBor, err = br.retireBorBlocks(ctx, br.blockReader.FrozenBorBlocks(), minBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
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
		ok, err = br.retireBlocks(ctx, minBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
		if err != nil {
			return err
		}

		if includeBor {
			minBorBlockNum := max(br.blockReader.FrozenBorBlocks(), requestedMinBlockNum)
			okBor, err = br.retireBorBlocks(ctx, minBorBlockNum, maxBlockNum, lvl, seedNewSnapshots, onDeleteSnapshots)
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

func (br *BlockRetire) BuildMissedIndicesIfNeed(ctx context.Context, logPrefix string, notifier services.DBEventNotifier, cc *chain.Config) error {
	if err := br.snapshots().BuildMissedIndices(ctx, logPrefix, notifier, br.dirs, cc, br.logger); err != nil {
		return err
	}

	if cc.Bor != nil {
		if err := br.borSnapshots().RoSnapshots.BuildMissedIndices(ctx, logPrefix, notifier, br.dirs, cc, br.logger); err != nil {
			return err
		}
	}

	return nil
}

func DumpBlocks(ctx context.Context, blockFrom, blockTo uint64, chainConfig *chain.Config, tmpDir, snapDir string, chainDB kv.RoDB, workers int, lvl log.Lvl, logger log.Logger, blockReader services.FullBlockReader) error {
	firstTxNum := blockReader.FirstTxnNumNotInSnapshots()
	for i := blockFrom; i < blockTo; i = chooseSegmentEnd(i, blockTo, coresnaptype.Enums.Headers, chainConfig) {
		lastTxNum, err := dumpBlocksRange(ctx, i, chooseSegmentEnd(i, blockTo, coresnaptype.Enums.Headers, chainConfig), tmpDir, snapDir, firstTxNum, chainDB, chainConfig, workers, lvl, logger)
		if err != nil {
			return err
		}
		firstTxNum = lastTxNum + 1
	}
	return nil
}

func dumpBlocksRange(ctx context.Context, blockFrom, blockTo uint64, tmpDir, snapDir string, firstTxNum uint64, chainDB kv.RoDB, chainConfig *chain.Config, workers int, lvl log.Lvl, logger log.Logger) (lastTxNum uint64, err error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	if _, err = dumpRange(ctx, coresnaptype.Headers.FileInfo(snapDir, blockFrom, blockTo),
		DumpHeaders, nil, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return 0, err
	}

	if lastTxNum, err = dumpRange(ctx, coresnaptype.Bodies.FileInfo(snapDir, blockFrom, blockTo),
		DumpBodies, func(context.Context) uint64 { return firstTxNum }, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return lastTxNum, err
	}

	if _, err = dumpRange(ctx, coresnaptype.Transactions.FileInfo(snapDir, blockFrom, blockTo),
		DumpTxs, func(context.Context) uint64 { return firstTxNum }, chainDB, chainConfig, tmpDir, workers, lvl, logger); err != nil {
		return lastTxNum, err
	}

	return lastTxNum, nil
}

type firstKeyGetter func(ctx context.Context) uint64
type dumpFunc func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collecter func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error)

var BlockCompressCfg = seg.Cfg{
	MinPatternScore: 1_000,
	MinPatternLen:   8, // `5` - reducing ratio because producing too much prefixes
	MaxPatternLen:   128,
	SamplingFactor:  4,         // not 1 - just to save my time
	MaxDictPatterns: 16 * 1024, // the lower RAM used by huffman tree (arrays)

	DictReducerSoftLimit: 1_000_000,
	Workers:              1,
}

func dumpRange(ctx context.Context, f snaptype.FileInfo, dumper dumpFunc, firstKey firstKeyGetter, chainDB kv.RoDB, chainConfig *chain.Config, tmpDir string, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
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
		return lastKeyValue, fmt.Errorf("DumpBodies: %w", err)
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

	chainID, _ := uint256.FromBig(chainConfig.ChainID)

	numBuf := make([]byte, 8)

	parse := func(ctx *types2.TxParseContext, v, valueBuf []byte, senders []common2.Address, j int) ([]byte, error) {
		var sender [20]byte
		slot := types2.TxSlot{}

		if _, err := ctx.ParseTransaction(v, 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
			return valueBuf, err
		}
		if len(senders) > 0 {
			sender = senders[j]
		}

		valueBuf = valueBuf[:0]
		valueBuf = append(valueBuf, slot.IDHash[:1]...)
		valueBuf = append(valueBuf, sender[:]...)
		valueBuf = append(valueBuf, v...)
		return valueBuf, nil
	}

	addSystemTx := func(ctx *types2.TxParseContext, tx kv.Tx, txId types.BaseTxnID) error {
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

		ctx.WithSender(false)

		valueBuf := bufPool.Get().(*[16 * 4096]byte)
		defer bufPool.Put(valueBuf)

		parsed, err := parse(ctx, tv, valueBuf[:], nil, 0)
		if err != nil {
			return err
		}
		if err := collect(parsed); err != nil {
			return err
		}
		return nil
	}

	doWarmup, warmupTxs, warmupSenders := blockTo-blockFrom >= 100_000 && workers > 4, &atomic.Bool{}, &atomic.Bool{}
	from := hexutility.EncodeTs(blockFrom)
	if err := kv.BigChunks(db, kv.HeaderCanonical, from, func(tx kv.Tx, k, v []byte) (bool, error) {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= blockTo { // [from, to)
			return false, nil
		}

		h := common2.BytesToHash(v)
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
			clean := kv.ReadAhead(warmupCtx, db, warmupSenders, kv.Senders, hexutility.EncodeTs(blockNum), 10_000)
			defer clean()
		}
		if doWarmup && !warmupTxs.Load() && blockNum%1_000 == 0 {
			clean := kv.ReadAhead(warmupCtx, db, warmupTxs, kv.EthTx, body.BaseTxnID.Bytes(), 100*10_000)
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
			if int(body.TxCount-2) > 1 {
				workers = int(body.TxCount - 2)
			} else {
				workers = 1
			}
		}

		parsers := errgroup.Group{}
		parsers.SetLimit(workers)

		valueBufs := make([][]byte, workers)
		parseCtxs := make([]*types2.TxParseContext, workers)

		for i := 0; i < workers; i++ {
			valueBuf := bufPool.Get().(*[16 * 4096]byte)
			defer bufPool.Put(valueBuf)
			valueBufs[i] = valueBuf[:]
			parseCtxs[i] = types2.NewTxParseContext(*chainID)
		}

		if err := addSystemTx(parseCtxs[0], tx, body.BaseTxnID); err != nil {
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
				parseCtx := parseCtxs[tx%workers]

				parseCtx.WithSender(len(senders) == 0)
				parseCtx.WithAllowPreEip2s(blockNum <= chainConfig.HomesteadBlock.Uint64())

				valueBuf, err := parse(parseCtx, tv, valueBufs[tx%workers], senders, tx)

				if err != nil {
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

		if err := addSystemTx(parseCtxs[0], tx, types.BaseTxnID(body.BaseTxnID.LastSystemTx(body.TxCount))); err != nil {
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
			logger.Log(lvl, "[snapshots] Dumping txs", "block num", blockNum,
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return 0, fmt.Errorf("BigChunks: %w", err)
	}
	return 0, nil
}

// DumpHeaders - [from, to)
func DumpHeaders(ctx context.Context, db kv.RoDB, _ *chain.Config, blockFrom, blockTo uint64, _ firstKeyGetter, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	key := make([]byte, 8+32)
	from := hexutility.EncodeTs(blockFrom)
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
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
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
	from := hexutility.EncodeTs(blockFrom)

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
				"alloc", common2.ByteCount(m.Alloc), "sys", common2.ByteCount(m.Sys),
			)
		default:
		}
		return true, nil
	}); err != nil {
		return lastTxNum, err
	}

	return lastTxNum, nil
}

func ForEachHeader(ctx context.Context, s *RoSnapshots, walker func(header *types.Header) error) error {
	r := bytes.NewReader(nil)
	word := make([]byte, 0, 2*4096)

	view := s.View()
	defer view.Close()

	for _, sn := range view.Headers() {
		if err := sn.src.WithReadAhead(func() error {
			g := sn.src.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				var header types.Header
				r.Reset(word[1:])
				if err := rlp.Decode(r, &header); err != nil {
					return err
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

type Merger struct {
	lvl             log.Lvl
	compressWorkers int
	tmpDir          string
	chainConfig     *chain.Config
	chainDB         kv.RoDB
	logger          log.Logger
	noFsync         bool // fsync is enabled by default, but tests can manually disable
}

func NewMerger(tmpDir string, compressWorkers int, lvl log.Lvl, chainDB kv.RoDB, chainConfig *chain.Config, logger log.Logger) *Merger {
	return &Merger{tmpDir: tmpDir, compressWorkers: compressWorkers, lvl: lvl, chainDB: chainDB, chainConfig: chainConfig, logger: logger}
}
func (m *Merger) DisableFsync() { m.noFsync = true }

func (m *Merger) FindMergeRanges(currentRanges []snapshotsync.Range, maxBlockNum uint64) (toMerge []snapshotsync.Range) {
	cfg := snapcfg.KnownCfg(m.chainConfig.ChainName)
	for i := len(currentRanges) - 1; i > 0; i-- {
		r := currentRanges[i]
		mergeLimit := snapcfg.MergeLimitFromCfg(cfg, snaptype.Unknown, r.From())
		if r.To()-r.From() >= mergeLimit {
			continue
		}
		for _, span := range snapcfg.MergeStepsFromCfg(cfg, snaptype.Unknown, r.From()) {
			if r.To()%span != 0 {
				continue
			}
			if r.To()-r.From() == span {
				break
			}
			aggFrom := r.To() - span
			toMerge = append(toMerge, snapshotsync.NewRange(aggFrom, r.To()))
			for currentRanges[i].From() > aggFrom {
				i--
			}
			break
		}
	}
	slices.SortFunc(toMerge, func(i, j snapshotsync.Range) int { return cmp.Compare(i.From(), j.From()) })
	return toMerge
}

func (m *Merger) filesByRange(v *View, from, to uint64) (map[snaptype.Enum][]*DirtySegment, error) {
	toMerge := map[snaptype.Enum][]*DirtySegment{}
	v.VisibleSegments.Scan(func(key snaptype.Enum, value *segmentsRotx) bool {
		toMerge[key.Type().Enum()] = m.filesByRangeOfType(v, from, to, key.Type())
		return true
	})

	return toMerge, nil
}

func (m *Merger) filesByRangeOfType(view *View, from, to uint64, snapshotType snaptype.Type) (out []*DirtySegment) {
	for _, sn := range view.segments(snapshotType) {
		if sn.from < from {
			continue
		}
		if sn.To() > to {
			break
		}

		out = append(out, sn.src)
	}
	return
}

func (m *Merger) mergeSubSegment(ctx context.Context, v *View, sn snaptype.FileInfo, toMerge []*DirtySegment, snapDir string, doIndex bool, onMerge func(r Range) error) (newDirtySegment *DirtySegment, err error) {
	defer func() {
		if err == nil {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic: %v", rec)
			}
		}
		if err != nil {
			f := sn.Path
			_ = os.Remove(f)
			_ = os.Remove(f + ".torrent")
			ext := filepath.Ext(f)
			withoutExt := f[:len(f)-len(ext)]
			_ = os.Remove(withoutExt + ".idx")
			isTxnType := strings.HasSuffix(withoutExt, coresnaptype.Transactions.Name())
			if isTxnType {
				_ = os.Remove(withoutExt + "-to-block.idx")
			}
		}
	}()
	if len(toMerge) == 0 {
		return
	}
	if newDirtySegment, err = m.merge(ctx, v, toMerge, sn, snapDir, nil); err != nil {
		err = fmt.Errorf("mergeByAppendSegments: %w", err)
		return
	}

	// new way to build index
	if doIndex {
		p := &background.Progress{}
		if err = buildIdx(ctx, sn, indexBuilder, m.chainConfig, m.tmpDir, p, m.lvl, m.logger); err != nil {
			return
		}
		err = newDirtySegment.reopenIdx(snapDir)
		if err != nil {
			return
		}
	}

	return
}

// Merge does merge segments in given ranges
func (m *Merger) Merge(ctx context.Context, snapshots *RoSnapshots, snapTypes []snaptype.Type, mergeRanges []Range, snapDir string, doIndex bool, onMerge func(r Range) error, onDelete func(l []string) error) (err error) {
	v := snapshots.View()
	defer v.Close()

	if len(mergeRanges) == 0 {
		return nil
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	in := make(map[snaptype.Enum][]*DirtySegment)
	out := make(map[snaptype.Enum][]*DirtySegment)

	for _, r := range mergeRanges {
		toMerge, err := m.filesByRange(v, r.from, r.to)
		if err != nil {
			return err
		}
		for snapType, t := range toMerge {
			if out[snapType] == nil {
				out[snapType] = make([]*DirtySegment, 0, len(t))
			}
			out[snapType] = append(out[snapType], t...)
		}

		for _, t := range snapTypes {
			newDirtySegment, err := m.mergeSubSegment(ctx, v, t.FileInfo(snapDir, r.from, r.to), toMerge[t.Enum()], snapDir, doIndex, onMerge)
			if err != nil {
				return err
			}
			if in[t.Enum()] == nil {
				in[t.Enum()] = make([]*DirtySegment, 0, len(toMerge[t.Enum()]))
			}
			in[t.Enum()] = append(in[t.Enum()], newDirtySegment)
		}

		snapshots.LogStat("merge")

		if onMerge != nil {
			if err := onMerge(r); err != nil {
				return err
			}
		}

		for _, t := range snapTypes {
			if len(toMerge[t.Enum()]) == 0 {
				continue
			}
			toMergeFilePaths := make([]string, 0, len(toMerge[t.Enum()]))
			for _, f := range toMerge[t.Enum()] {
				toMergeFilePaths = append(toMergeFilePaths, f.FilePath())
			}
			if onDelete != nil {
				if err := onDelete(toMergeFilePaths); err != nil {
					return err
				}
			}
		}
	}
	m.integrateMergedDirtyFiles(snapshots, in, out)
	m.logger.Log(m.lvl, "[snapshots] Merge done", "from", mergeRanges[0].from, "to", mergeRanges[0].to)
	return nil
}

func (m *Merger) integrateMergedDirtyFiles(snapshots *RoSnapshots, in, out map[snaptype.Enum][]*DirtySegment) {
	defer snapshots.recalcVisibleFiles()

	snapshots.dirtySegmentsLock.Lock()
	defer snapshots.dirtySegmentsLock.Unlock()

	// add new segments
	for enum, newSegs := range in {
		segs, b := snapshots.segments.Get(enum)
		if !b {
			m.logger.Error("[snapshots] Merge: segment not found", "enum", enum)
			continue
		}
		dirtySegments := segs.DirtySegments
		for _, newSeg := range newSegs {
			dirtySegments.Set(newSeg)
			if newSeg.frozen {
				dirtySegments.Walk(func(items []*DirtySegment) bool {
					for _, item := range items {
						if item.frozen || item.to > newSeg.to {
							continue
						}
						if out[enum] == nil {
							out[enum] = make([]*DirtySegment, 0, 1)
						}
						out[enum] = append(out[enum], item)
					}
					return true
				})
			}
		}
	}

	// delete old sub segments
	for enum, delSegs := range out {
		segs, b := snapshots.segments.Get(enum)
		if !b {
			m.logger.Error("[snapshots] Merge: segment not found", "enum", enum)
			continue
		}
		dirtySegments := segs.DirtySegments
		for _, delSeg := range delSegs {
			dirtySegments.Delete(delSeg)
			delSeg.canDelete.Store(true)
			if delSeg.refcount.Load() == 0 {
				delSeg.closeAndRemoveFiles()
			}
		}
	}
}

func (m *Merger) merge(ctx context.Context, v *View, toMerge []*DirtySegment, targetFile snaptype.FileInfo, snapDir string, logEvery *time.Ticker) (*DirtySegment, error) {
	var word = make([]byte, 0, 4096)
	var expectedTotal int
	cList := make([]*seg.Decompressor, len(toMerge))
	for i, cFile := range toMerge {
		d, err := seg.NewDecompressor(cFile.FilePath())
		if err != nil {
			return nil, err
		}
		defer d.Close()
		cList[i] = d
		expectedTotal += d.Count()
	}

	compresCfg := seg.DefaultCfg
	compresCfg.Workers = m.compressWorkers
	f, err := seg.NewCompressor(ctx, "Snapshots merge", targetFile.Path, m.tmpDir, compresCfg, log.LvlTrace, m.logger)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if m.noFsync {
		f.DisableFsync()
	}
	m.logger.Debug("[snapshots] merge", "file", targetFile.Name())

	for _, d := range cList {
		if err := d.WithReadAhead(func() error {
			g := d.MakeGetter()
			for g.HasNext() {
				word, _ = g.Next(word[:0])
				if err := f.AddWord(word); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if f.Count() != expectedTotal {
		return nil, fmt.Errorf("unexpected amount after segments merge. got: %d, expected: %d", f.Count(), expectedTotal)
	}
	if err = f.Compress(); err != nil {
		return nil, err
	}
	sn := &DirtySegment{segType: targetFile.Type, version: targetFile.Version, Range: Range{targetFile.From, targetFile.To},
		frozen: snapcfg.Seedable(v.s.cfg.ChainName, targetFile)}

	err = sn.reopenSeg(snapDir)
	if err != nil {
		return nil, err
	}
	return sn, nil
}

type View struct {
	base *snapshotsync.View
}

func (s *RoSnapshots) View() *View {
	return &View{base: s.RoSnapshots.View().WithBaseSegType(coresnaptype.Transactions)}
}

func (v *View) Close() {
	v.base.Close()
}

func (v *View) Headers() []*snapshotsync.Segment { return v.base.Segments(coresnaptype.Headers) }
func (v *View) Bodies() []*snapshotsync.Segment  { return v.base.Segments(coresnaptype.Bodies) }
func (v *View) Txs() []*snapshotsync.Segment     { return v.base.Segments(coresnaptype.Transactions) }

func (v *View) HeadersSegment(blockNum uint64) (*VisibleSegment, bool) {
	return v.Segment(coresnaptype.Headers, blockNum)
}

func (v *View) BodiesSegment(blockNum uint64) (*VisibleSegment, bool) {
	return v.Segment(coresnaptype.Bodies, blockNum)
}
func (v *View) TxsSegment(blockNum uint64) (*VisibleSegment, bool) {
	return v.Segment(coresnaptype.Transactions, blockNum)
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
				if err = os.Remove(fPath); err != nil {
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
