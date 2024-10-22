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
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
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
	return &RoSnapshots{*snapshotsync.NewRoSnapshots(cfg, snapDir, coresnaptype.BlockSnapshotTypes, segmentsMin, true, logger)}
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

func Segments(dir string, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []snapshotsync.Range, err error) {
	return snapshotsync.TypedSegments(dir, minBlock, coresnaptype.BlockSnapshotTypes, true)
}

func SegmentsCaplin(dir string, minBlock uint64) (res []snaptype.FileInfo, missingSnapshots []snapshotsync.Range, err error) {
	list, err := snaptype.Segments(dir)
	if err != nil {
		return nil, missingSnapshots, err
	}

	{
		var l, lSidecars []snaptype.FileInfo
		var m []snapshotsync.Range
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
		l, m = snapshotsync.NoGaps(snapshotsync.NoOverlaps(l))
		if len(m) > 0 {
			lst := m[len(m)-1]
			log.Debug("[snapshots] see gap", "type", snaptype.CaplinEnums.BeaconBlocks, "from", lst.From())
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
	config      *ethconfig.Config

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
	config *ethconfig.Config,
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
		config:         config,
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
	return snapshotsync.CanRetire(blockFrom, curBlockNum-keep, snapType, chainConfig)
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

		if err := snapshots.OpenFolder(); err != nil {
			return ok, fmt.Errorf("open: %w", err)
		}
		snapshots.LogStat("blocks:retire")
		if notifier != nil && !reflect.ValueOf(notifier).IsNil() { // notify about new snapshots of any size
			notifier.OnNewSnapshot()
		}
	}

	merger := snapshotsync.NewMerger(tmpDir, workers, lvl, db, br.chainConfig, logger)
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

				pruneTx := tx
				if br.config.PolygonSync {
					pruneTx = nil
				}

				return bordb.PruneHeimdall(context.Background(),
					br.heimdallStore, br.bridgeStore, pruneTx, canDeleteTo, limit)
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
		if err := sn.Src().WithReadAhead(func() error {
			g := sn.Src().MakeGetter()
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

type View struct {
	base *snapshotsync.View
}

func (s *RoSnapshots) View() *View {
	return &View{base: s.RoSnapshots.View().WithBaseSegType(coresnaptype.Transactions)}
}

func (v *View) Close() {
	v.base.Close()
}

func (v *View) Headers() []*snapshotsync.VisibleSegment { return v.base.Segments(coresnaptype.Headers) }
func (v *View) Bodies() []*snapshotsync.VisibleSegment  { return v.base.Segments(coresnaptype.Bodies) }
func (v *View) Txs() []*snapshotsync.VisibleSegment {
	return v.base.Segments(coresnaptype.Transactions)
}

func (v *View) HeadersSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(coresnaptype.Headers, blockNum)
}

func (v *View) BodiesSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(coresnaptype.Bodies, blockNum)
}
func (v *View) TxsSegment(blockNum uint64) (*snapshotsync.VisibleSegment, bool) {
	return v.base.Segment(coresnaptype.Transactions, blockNum)
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
