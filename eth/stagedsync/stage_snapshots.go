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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/erigontech/erigon-lib/downloader"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/etl"
	protodownloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/state/stats"
	"github.com/erigontech/erigon/core/rawdb"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/silkworm"
	"github.com/erigontech/erigon/turbo/snapshotsync"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

const (
	/*
		we strive to read indexes from snapshots instead to db... this means that there can be sometimes (e.g when we merged past indexes),
		a situation when we need to read indexes and we choose to read them from either a corrupt index or an incomplete index.
		so we need to extend the threshold to > max_merge_segment_size.
	*/
	pruneMarkerSafeThreshold = snaptype.Erigon2MergeLimit * 1.5 // 1.5x the merge limit
)

type SnapshotsCfg struct {
	db          kv.RwDB
	chainConfig chain.Config
	dirs        datadir.Dirs

	blockRetire        services.BlockRetire
	snapshotDownloader protodownloader.DownloaderClient
	blockReader        services.FullBlockReader
	notifier           *shards.Notifications

	caplin           bool
	blobs            bool
	caplinState      bool
	silkworm         *silkworm.Silkworm
	snapshotUploader *snapshotUploader
	syncConfig       ethconfig.Sync
	prune            prune.Mode
}

func StageSnapshotsCfg(db kv.RwDB,
	chainConfig chain.Config,
	syncConfig ethconfig.Sync,
	dirs datadir.Dirs,
	blockRetire services.BlockRetire,
	snapshotDownloader protodownloader.DownloaderClient,
	blockReader services.FullBlockReader,
	notifier *shards.Notifications,
	caplin bool,
	blobs bool,
	caplinState bool,
	silkworm *silkworm.Silkworm,
	prune prune.Mode,
) SnapshotsCfg {
	cfg := SnapshotsCfg{
		db:                 db,
		chainConfig:        chainConfig,
		dirs:               dirs,
		blockRetire:        blockRetire,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		notifier:           notifier,
		caplin:             caplin,
		silkworm:           silkworm,
		syncConfig:         syncConfig,
		blobs:              blobs,
		prune:              prune,
		caplinState:        caplinState,
	}

	if uploadFs := cfg.syncConfig.UploadLocation; len(uploadFs) > 0 {

		cfg.snapshotUploader = &snapshotUploader{
			cfg:          &cfg,
			uploadFs:     uploadFs,
			torrentFiles: downloader.NewAtomicTorrentFS(cfg.dirs.Snap),
		}

		cfg.blockRetire.SetWorkers(estimate.CompressSnapshot.Workers())

		freezingCfg := cfg.blockReader.FreezingCfg()

		if freezingCfg.ProduceE2 {
			u := cfg.snapshotUploader

			if maxSeedable := u.maxSeedableHeader(); u.cfg.syncConfig.FrozenBlockLimit > 0 && maxSeedable > u.cfg.syncConfig.FrozenBlockLimit {
				blockLimit := maxSeedable - u.minBlockNumber()

				if u.cfg.syncConfig.FrozenBlockLimit < blockLimit {
					blockLimit = u.cfg.syncConfig.FrozenBlockLimit
				}

				if snapshots, ok := u.cfg.blockReader.Snapshots().(*freezeblocks.RoSnapshots); ok {
					snapshots.SetSegmentsMin(maxSeedable - blockLimit)
				}

				if snapshots, ok := u.cfg.blockReader.BorSnapshots().(*heimdall.RoSnapshots); ok {
					snapshots.SetSegmentsMin(maxSeedable - blockLimit)
				}
			}
		}
	}

	return cfg
}

func SpawnStageSnapshots(
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg SnapshotsCfg,
	logger log.Logger,
) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if err := DownloadAndIndexSnapshotsIfNeed(s, ctx, tx, cfg, logger); err != nil {
		return err
	}
	var minProgress uint64
	for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.Senders, stages.TxLookup} {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return err
		}
		if minProgress == 0 || progress < minProgress {
			minProgress = progress
		}

		if stage == stages.SyncStage(cfg.syncConfig.BreakAfterStage) {
			break
		}
	}

	if minProgress > s.BlockNumber {
		if err = s.Update(tx, minProgress); err != nil {
			return err
		}
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	// call this after the tx is commited otherwise observing
	// components see an inconsistent db view
	if !cfg.blockReader.Snapshots().DownloadReady() {
		cfg.blockReader.Snapshots().DownloadComplete()
	}
	if cfg.chainConfig.Bor != nil && !cfg.blockReader.BorSnapshots().DownloadReady() {
		cfg.blockReader.BorSnapshots().DownloadComplete()
	}

	return nil
}

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg SnapshotsCfg, logger log.Logger) error {
	if cfg.snapshotUploader != nil {
		u := cfg.snapshotUploader

		u.init(ctx, logger)

		if cfg.syncConfig.UploadFrom != rpc.EarliestBlockNumber {
			u.downloadLatestSnapshots(ctx, cfg.syncConfig.UploadFrom)
		}

		if maxSeedable := u.maxSeedableHeader(); u.cfg.syncConfig.FrozenBlockLimit > 0 && maxSeedable > u.cfg.syncConfig.FrozenBlockLimit {
			blockLimit := maxSeedable - u.minBlockNumber()

			if cfg.syncConfig.FrozenBlockLimit < blockLimit {
				blockLimit = u.cfg.syncConfig.FrozenBlockLimit
			}

			cfg.blockReader.Snapshots().SetSegmentsMin(maxSeedable - blockLimit)
			if cfg.chainConfig.Bor != nil {
				cfg.blockReader.BorSnapshots().SetSegmentsMin(maxSeedable - blockLimit)
			}
		}

		if err := cfg.blockReader.Snapshots().OpenFolder(); err != nil {
			return err
		}

		if cfg.chainConfig.Bor != nil {
			if err := cfg.blockReader.BorSnapshots().OpenFolder(); err != nil {
				return err
			}
		}
	}

	if !s.CurrentSyncCycle.IsFirstCycle {
		return nil
	}

	diagnostics.Send(diagnostics.CurrentSyncStage{Stage: string(stages.Snapshots)})

	cstate := snapshotsync.NoCaplin
	if cfg.caplin {
		cstate = snapshotsync.AlsoCaplin
	}

	subStages := diagnostics.InitSubStagesFromList([]string{"Download header-chain", "Download snapshots", "E2 Indexing", "E3 Indexing", "Fill DB"})
	diagnostics.Send(diagnostics.SetSyncSubStageList{
		Stage: string(stages.Snapshots),
		List:  subStages,
	})

	diagnostics.Send(diagnostics.CurrentSyncSubStage{SubStage: "Download header-chain"})
	agg := cfg.db.(*temporal.DB).Agg().(*state2.Aggregator)
	// Download only the snapshots that are for the header chain.
	if err := snapshotsync.WaitForDownloader(ctx, s.LogPrefix(), cfg.dirs, true, cfg.blobs, cfg.caplinState, cfg.prune, cstate, agg, tx, cfg.blockReader, &cfg.chainConfig, cfg.snapshotDownloader, cfg.syncConfig); err != nil {
		return err
	}

	if err := cfg.blockReader.Snapshots().OpenSegments([]snaptype.Type{coresnaptype.Headers, coresnaptype.Bodies}, true, false); err != nil {
		return err
	}

	diagnostics.Send(diagnostics.CurrentSyncSubStage{SubStage: "Download snapshots"})
	if err := snapshotsync.WaitForDownloader(ctx, s.LogPrefix(), cfg.dirs, false, cfg.blobs, cfg.caplinState, cfg.prune, cstate, agg, tx, cfg.blockReader, &cfg.chainConfig, cfg.snapshotDownloader, cfg.syncConfig); err != nil {
		return err
	}
	if cfg.notifier.Events != nil {
		cfg.notifier.Events.OnNewSnapshot()
	}

	diagnostics.Send(diagnostics.CurrentSyncSubStage{SubStage: "E2 Indexing"})
	if err := cfg.blockRetire.BuildMissedIndicesIfNeed(ctx, s.LogPrefix(), cfg.notifier.Events); err != nil {
		return err
	}

	if cfg.silkworm != nil {
		if err := cfg.blockReader.Snapshots().(silkworm.CanAddSnapshotsToSilkwarm).AddSnapshotsToSilkworm(cfg.silkworm); err != nil {
			return err
		}
	}

	indexWorkers := estimate.IndexSnapshot.Workers()
	diagnostics.Send(diagnostics.CurrentSyncSubStage{SubStage: "E3 Indexing"})
	if err := agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
		return err
	}

	if temporal, ok := tx.(*temporal.Tx); ok {
		temporal.ForceReopenAggCtx() // otherwise next stages will not see just-indexed-files
	}

	// It's ok to notify before tx.Commit(), because RPCDaemon does read list of files by gRPC (not by reading from db)
	if cfg.notifier.Events != nil {
		cfg.notifier.Events.OnNewSnapshot()
	}

	frozenBlocks := cfg.blockReader.FrozenBlocks()
	if s.BlockNumber < frozenBlocks { // allow genesis
		if err := s.Update(tx, frozenBlocks); err != nil {
			return err
		}
		s.BlockNumber = frozenBlocks
	}

	diagnostics.Send(diagnostics.CurrentSyncSubStage{SubStage: "Fill DB"})
	if err := FillDBFromSnapshots(s.LogPrefix(), ctx, tx, cfg.dirs, cfg.blockReader, logger); err != nil {
		return fmt.Errorf("FillDBFromSnapshots: %w", err)
	}

	if temporal, ok := tx.(*temporal.Tx); ok {
		temporal.ForceReopenAggCtx() // otherwise next stages will not see just-indexed-files
	}

	cfg.blockReader.Snapshots().LogStat("download")
	txNumsReader := cfg.blockReader.TxnumReader(ctx)
	if temporal, ok := tx.(*temporal.Tx); ok {
		stats.LogStats(temporal, logger, func(endTxNumMinimax uint64) (uint64, error) {
			histBlockNumProgress, _, err := txNumsReader.FindBlockNum(tx, endTxNumMinimax)
			return histBlockNumProgress, err
		})
	}

	return nil
}

func getPruneMarkerSafeThreshold(blockReader services.FullBlockReader) uint64 {
	snapProgress := min(blockReader.FrozenBorBlocks(), blockReader.FrozenBlocks())
	if blockReader.BorSnapshots() == nil {
		snapProgress = blockReader.FrozenBlocks()
	}
	if snapProgress < pruneMarkerSafeThreshold {
		return 0
	}
	return snapProgress - pruneMarkerSafeThreshold
}

func FillDBFromSnapshots(logPrefix string, ctx context.Context, tx kv.RwTx, dirs datadir.Dirs, blockReader services.FullBlockReader, logger log.Logger) error {
	startTime := time.Now()
	blocksAvailable := blockReader.FrozenBlocks()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	pruneMarkerBlockThreshold := getPruneMarkerSafeThreshold(blockReader)

	// updating the progress of further stages (but only forward) that are contained inside of snapshots
	// We don't set StageExec progress here - even that we have a lot of state snapshots, because:
	//   state files (.kv/.v/.ef) may be at "half-block" progress (sharded by `txNum`). And only StageExec
	//   has logic to handle this corner-case right
	for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.BlockHashes, stages.Senders} {
		progress, err := stages.GetStageProgress(tx, stage)

		if err != nil {
			return fmt.Errorf("get %s stage progress to advance: %w", stage, err)
		}
		if progress >= blocksAvailable {
			continue
		}

		if err = stages.SaveStageProgress(tx, stage, blocksAvailable); err != nil {
			return fmt.Errorf("advancing %s stage: %w", stage, err)
		}

		switch stage {
		case stages.Headers:
			h2n := etl.NewCollector(logPrefix, dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize/2), logger)
			defer h2n.Close()
			h2n.SortAndFlushInBackground(true)
			h2n.LogLvl(log.LvlDebug)

			// fill some small tables from snapshots, in future we may store this data in snapshots also, but
			// for now easier just store them in db
			td := big.NewInt(0)
			blockNumBytes := make([]byte, 8)
			if err := blockReader.HeadersRange(ctx, func(header *types.Header) error {
				blockNum, blockHash := header.Number.Uint64(), header.Hash()
				td.Add(td, header.Difficulty)
				// What can happen if chaindata is deleted is that maybe header.seg progress is lower or higher than
				// body.seg progress. In this case we need to skip the header, and "normalize" the progress to keep them in sync.
				if blockNum > blocksAvailable {
					return nil // This can actually happen as FrozenBlocks() is SegmentIdMax() and not the last .seg
				}
				if !dbg.PruneTotalDifficulty() {
					if err := rawdb.WriteTd(tx, blockHash, blockNum, td); err != nil {
						return err
					}
				}

				// Write marker for pruning only if we are above our safe threshold
				if blockNum >= pruneMarkerBlockThreshold || blockNum == 0 {
					if err := rawdb.WriteCanonicalHash(tx, blockHash, blockNum); err != nil {
						return err
					}
					binary.BigEndian.PutUint64(blockNumBytes, blockNum)
					if err := h2n.Collect(blockHash[:], blockNumBytes); err != nil {
						return err
					}
					if dbg.PruneTotalDifficulty() {
						if err := rawdb.WriteTd(tx, blockHash, blockNum, td); err != nil {
							return err
						}
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					diagnostics.Send(diagnostics.SnapshotFillDBStageUpdate{
						Stage: diagnostics.SnapshotFillDBStage{
							StageName: string(stage),
							Current:   header.Number.Uint64(),
							Total:     blocksAvailable,
						},
						TimeElapsed: time.Since(startTime).Seconds(),
					})
					logger.Info(fmt.Sprintf("[%s] Total difficulty index: %s/%s", logPrefix,
						common.PrettyCounter(header.Number.Uint64()), common.PrettyCounter(blockReader.FrozenBlocks())))
				default:
				}
				return nil
			}); err != nil {
				return err
			}
			if err := h2n.Load(tx, kv.HeaderNumber, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
				return err
			}
			canonicalHash, ok, err := blockReader.CanonicalHash(ctx, tx, blocksAvailable)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("canonical marker not found: %d", blocksAvailable)
			}
			if err = rawdb.WriteHeadHeaderHash(tx, canonicalHash); err != nil {
				return err
			}

		case stages.Bodies:
			firstTxNum := blockReader.FirstTxnNumNotInSnapshots()
			// ResetSequence - allow set arbitrary value to sequence (for example to decrement it to exact value)
			if err := rawdb.ResetSequence(tx, kv.EthTx, firstTxNum); err != nil {
				return err
			}

			_ = tx.ClearBucket(kv.MaxTxNum)
			if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					diagnostics.Send(diagnostics.SnapshotFillDBStageUpdate{
						Stage: diagnostics.SnapshotFillDBStage{
							StageName: string(stage),
							Current:   blockNum,
							Total:     blocksAvailable,
						},
						TimeElapsed: time.Since(startTime).Seconds(),
					})
					logger.Info(fmt.Sprintf("[%s] MaxTxNums index: %s/%s", logPrefix, common.PrettyCounter(blockNum), common.PrettyCounter(blockReader.FrozenBlocks())))
				default:
				}
				if baseTxNum+txAmount == 0 {
					panic(baseTxNum + txAmount) //uint-underflow
				}
				maxTxNum := baseTxNum + txAmount - 1
				// What can happen if chaindata is deleted is that maybe header.seg progress is lower or higher than
				// body.seg progress. In this case we need to skip the header, and "normalize" the progress to keep them in sync.
				if blockNum > blocksAvailable {
					return nil // This can actually happen as FrozenBlocks() is SegmentIdMax() and not the last .seg
				}
				if blockNum >= pruneMarkerBlockThreshold || blockNum == 0 {
					if blockNum < 2000 {
						fmt.Printf("[dbg] TxNum.Max: bn=%d, tn=%d\n", blockNum, maxTxNum)
					}
					if err := rawdbv3.TxNums.Append(tx, blockNum, maxTxNum); err != nil {
						return fmt.Errorf("%w. blockNum=%d, maxTxNum=%d", err, blockNum, maxTxNum)
					}
				}
				return nil
			}); err != nil {
				return fmt.Errorf("build txNum => blockNum mapping: %w", err)
			}
			if blockReader.FrozenBlocks() > 0 {
				if err := rawdb.AppendCanonicalTxNums(tx, blockReader.FrozenBlocks()+1); err != nil {
					return err
				}
			} else {
				if err := rawdb.AppendCanonicalTxNums(tx, 0); err != nil {
					return err
				}
			}

		default:
			diagnostics.Send(diagnostics.SnapshotFillDBStageUpdate{
				Stage: diagnostics.SnapshotFillDBStage{
					StageName: string(stage),
					Current:   blocksAvailable, // as we are done with other stages
					Total:     blocksAvailable,
				},
				TimeElapsed: time.Since(startTime).Seconds(),
			})
		}
	}
	return nil
}

func pruneCanonicalMarkers(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader) error {
	pruneThreshold := getPruneMarkerSafeThreshold(blockReader)
	if pruneThreshold == 0 {
		return nil
	}

	c, err := tx.RwCursor(kv.HeaderCanonical) // Number -> Hash
	if err != nil {
		return err
	}
	defer c.Close()
	var tdKey [40]byte
	for k, v, err := c.First(); k != nil && err == nil; k, v, err = c.Next() {
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum == 0 { // Do not prune genesis marker
			continue
		}
		if blockNum >= pruneThreshold {
			break
		}
		if err := tx.Delete(kv.HeaderNumber, v); err != nil {
			return err
		}
		if dbg.PruneTotalDifficulty() {
			copy(tdKey[:], k)
			copy(tdKey[8:], v)
			if err := tx.Delete(kv.HeaderTD, tdKey[:]); err != nil {
				return err
			}
		}
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
	}
	return nil
}

/* ====== PRUNING ====== */
// snapshots pruning sections works more as a retiring of blocks
// retiring blocks means moving block data from db into snapshots
func SnapshotsPrune(s *PruneState, cfg SnapshotsCfg, ctx context.Context, tx kv.RwTx, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	freezingCfg := cfg.blockReader.FreezingCfg()
	if freezingCfg.ProduceE2 {
		//TODO: initialSync maybe save files progress here

		var minBlockNumber uint64

		if cfg.snapshotUploader != nil {
			minBlockNumber = cfg.snapshotUploader.minBlockNumber()
		}

		if s.CurrentSyncCycle.IsInitialCycle {
			cfg.blockRetire.SetWorkers(estimate.CompressSnapshot.Workers())
		} else {
			cfg.blockRetire.SetWorkers(1)
		}

		cfg.blockRetire.RetireBlocksInBackground(ctx, minBlockNumber, s.ForwardProgress, log.LvlDebug, func(downloadRequest []snapshotsync.DownloadRequest) error {
			if cfg.snapshotDownloader != nil && !reflect.ValueOf(cfg.snapshotDownloader).IsNil() {
				if err := snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader, ""); err != nil {
					return err
				}
			}

			return nil
		}, func(l []string) error {
			//if cfg.snapshotUploader != nil {
			// TODO - we need to also remove files from the uploader (100k->500K transition)
			//}

			if !(cfg.snapshotDownloader == nil || reflect.ValueOf(cfg.snapshotDownloader).IsNil()) {
				_, err := cfg.snapshotDownloader.Delete(ctx, &protodownloader.DeleteRequest{Paths: l})
				return err
			}

			return nil
		}, func() error {
			filesDeleted, err := pruneBlockSnapshots(ctx, cfg, logger)
			if filesDeleted && cfg.notifier != nil {
				cfg.notifier.Events.OnNewSnapshot()
			}
			return err
		})

		//	cfg.agg.BuildFilesInBackground()

	}

	pruneLimit := 10
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneLimit = 10_000
	}
	if _, err := cfg.blockRetire.PruneAncientBlocks(tx, pruneLimit); err != nil {
		return err
	}
	if err := pruneCanonicalMarkers(ctx, tx, cfg.blockReader); err != nil {
		return err
	}

	if cfg.snapshotUploader != nil {
		// if we're uploading make sure that the DB does not get too far
		// ahead of the snapshot production process - otherwise DB will
		// grow larger than necessary - we may also want to increase the
		// workers
		if s.ForwardProgress > cfg.blockReader.FrozenBlocks()+300_000 {
			func() {
				checkEvery := time.NewTicker(logInterval)
				defer checkEvery.Stop()

				for s.ForwardProgress > cfg.blockReader.FrozenBlocks()+300_000 {
					select {
					case <-ctx.Done():
						return
					case <-checkEvery.C:
						log.Info(fmt.Sprintf("[%s] Waiting for snapshots...", s.LogPrefix()), "progress", s.ForwardProgress, "frozen", cfg.blockReader.FrozenBlocks(), "gap", s.ForwardProgress-cfg.blockReader.FrozenBlocks())
					}
				}
			}()
		}
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func pruneBlockSnapshots(ctx context.Context, cfg SnapshotsCfg, logger log.Logger) (bool, error) {
	tx, err := cfg.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	// Prune snapshots if necessary (remove .segs or idx files appropriately)
	headNumber := cfg.blockReader.FrozenBlocks()
	executionProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return false, err
	}
	// If we are behind the execution stage, we should not prune snapshots
	if headNumber > executionProgress || !cfg.prune.Blocks.Enabled() {
		return false, nil
	}

	// Keep at least 2 block snapshots as we do not want FrozenBlocks to be 0
	pruneTo := cfg.prune.Blocks.PruneTo(headNumber)

	if pruneTo > executionProgress {
		return false, nil
	}

	snapshotFileNames := cfg.blockReader.FrozenFiles()
	filesDeleted := false
	// Prune blocks snapshots if necessary
	for _, file := range snapshotFileNames {
		if !cfg.prune.Blocks.Enabled() || headNumber == 0 || !strings.Contains(file, "transactions") {
			continue
		}

		// take the snapshot file name and parse it to get the "from"
		info, _, ok := snaptype.ParseFileName(cfg.dirs.Snap, file)
		if !ok {
			continue
		}
		if info.To >= pruneTo {
			continue
		}
		if info.To-info.From != snaptype.Erigon2MergeLimit {
			continue
		}
		if cfg.snapshotDownloader != nil {
			if _, err := cfg.snapshotDownloader.Delete(ctx, &protodownloader.DeleteRequest{Paths: []string{file}}); err != nil {
				return filesDeleted, err
			}
		}
		if err := cfg.blockReader.Snapshots().Delete(file); err != nil {
			return filesDeleted, err
		}
		filesDeleted = true
	}
	return filesDeleted, nil
}

type uploadState struct {
	sync.Mutex
	file             string
	info             *snaptype.FileInfo
	torrent          *torrent.TorrentSpec
	buildingTorrent  bool
	uploads          []string
	remote           bool
	hasRemoteTorrent bool
	//remoteHash       string
	local     bool
	localHash string
}

type snapshotUploader struct {
	cfg             *SnapshotsCfg
	files           map[string]*uploadState
	uploadFs        string
	rclone          *downloader.RCloneClient
	uploadSession   *downloader.RCloneSession
	uploadScheduled atomic.Bool
	uploading       atomic.Bool
	manifestMutex   sync.Mutex
	torrentFiles    *downloader.AtomicTorrentFS
}

func (u *snapshotUploader) init(ctx context.Context, logger log.Logger) {
	if u.files == nil {
		freezingCfg := u.cfg.blockReader.FreezingCfg()

		if freezingCfg.ProduceE2 {
			u.files = map[string]*uploadState{}
			u.start(ctx, logger)
		}
	}
}

func (u *snapshotUploader) maxUploadedHeader() uint64 {
	var _max uint64

	if len(u.files) > 0 {
		for _, state := range u.files {
			if state.local && state.remote {
				if state.info != nil {
					if state.info.Type.Enum() == coresnaptype.Enums.Headers {
						if state.info.To > _max {
							_max = state.info.To
						}
					}
				} else {
					if info, _, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, state.file); ok {
						if info.Type.Enum() == coresnaptype.Enums.Headers {
							if info.To > _max {
								_max = info.To
							}
						}
						state.info = &info
					}
				}
			}
		}
	}

	return _max
}

type dirEntry struct {
	name string
}

type snapInfo struct {
	snaptype.FileInfo
}

func (i *snapInfo) Version() snaptype.Version {
	return i.FileInfo.Version
}

func (i *snapInfo) From() uint64 {
	return i.FileInfo.From
}

func (i *snapInfo) To() uint64 {
	return i.FileInfo.To
}

func (i *snapInfo) Type() snaptype.Type {
	return i.FileInfo.Type
}

func (e dirEntry) Name() string {
	return e.name
}

func (e dirEntry) IsDir() bool {
	return false
}

func (e dirEntry) Type() fs.FileMode {
	return e.Mode()
}

func (e dirEntry) Size() int64 {
	return -1
}

func (e dirEntry) Mode() fs.FileMode {
	return fs.ModeIrregular
}

func (e dirEntry) ModTime() time.Time {
	return time.Time{}
}

func (e dirEntry) Sys() any {
	if info, _, ok := snaptype.ParseFileName("", e.name); ok {
		return &snapInfo{info}
	}

	return nil
}

func (e dirEntry) Info() (fs.FileInfo, error) {
	return e, nil
}

var checkKnownSizes = false

func (u *snapshotUploader) seedable(fi snaptype.FileInfo) bool {
	if !snapcfg.Seedable(u.cfg.chainConfig.ChainName, fi) {
		return false
	}

	if checkKnownSizes {
		for _, it := range snapcfg.KnownCfg(u.cfg.chainConfig.ChainName).Preverified {
			info, _, _ := snaptype.ParseFileName("", it.Name)

			if fi.From == info.From {
				return fi.To == info.To
			}

			if fi.From < info.From {
				return info.To-info.From == fi.To-fi.From
			}

			if fi.From < info.To {
				return false
			}
		}
	}

	return true
}

func (u *snapshotUploader) downloadManifest(ctx context.Context) ([]fs.DirEntry, error) {
	u.manifestMutex.Lock()
	defer u.manifestMutex.Unlock()

	reader, err := u.uploadSession.Cat(ctx, "manifest.txt")

	if err != nil {
		return nil, err
	}

	var entries []fs.DirEntry

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		entries = append(entries, dirEntry{scanner.Text()})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, io.ErrUnexpectedEOF
	}

	return entries, nil
}

func (u *snapshotUploader) uploadManifest(ctx context.Context, remoteRefresh bool) error {
	u.manifestMutex.Lock()
	defer u.manifestMutex.Unlock()

	if remoteRefresh {
		u.refreshFromRemote(ctx)
	}

	manifestFile := "manifest.txt"

	fileMap := map[string]string{}

	for file, state := range u.files {
		if state.remote {
			if state.hasRemoteTorrent {
				fileMap[file] = file + ".torrent"
			} else {
				fileMap[file] = ""
			}
		}
	}

	files := make([]string, 0, len(fileMap))

	for torrent, file := range fileMap {
		files = append(files, file)

		if len(torrent) > 0 {
			files = append(files, torrent)
		}
	}

	sort.Strings(files)

	manifestEntries := bytes.Buffer{}

	for _, file := range files {
		fmt.Fprintln(&manifestEntries, file)
	}

	_ = os.WriteFile(filepath.Join(u.cfg.dirs.Snap, manifestFile), manifestEntries.Bytes(), 0644)
	defer os.Remove(filepath.Join(u.cfg.dirs.Snap, manifestFile))

	return u.uploadSession.Upload(ctx, manifestFile)
}

func (u *snapshotUploader) refreshFromRemote(ctx context.Context) {
	remoteFiles, err := u.uploadSession.ReadRemoteDir(ctx, true)

	if err != nil {
		return
	}

	u.updateRemotes(remoteFiles)
}

func (u *snapshotUploader) updateRemotes(remoteFiles []fs.DirEntry) {
	for _, fi := range remoteFiles {
		var file string
		var hasTorrent bool

		if hasTorrent = filepath.Ext(fi.Name()) == ".torrent"; hasTorrent {
			file = strings.TrimSuffix(fi.Name(), ".torrent")
		} else {
			file = fi.Name()
		}

		// if we have found the file & its torrent we don't
		// need to attempt another sync operation
		if state, ok := u.files[file]; ok {
			state.remote = true

			if hasTorrent {
				state.hasRemoteTorrent = true
			}

		} else {
			info, isStateFile, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, fi.Name())
			if !ok {
				continue
			}
			if isStateFile {
				//TODO
				continue
			}

			u.files[file] = &uploadState{
				file:             file,
				info:             &info,
				local:            dir.FileNonZero(info.Path),
				hasRemoteTorrent: hasTorrent,
			}
		}
	}
}

func (u *snapshotUploader) downloadLatestSnapshots(ctx context.Context, blockNumber rpc.BlockNumber) error {

	entries, err := u.downloadManifest(ctx)

	if err != nil {
		entries, err = u.uploadSession.ReadRemoteDir(ctx, true)
	}

	if err != nil {
		return err
	}

	lastSegments := map[snaptype.Enum]fs.FileInfo{}
	torrents := map[string]string{}

	for _, ent := range entries {
		if info, err := ent.Info(); err == nil {

			if info.Size() > -1 && info.Size() <= 32 {
				continue
			}

			snapInfo, ok := info.Sys().(downloader.SnapInfo)

			if ok && snapInfo.Type() != nil {
				if last, ok := lastSegments[snapInfo.Type().Enum()]; ok {
					if lastInfo, ok := last.Sys().(downloader.SnapInfo); ok && snapInfo.To() > lastInfo.To() {
						lastSegments[snapInfo.Type().Enum()] = info
					}
				} else {
					lastSegments[snapInfo.Type().Enum()] = info
				}
			} else {
				if ext := filepath.Ext(info.Name()); ext == ".torrent" {
					fileName := strings.TrimSuffix(info.Name(), ".torrent")
					torrents[fileName] = info.Name()
				}
			}
		}
	}

	var _min uint64

	for _, info := range lastSegments {
		if lastInfo, ok := info.Sys().(downloader.SnapInfo); ok {
			if _min == 0 || lastInfo.From() < _min {
				_min = lastInfo.From()
			}
		}
	}

	for segType, info := range lastSegments {
		if lastInfo, ok := info.Sys().(downloader.SnapInfo); ok {
			if lastInfo.From() > _min {
				for _, ent := range entries {
					if info, err := ent.Info(); err == nil {
						snapInfo, ok := info.Sys().(downloader.SnapInfo)

						if ok && snapInfo.Type().Enum() == segType &&
							snapInfo.From() == _min {
							lastSegments[segType] = info
						}
					}
				}
			}
		}
	}

	downloads := make([]string, 0, len(lastSegments))

	for _, info := range lastSegments {
		downloads = append(downloads, info.Name())
		if torrent, ok := torrents[info.Name()]; ok {
			downloads = append(downloads, torrent)
		}
	}

	if len(downloads) > 0 {
		return u.uploadSession.Download(ctx, downloads...)
	}

	return nil
}

func (u *snapshotUploader) maxSeedableHeader() uint64 {
	return snapcfg.MaxSeedableSegment(u.cfg.chainConfig.ChainName, u.cfg.dirs.Snap)
}

func (u *snapshotUploader) minBlockNumber() uint64 {
	var _min uint64

	if list, err := snaptype.Segments(u.cfg.dirs.Snap); err == nil {
		for _, info := range list {
			if u.seedable(info) && _min == 0 || info.From < _min {
				_min = info.From
			}
		}
	}

	return _min
}

func expandHomeDir(dirpath string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return dirpath
	}
	prefix := fmt.Sprintf("~%c", os.PathSeparator)
	if strings.HasPrefix(dirpath, prefix) {
		return filepath.Join(home, dirpath[len(prefix):])
	} else if dirpath == "~" {
		return home
	}
	return dirpath
}

func isLocalFs(ctx context.Context, rclient *downloader.RCloneClient, fs string) bool {

	remotes, _ := rclient.ListRemotes(ctx)

	if remote, _, ok := strings.Cut(fs, ":"); ok {
		for _, r := range remotes {
			if remote == r {
				return false
			}
		}

		return filepath.VolumeName(fs) == remote
	}

	return true
}

func (u *snapshotUploader) start(ctx context.Context, logger log.Logger) {
	var err error

	u.rclone, err = downloader.NewRCloneClient(logger)

	if err != nil {
		logger.Warn("[uploader] Uploading disabled: rclone start failed", "err", err)
		return
	}

	uploadFs := u.uploadFs

	if isLocalFs(ctx, u.rclone, uploadFs) {
		uploadFs = expandHomeDir(filepath.Clean(uploadFs))

		uploadFs, err = filepath.Abs(uploadFs)

		if err != nil {
			logger.Warn("[uploader] Uploading disabled: invalid upload fs", "err", err, "fs", u.uploadFs)
			return
		}

		if err := os.MkdirAll(uploadFs, 0755); err != nil {
			logger.Warn("[uploader] Uploading disabled: can't create upload fs", "err", err, "fs", u.uploadFs)
			return
		}
	}

	u.uploadSession, err = u.rclone.NewSession(ctx, u.cfg.dirs.Snap, uploadFs, nil)

	if err != nil {
		logger.Warn("[uploader] Uploading disabled: rclone session failed", "err", err)
		return
	}

	go func() {

		remoteFiles, _ := u.downloadManifest(ctx)
		refreshFromRemote := false

		if len(remoteFiles) > 0 {
			u.updateRemotes(remoteFiles)
			refreshFromRemote = true
		} else {
			u.refreshFromRemote(ctx)
		}

		go u.uploadManifest(ctx, refreshFromRemote)

		logger.Debug("[snapshot uploader] starting snapshot subscription...")
		snapshotSubCh, snapshotSubClean := u.cfg.notifier.Events.AddNewSnapshotSubscription()
		defer snapshotSubClean()

		logger.Info("[snapshot uploader] subscription established")

		defer func() {
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("[snapshot uploader] subscription closed", "reason", err)
				}
			} else {
				logger.Warn("[snapshot uploader] subscription closed")
			}
		}()

		u.scheduleUpload(ctx, logger)

		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			case <-snapshotSubCh:
				logger.Info("[snapshot uploader] new snapshot received")
				u.scheduleUpload(ctx, logger)
			}
		}
	}()
}

func (u *snapshotUploader) scheduleUpload(ctx context.Context, logger log.Logger) {
	if !u.uploadScheduled.CompareAndSwap(false, true) {
		return
	}

	if u.uploading.CompareAndSwap(false, true) {
		go func() {
			defer u.uploading.Store(false)
			for u.uploadScheduled.Load() {
				u.uploadScheduled.Store(false)
				u.upload(ctx, logger)
			}
		}()
	}
}

func (u *snapshotUploader) removeBefore(before uint64) {
	list, err := snaptype.Segments(u.cfg.dirs.Snap)

	if err != nil {
		return
	}

	var toReopen []string
	var borToReopen []string

	toRemove := make([]string, 0, len(list))

	for _, f := range list {
		if f.To > before {
			switch f.Type.Enum() {
			case heimdall.Enums.Events, heimdall.Enums.Spans,
				heimdall.Enums.Checkpoints, heimdall.Enums.Milestones:
				borToReopen = append(borToReopen, filepath.Base(f.Path))
			default:
				toReopen = append(toReopen, filepath.Base(f.Path))
			}

			continue
		}

		toRemove = append(toRemove, f.Path)
	}

	if len(toRemove) > 0 {
		if snapshots, ok := u.cfg.blockReader.Snapshots().(*freezeblocks.RoSnapshots); ok {
			snapshots.SetSegmentsMin(before)
			snapshots.OpenList(toReopen, true)
		}

		if snapshots, ok := u.cfg.blockReader.BorSnapshots().(*heimdall.RoSnapshots); ok {
			snapshots.OpenList(borToReopen, true)
			snapshots.SetSegmentsMin(before)
		}

		for _, f := range toRemove {
			_ = os.Remove(f)
			_ = os.Remove(f + ".torrent")
			ext := filepath.Ext(f)
			withoutExt := f[:len(f)-len(ext)]
			_ = os.Remove(withoutExt + ".idx")

			if strings.HasSuffix(withoutExt, "transactions") {
				_ = os.Remove(withoutExt + "-to-block.idx")
			}
		}
	}
}

func (u *snapshotUploader) upload(ctx context.Context, logger log.Logger) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("[snapshot uploader] snapshot upload failed", "err", r, "stack", dbg.Stack())
		}
	}()

	retryTime := 30 * time.Second
	maxRetryTime := 300 * time.Second

	var uploadCount int

	for {
		var processList []*uploadState

		for _, f := range u.cfg.blockReader.FrozenFiles() {
			if state, ok := u.files[f]; !ok {
				if fi, isStateFile, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, f); ok {
					if isStateFile {
						//TODO
						continue
					}

					if u.seedable(fi) {
						state := &uploadState{
							file:  f,
							info:  &fi,
							local: true,
						}
						exists, err := fi.TorrentFileExists()
						if err != nil {
							logger.Debug("TorrentFileExists error", "err", err)
						}
						if exists {
							state.torrent, _ = u.torrentFiles.LoadByName(f)
						}

						u.files[f] = state
						processList = append(processList, state)
					}
				}
			} else {
				func() {
					state.Lock()
					defer state.Unlock()

					state.local = true
					exists, err := state.info.TorrentFileExists()
					if err != nil {
						logger.Debug("TorrentFileExists error", "err", err)
					}
					if state.torrent == nil && exists {
						state.torrent, _ = u.torrentFiles.LoadByName(f)
						if state.torrent != nil {
							state.localHash = state.torrent.InfoHash.String()
						}
					}

					if !state.remote {
						processList = append(processList, state)
					}
				}()
			}
		}

		var torrentList []*uploadState

		for _, state := range processList {
			func() {
				state.Lock()
				defer state.Unlock()
				if !(state.torrent != nil || state.buildingTorrent) {
					torrentList = append(torrentList, state)
					state.buildingTorrent = true
				}
			}()
		}

		if len(torrentList) > 0 {
			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(runtime.GOMAXPROCS(-1) * 4)
			var i atomic.Int32

			go func() {
				logEvery := time.NewTicker(20 * time.Second)
				defer logEvery.Stop()

				for int(i.Load()) < len(torrentList) {
					select {
					case <-gctx.Done():
						return
					case <-logEvery.C:
						if int(i.Load()) == len(torrentList) {
							return
						}
						log.Info("[snapshot uploader] Creating .torrent files", "progress", fmt.Sprintf("%d/%d", i.Load(), len(torrentList)))
					}
				}
			}()

			for _, s := range torrentList {
				state := s

				g.Go(func() error {
					defer i.Add(1)

					_, err := downloader.BuildTorrentIfNeed(gctx, state.file, u.cfg.dirs.Snap, u.torrentFiles)

					state.Lock()
					state.buildingTorrent = false
					state.Unlock()

					if err != nil {
						return err
					}

					torrent, err := u.torrentFiles.LoadByName(state.file)

					if err != nil {
						return err
					}

					state.Lock()
					state.torrent = torrent
					state.Unlock()

					state.localHash = state.torrent.InfoHash.String()

					logger.Info("[snapshot uploader] built torrent", "file", state.file, "hash", state.localHash)

					return nil
				})
			}

			if err := g.Wait(); err != nil {
				logger.Debug(".torrent file creation failed", "err", err)
			}
		}

		var f atomic.Int32

		var uploadList []*uploadState

		for _, state := range processList {
			err := func() error {
				state.Lock()
				defer state.Unlock()
				if !state.remote && state.torrent != nil && len(state.uploads) == 0 && u.rclone != nil {
					state.uploads = []string{state.file, state.file + ".torrent"}
					uploadList = append(uploadList, state)
				}

				return nil
			}()

			if err != nil {
				logger.Debug("upload failed", "file", state.file, "err", err)
			}
		}

		if len(uploadList) > 0 {
			log.Info("[snapshot uploader] Starting upload", "count", len(uploadList))

			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(16)
			var i atomic.Int32

			go func() {
				logEvery := time.NewTicker(20 * time.Second)
				defer logEvery.Stop()

				for int(i.Load()) < len(processList) {
					select {
					case <-gctx.Done():
						log.Info("[snapshot uploader] Uploaded files", "processed", fmt.Sprintf("%d/%d/%d", i.Load(), len(processList), f.Load()))
						return
					case <-logEvery.C:
						if int(i.Load()+f.Load()) == len(processList) {
							return
						}
						log.Info("[snapshot uploader] Uploading files", "progress", fmt.Sprintf("%d/%d/%d", i.Load(), len(processList), f.Load()))
					}
				}
			}()

			for _, s := range uploadList {
				state := s
				func() {
					state.Lock()
					defer state.Unlock()

					g.Go(func() error {
						defer i.Add(1)
						defer func() {
							state.Lock()
							state.uploads = nil
							state.Unlock()
						}()

						if err := u.uploadSession.Upload(gctx, state.uploads...); err != nil {
							f.Add(1)
							return nil
						}

						uploadCount++

						state.Lock()
						state.remote = true
						state.hasRemoteTorrent = true
						state.Unlock()
						return nil
					})
				}()
			}

			if err := g.Wait(); err != nil {
				logger.Debug("[snapshot uploader] upload failed", "err", err)
			}
		}

		if f.Load() == 0 {
			break
		}

		time.Sleep(retryTime)

		if retryTime < maxRetryTime {
			retryTime += retryTime
		} else {
			retryTime = maxRetryTime
		}
	}

	var err error

	if uploadCount > 0 {
		err = u.uploadManifest(ctx, false)
	}

	if err == nil {
		if maxUploaded := u.maxUploadedHeader(); u.cfg.syncConfig.FrozenBlockLimit > 0 && maxUploaded > u.cfg.syncConfig.FrozenBlockLimit {
			u.removeBefore(maxUploaded - u.cfg.syncConfig.FrozenBlockLimit)
		}
	}
}
