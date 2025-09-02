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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/estimate"
	protodownloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/stats"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/rawdbreset"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/silkworm"
)

type SnapshotsCfg struct {
	db          kv.TemporalRwDB
	chainConfig *chain.Config
	dirs        datadir.Dirs

	blockRetire        services.BlockRetire
	snapshotDownloader protodownloader.DownloaderClient
	blockReader        services.FullBlockReader
	notifier           *shards.Notifications

	caplin      bool
	blobs       bool
	caplinState bool
	silkworm    *silkworm.Silkworm
	syncConfig  ethconfig.Sync
	prune       prune.Mode
}

func StageSnapshotsCfg(db kv.TemporalRwDB,
	chainConfig *chain.Config,
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
	if !s.CurrentSyncCycle.IsFirstCycle {
		return nil
	}

	diaglib.Send(diaglib.CurrentSyncStage{Stage: string(stages.Snapshots)})

	cstate := snapshotsync.NoCaplin
	if cfg.caplin {
		cstate = snapshotsync.AlsoCaplin
	}

	subStages := diaglib.InitSubStagesFromList([]string{"Download header-chain", "Download snapshots", "E2 Indexing", "E3 Indexing", "Fill DB"})
	diaglib.Send(diaglib.SetSyncSubStageList{
		Stage: string(stages.Snapshots),
		List:  subStages,
	})

	log.Info("[OtterSync] Starting Ottersync")
	log.Info(snapshotsync.GreatOtterBanner)

	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "Download header-chain"})
	agg := cfg.db.(*temporal.DB).Agg().(*state.Aggregator)
	// Download only the snapshots that are for the header chain.

	if err := snapshotsync.SyncSnapshots(
		ctx,
		s.LogPrefix(),
		"header-chain",
		true, /*headerChain=*/
		cfg.blobs,
		cfg.caplinState,
		cfg.prune,
		cstate,
		tx,
		cfg.blockReader,
		cfg.chainConfig,
		cfg.snapshotDownloader,
		cfg.syncConfig,
	); err != nil {
		return err
	}

	// Erigon can start on datadir with broken files `transactions.seg` files and Downloader will
	// fix them, but only if Erigon call `.Add()` for broken files. But `headerchain` feature
	// calling `.Add()` only for header/body files (not for `transactions.seg`) and `.OpenFolder()` will fail
	if err := cfg.blockReader.Snapshots().OpenSegments([]snaptype.Type{snaptype2.Headers, snaptype2.Bodies}, true, false); err != nil {
		err = fmt.Errorf("error opening segments after syncing header chain: %w", err)
		return err
	}

	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "Download snapshots"})
	if err := snapshotsync.SyncSnapshots(
		ctx,
		s.LogPrefix(),
		"remaining snapshots",
		false, /*headerChain=*/
		cfg.blobs,
		cfg.caplinState,
		cfg.prune,
		cstate,
		tx,
		cfg.blockReader,
		cfg.chainConfig,
		cfg.snapshotDownloader,
		cfg.syncConfig,
	); err != nil {
		return err
	}

	{ // Now can open all files
		if err := cfg.blockReader.Snapshots().OpenFolder(); err != nil {
			return err
		}

		if cfg.chainConfig.Bor != nil {
			if err := cfg.blockReader.BorSnapshots().OpenFolder(); err != nil {
				return err
			}
		}
		if err := agg.OpenFolder(); err != nil {
			return err
		}

		if err := firstNonGenesisCheck(tx, cfg.blockReader.Snapshots(), s.LogPrefix(), cfg.dirs); err != nil {
			return err
		}
	}

	// All snapshots are downloaded. Now commit the preverified.toml file so we load the same set of
	// hashes next time.
	err := downloadercfg.SaveSnapshotHashes(cfg.dirs, cfg.chainConfig.ChainName)
	if err != nil {
		err = fmt.Errorf("saving snapshot hashes: %w", err)
		return err
	}

	if cfg.notifier.Events != nil {
		cfg.notifier.Events.OnNewSnapshot()
	}

	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "E2 Indexing"})
	if err := cfg.blockRetire.BuildMissedIndicesIfNeed(ctx, s.LogPrefix(), cfg.notifier.Events); err != nil {
		return err
	}

	indexWorkers := estimate.IndexSnapshot.Workers()
	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "E3 Indexing"})
	if err := agg.BuildMissedAccessors(ctx, indexWorkers); err != nil {
		return err
	}

	if temporal, ok := tx.(*temporal.RwTx); ok {
		temporal.ForceReopenAggCtx() // otherwise next stages will not see just-indexed-files
	}

	// It's ok to notify before tx.Commit(), because RPCDaemon does read list of files by gRPC (not by reading from db)
	if cfg.notifier.Events != nil {
		cfg.notifier.Events.OnNewSnapshot()
	}

	if cfg.silkworm != nil {
		repository := silkworm.NewSnapshotsRepository(
			cfg.silkworm,
			cfg.blockReader.Snapshots().(*freezeblocks.RoSnapshots),
			agg,
			logger,
		)
		if err := repository.Update(); err != nil {
			return err
		}
	}

	frozenBlocks := cfg.blockReader.FrozenBlocks()
	if s.BlockNumber < frozenBlocks { // allow genesis
		if err := s.Update(tx, frozenBlocks); err != nil {
			return err
		}
		s.BlockNumber = frozenBlocks
	}

	diaglib.Send(diaglib.CurrentSyncSubStage{SubStage: "Fill DB"})
	if err := rawdbreset.FillDBFromSnapshots(s.LogPrefix(), ctx, tx, cfg.dirs, cfg.blockReader, logger); err != nil {
		return fmt.Errorf("FillDBFromSnapshots: %w", err)
	}

	if temporal, ok := tx.(*temporal.RwTx); ok {
		temporal.ForceReopenAggCtx() // otherwise next stages will not see just-indexed-files
	}

	{
		cfg.blockReader.Snapshots().LogStat("download")
		txNumsReader := cfg.blockReader.TxnumReader(ctx)
		aggtx := state.AggTx(tx)
		stats.LogStats(aggtx, tx, logger, func(endTxNumMinimax uint64) (uint64, error) {
			histBlockNumProgress, _, err := txNumsReader.FindBlockNum(tx, endTxNumMinimax)
			return histBlockNumProgress, err
		})
	}

	return nil
}

func firstNonGenesisCheck(tx kv.RwTx, snapshots snapshotsync.BlockSnapshots, logPrefix string, dirs datadir.Dirs) error {
	firstNonGenesis, err := rawdbv3.SecondKey(tx, kv.Headers)
	if err != nil {
		return err
	}
	if firstNonGenesis != nil {
		firstNonGenesisBlockNumber := binary.BigEndian.Uint64(firstNonGenesis)
		if snapshots.SegmentsMax()+1 < firstNonGenesisBlockNumber {
			log.Warn(fmt.Sprintf("[%s] Some blocks are not in snapshots and not in db. This could have happened because the node was stopped at the wrong time; you can fix this with 'rm -rf %s' (this is not equivalent to a full resync)", logPrefix, dirs.Chaindata), "max_in_snapshots", snapshots.SegmentsMax(), "min_in_db", firstNonGenesisBlockNumber)
			return fmt.Errorf("some blocks are not in snapshots and not in db. This could have happened because the node was stopped at the wrong time; you can fix this with 'rm -rf %s' (this is not equivalent to a full resync)", dirs.Chaindata)
		}
	}
	return nil
}

func pruneCanonicalMarkers(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader) error {
	pruneThreshold := rawdbreset.GetPruneMarkerSafeThreshold(blockReader)
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

// SnapshotsPrune moving block data from db into snapshots, removing old snapshots (if --prune.* enabled)
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

		if s.CurrentSyncCycle.IsInitialCycle {
			cfg.blockRetire.SetWorkers(estimate.CompressSnapshot.Workers())
		} else {
			cfg.blockRetire.SetWorkers(1)
		}

		noDl := cfg.snapshotDownloader == nil || reflect.ValueOf(cfg.snapshotDownloader).IsNil()
		started := cfg.blockRetire.RetireBlocksInBackground(
			ctx,
			minBlockNumber,
			s.ForwardProgress,
			log.LvlDebug,
			func(downloadRequest []snapshotsync.DownloadRequest) error {
				if noDl {
					return nil
				}
				return snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader, "")
			}, func(l []string) error {
				if noDl {
					return nil
				}
				if _, err := cfg.snapshotDownloader.Delete(ctx, &protodownloader.DeleteRequest{Paths: l}); err != nil {
					return err
				}
				return nil
			}, func() error {
				filesDeleted, err := pruneBlockSnapshots(ctx, cfg, logger)
				if filesDeleted && cfg.notifier != nil {
					cfg.notifier.Events.OnNewSnapshot()
				}
				return err
			}, func() {
				if cfg.notifier != nil {
					cfg.notifier.Events.OnRetirementDone()
				}
			})
		if cfg.notifier != nil {
			cfg.notifier.Events.OnRetirementStart(started)
		}

		//	cfg.agg.BuildFilesInBackground()
	}

	pruneLimit := 10
	pruneTimeout := 125 * time.Millisecond
	if s.CurrentSyncCycle.IsInitialCycle {
		pruneLimit = 10_000
		pruneTimeout = time.Hour
	}
	if _, err := cfg.blockRetire.PruneAncientBlocks(tx, pruneLimit, pruneTimeout); err != nil {
		return err
	}
	if err := pruneCanonicalMarkers(ctx, tx, cfg.blockReader); err != nil {
		return err
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

	//TODO: push-down this logic into `blockRetire`: instead of work on raw file names - we must work on dirtySegments. Instead of calling downloader.Del(file) we must call `downloader.Del(dirtySegment.Paths(snapDir)`
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
			relativePathToFile := file
			if filepath.IsAbs(file) {
				relativePathToFile, _ = filepath.Rel(cfg.dirs.Snap, file)
			}
			if _, err := cfg.snapshotDownloader.Delete(ctx, &protodownloader.DeleteRequest{Paths: []string{relativePathToFile}}); err != nil {
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
