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
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/etl"
	protodownloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

type SnapshotsCfg struct {
	db          kv.RwDB
	chainConfig chain.Config
	dirs        datadir.Dirs

	blockRetire        services.BlockRetire
	snapshotDownloader protodownloader.DownloaderClient
	blockReader        services.FullBlockReader
	notifier           *shards.Notifications

	historyV3        bool
	caplin           bool
	agg              *state.AggregatorV3
	silkworm         *silkworm.Silkworm
	snapshotUploader *snapshotUploader
	syncConfig       ethconfig.Sync
}

func StageSnapshotsCfg(db kv.RwDB,
	chainConfig chain.Config,
	syncConfig ethconfig.Sync,
	dirs datadir.Dirs,
	blockRetire services.BlockRetire,
	snapshotDownloader protodownloader.DownloaderClient,
	blockReader services.FullBlockReader,
	notifier *shards.Notifications,
	historyV3 bool,
	agg *state.AggregatorV3,
	caplin bool,
	silkworm *silkworm.Silkworm,
) SnapshotsCfg {
	cfg := SnapshotsCfg{
		db:                 db,
		chainConfig:        chainConfig,
		dirs:               dirs,
		blockRetire:        blockRetire,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		notifier:           notifier,
		historyV3:          historyV3,
		caplin:             caplin,
		agg:                agg,
		silkworm:           silkworm,
		syncConfig:         syncConfig,
	}

	if uploadFs := cfg.syncConfig.UploadLocation; len(uploadFs) > 0 {

		cfg.snapshotUploader = &snapshotUploader{
			cfg:          &cfg,
			uploadFs:     uploadFs,
			torrentFiles: downloader.NewAtomicTorrentFiles(cfg.dirs.Snap),
		}

		cfg.blockRetire.SetWorkers(estimate.CompressSnapshot.Workers())

		freezingCfg := cfg.blockReader.FreezingCfg()

		if freezingCfg.Enabled && freezingCfg.Produce {
			u := cfg.snapshotUploader

			if maxSeedable := u.maxSeedableHeader(); u.cfg.syncConfig.FrozenBlockLimit > 0 && maxSeedable > u.cfg.syncConfig.FrozenBlockLimit {
				blockLimit := maxSeedable - u.minBlockNumber()

				if u.cfg.syncConfig.FrozenBlockLimit < blockLimit {
					blockLimit = u.cfg.syncConfig.FrozenBlockLimit
				}

				if snapshots, ok := u.cfg.blockReader.Snapshots().(*freezeblocks.RoSnapshots); ok {
					snapshots.SetSegmentsMin(maxSeedable - blockLimit)
				}

				if snapshots, ok := u.cfg.blockReader.BorSnapshots().(*freezeblocks.BorRoSnapshots); ok {
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
	initialCycle bool,
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
	if err := DownloadAndIndexSnapshotsIfNeed(s, ctx, tx, cfg, initialCycle, logger); err != nil {
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

	return nil
}

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg SnapshotsCfg, initialCycle bool, logger log.Logger) error {
	if !initialCycle {
		return nil
	}
	if !cfg.blockReader.FreezingCfg().Enabled {
		return nil
	}
	cstate := snapshotsync.NoCaplin
	if cfg.caplin { //TODO(Giulio2002): uncomment
		cstate = snapshotsync.AlsoCaplin
	}

	if cfg.snapshotUploader != nil {
		u := cfg.snapshotUploader

		u.init(ctx, logger)

		if cfg.syncConfig.UploadFrom != rpc.EarliestBlockNumber {
			u.downloadLatestSnapshots(ctx, cfg.syncConfig.UploadFrom)
		}

		if maxSeedable := u.maxSeedableHeader(); u.cfg.syncConfig.FrozenBlockLimit > 0 && maxSeedable > u.cfg.syncConfig.FrozenBlockLimit {
			blockLimit := maxSeedable - u.minBlockNumber()

			if u.cfg.syncConfig.FrozenBlockLimit < blockLimit {
				blockLimit = u.cfg.syncConfig.FrozenBlockLimit
			}

			if snapshots, ok := u.cfg.blockReader.Snapshots().(*freezeblocks.RoSnapshots); ok {
				snapshots.SetSegmentsMin(maxSeedable - blockLimit)
			}

			if snapshots, ok := u.cfg.blockReader.BorSnapshots().(*freezeblocks.BorRoSnapshots); ok {
				snapshots.SetSegmentsMin(maxSeedable - blockLimit)
			}
		}

		if err := cfg.blockReader.Snapshots().ReopenFolder(); err != nil {
			return err
		}

		if cfg.chainConfig.Bor != nil {
			if err := cfg.blockReader.BorSnapshots().ReopenFolder(); err != nil {
				return err
			}
		}
		if cfg.notifier.Events != nil { // can notify right here, even that write txn is not commit
			cfg.notifier.Events.OnNewSnapshot()
		}
	} else {
		if err := snapshotsync.WaitForDownloader(ctx, s.LogPrefix(), cfg.historyV3, cstate, cfg.agg, tx, cfg.blockReader, &cfg.chainConfig, cfg.snapshotDownloader, s.state.StagesIdsList()); err != nil {
			return err
		}
	}
	// It's ok to notify before tx.Commit(), because RPCDaemon does read list of files by gRPC (not by reading from db)
	if cfg.notifier.Events != nil {
		cfg.notifier.Events.OnNewSnapshot()
	}

	cfg.blockReader.Snapshots().LogStat("download")
	cfg.agg.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
		_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
		return histBlockNumProgress
	})

	if err := cfg.blockRetire.BuildMissedIndicesIfNeed(ctx, s.LogPrefix(), cfg.notifier.Events, &cfg.chainConfig); err != nil {
		return err
	}

	if cfg.silkworm != nil {
		if err := cfg.blockReader.Snapshots().(silkworm.CanAddSnapshotsToSilkwarm).AddSnapshotsToSilkworm(cfg.silkworm); err != nil {
			return err
		}
	}

	if cfg.historyV3 {
		cfg.agg.CleanDir()

		indexWorkers := estimate.IndexSnapshot.Workers()
		if err := cfg.agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
			return err
		}
		if cfg.notifier.Events != nil {
			cfg.notifier.Events.OnNewSnapshot()
		}
	}

	frozenBlocks := cfg.blockReader.FrozenBlocks()
	if s.BlockNumber < frozenBlocks { // allow genesis
		if err := s.Update(tx, frozenBlocks); err != nil {
			return err
		}
		s.BlockNumber = frozenBlocks
	}

	if err := FillDBFromSnapshots(s.LogPrefix(), ctx, tx, cfg.dirs, cfg.blockReader, cfg.agg, logger); err != nil {
		return err
	}

	return nil
}

func FillDBFromSnapshots(logPrefix string, ctx context.Context, tx kv.RwTx, dirs datadir.Dirs, blockReader services.FullBlockReader, agg *state.AggregatorV3, logger log.Logger) error {
	blocksAvailable := blockReader.FrozenBlocks()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	// updating the progress of further stages (but only forward) that are contained inside of snapshots
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
			h2n := etl.NewCollector(logPrefix, dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
			defer h2n.Close()
			h2n.LogLvl(log.LvlDebug)

			// fill some small tables from snapshots, in future we may store this data in snapshots also, but
			// for now easier just store them in db
			td := big.NewInt(0)
			blockNumBytes := make([]byte, 8)
			if err := blockReader.HeadersRange(ctx, func(header *types.Header) error {
				blockNum, blockHash := header.Number.Uint64(), header.Hash()
				td.Add(td, header.Difficulty)

				if err := rawdb.WriteTd(tx, blockHash, blockNum, td); err != nil {
					return err
				}
				if err := rawdb.WriteCanonicalHash(tx, blockHash, blockNum); err != nil {
					return err
				}
				binary.BigEndian.PutUint64(blockNumBytes, blockNum)
				if err := h2n.Collect(blockHash[:], blockNumBytes); err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					logger.Info(fmt.Sprintf("[%s] Total difficulty index: %dk/%dk", logPrefix, header.Number.Uint64()/1000, blockReader.FrozenBlocks()/1000))
				default:
				}
				return nil
			}); err != nil {
				return err
			}
			if err := h2n.Load(tx, kv.HeaderNumber, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
				return err
			}
			canonicalHash, err := blockReader.CanonicalHash(ctx, tx, blocksAvailable)
			if err != nil {
				return err
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
			if err != nil {
				return err
			}

			historyV3, err := kvcfg.HistoryV3.Enabled(tx)
			if err != nil {
				return err
			}
			if historyV3 {
				_ = tx.ClearBucket(kv.MaxTxNum)
				if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-logEvery.C:
						logger.Info(fmt.Sprintf("[%s] MaxTxNums index: %dk/%dk", logPrefix, blockNum/1000, blockReader.FrozenBlocks()/1000))
					default:
					}
					maxTxNum := baseTxNum + txAmount - 1

					if err := rawdbv3.TxNums.Append(tx, blockNum, maxTxNum); err != nil {
						return fmt.Errorf("%w. blockNum=%d, maxTxNum=%d", err, blockNum, maxTxNum)
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
			}
			if err := rawdb.WriteSnapshots(tx, blockReader.FrozenFiles(), agg.Files()); err != nil {
				return err
			}
		}
	}
	return nil
}

/* ====== PRUNING ====== */
// snapshots pruning sections works more as a retiring of blocks
// retiring blocks means moving block data from db into snapshots
func SnapshotsPrune(s *PruneState, initialCycle bool, cfg SnapshotsCfg, ctx context.Context, tx kv.RwTx, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	freezingCfg := cfg.blockReader.FreezingCfg()

	if freezingCfg.Enabled {
		if freezingCfg.Produce {
			//TODO: initialSync maybe save files progress here
			if cfg.blockRetire.HasNewFrozenFiles() || cfg.agg.HasNewFrozenFiles() {
				if err := rawdb.WriteSnapshots(tx, cfg.blockReader.FrozenFiles(), cfg.agg.Files()); err != nil {
					return err
				}
			}

			var minBlockNumber uint64

			if cfg.snapshotUploader != nil {
				minBlockNumber = cfg.snapshotUploader.minBlockNumber()
			}

			cfg.blockRetire.RetireBlocksInBackground(ctx, minBlockNumber, s.ForwardProgress, log.LvlDebug, func(downloadRequest []services.DownloadRequest) error {
				if cfg.snapshotDownloader != nil && !reflect.ValueOf(cfg.snapshotDownloader).IsNil() {
					if err := snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader); err != nil {
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
			})

			//cfg.agg.BuildFilesInBackground()
		}

		if err := cfg.blockRetire.PruneAncientBlocks(tx, cfg.syncConfig.PruneLimit); err != nil {
			return err
		}
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
	torrentFiles    *downloader.TorrentFiles
}

func (u *snapshotUploader) init(ctx context.Context, logger log.Logger) {
	if u.files == nil {
		freezingCfg := u.cfg.blockReader.FreezingCfg()

		if freezingCfg.Enabled && freezingCfg.Produce {
			u.files = map[string]*uploadState{}
			u.start(ctx, logger)
		}
	}
}

func (u *snapshotUploader) maxUploadedHeader() uint64 {
	var max uint64

	if len(u.files) > 0 {
		for _, state := range u.files {
			if state.local && state.remote {
				if state.info != nil {
					if state.info.Type.Enum() == snaptype.Enums.Headers {
						if state.info.To > max {
							max = state.info.To
						}
					}
				} else {
					if info, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, state.file); ok {
						if info.Type.Enum() == snaptype.Enums.Headers {
							if info.To > max {
								max = info.To
							}
						}
						state.info = &info
					}
				}
			}
		}
	}

	return max
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
	if info, ok := snaptype.ParseFileName("", e.name); ok {
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
			info, _ := snaptype.ParseFileName("", it.Name)

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
			info, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, fi.Name())

			if !ok {
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

	var min uint64

	for _, info := range lastSegments {
		if lastInfo, ok := info.Sys().(downloader.SnapInfo); ok {
			if min == 0 || lastInfo.From() < min {
				min = lastInfo.From()
			}
		}
	}

	for segType, info := range lastSegments {
		if lastInfo, ok := info.Sys().(downloader.SnapInfo); ok {
			if lastInfo.From() > min {
				for _, ent := range entries {
					if info, err := ent.Info(); err == nil {
						snapInfo, ok := info.Sys().(downloader.SnapInfo)

						if ok && snapInfo.Type().Enum() == segType &&
							snapInfo.From() == min {
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
	var min uint64

	if list, err := snaptype.Segments(u.cfg.dirs.Snap); err == nil {
		for _, info := range list {
			if u.seedable(info) && min == 0 || info.From < min {
				min = info.From
			}
		}
	}

	return min
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

	u.uploadSession, err = u.rclone.NewSession(ctx, u.cfg.dirs.Snap, uploadFs)

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

	var toRemove []string //nolint:prealloc

	for _, f := range list {
		if f.To > before {
			switch f.Type.Enum() {
			case snaptype.Enums.BorEvents, snaptype.Enums.BorSpans:
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
			snapshots.ReopenList(toReopen, true)
		}

		if snapshots, ok := u.cfg.blockReader.BorSnapshots().(*freezeblocks.BorRoSnapshots); ok {
			snapshots.ReopenList(borToReopen, true)
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
				if fi, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, f); ok {
					if u.seedable(fi) {
						state := &uploadState{
							file:  f,
							info:  &fi,
							local: true,
						}

						if fi.TorrentFileExists() {
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

					if state.torrent == nil && state.info.TorrentFileExists() {
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

					err := downloader.BuildTorrentIfNeed(gctx, state.file, u.cfg.dirs.Snap, u.torrentFiles)

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
