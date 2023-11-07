package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/etl"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
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
	snapshotDownloader proto_downloader.DownloaderClient
	blockReader        services.FullBlockReader
	notifier           *shards.Notifications

	historyV3        bool
	agg              *state.AggregatorV3
	silkworm         *silkworm.Silkworm
	snapshotUploader *snapshotUploader
}

func StageSnapshotsCfg(db kv.RwDB,
	chainConfig chain.Config,
	dirs datadir.Dirs,
	blockRetire services.BlockRetire,
	snapshotDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	notifier *shards.Notifications,
	historyV3 bool,
	agg *state.AggregatorV3,
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
		agg:                agg,
		silkworm:           silkworm,
	}

	if uploadFs := dbg.SnapshotUploadFs(); len(uploadFs) > 0 {

		cfg.snapshotUploader = &snapshotUploader{version: dbg.SnapshotVersion(), cfg: &cfg, uploadFs: uploadFs}

		freezingCfg := cfg.blockReader.FreezingCfg()

		if freezingCfg.Enabled && freezingCfg.Produce {
			u := cfg.snapshotUploader

			if dbg.FrozenBlockLimit() != 0 {
				u.frozenBlockLimit = dbg.FrozenBlockLimit()
			}

			if maxSeedable := u.maxSeedableHeader(); u.frozenBlockLimit > 0 && maxSeedable > u.frozenBlockLimit {
				if snapshots, ok := u.cfg.blockReader.Snapshots().(*freezeblocks.RoSnapshots); ok {
					snapshots.SetSegmentsMin(maxSeedable - u.frozenBlockLimit)
				}

				if snapshots, ok := u.cfg.blockReader.BorSnapshots().(*freezeblocks.BorRoSnapshots); ok {
					snapshots.SetSegmentsMin(maxSeedable - u.frozenBlockLimit)
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

		if stage == stages.SyncStage(dbg.BreakAfterStage()) {
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

	if err := snapshotsync.WaitForDownloader(s.LogPrefix(), ctx, cfg.historyV3, snapshotsync.NoCaplin, cfg.agg, tx, cfg.blockReader, cfg.notifier.Events, &cfg.chainConfig, cfg.snapshotDownloader); err != nil {
		return err
	}

	cfg.agg.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
		_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
		return histBlockNumProgress
	})

	if err := cfg.blockRetire.BuildMissedIndicesIfNeed(ctx, s.LogPrefix(), cfg.notifier.Events, &cfg.chainConfig); err != nil {
		return err
	}

	if cfg.silkworm != nil {
		if err := cfg.blockReader.Snapshots().(*freezeblocks.RoSnapshots).AddSnapshotsToSilkworm(cfg.silkworm); err != nil {
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
			type LastTxNumProvider interface {
				FirstTxNumNotInSnapshots() uint64
			}
			firstTxNum := blockReader.(LastTxNumProvider).FirstTxNumNotInSnapshots()
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
				type IterBody interface {
					IterateFrozenBodies(f func(blockNum, baseTxNum, txAmount uint64) error) error
				}
				if err := blockReader.(IterBody).IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
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
		if err := cfg.blockRetire.PruneAncientBlocks(tx, 100, cfg.chainConfig.Bor != nil); err != nil {
			return err
		}
	}
	if freezingCfg.Enabled && freezingCfg.Produce {
		//TODO: initialSync maybe save files progress here
		if cfg.blockRetire.HasNewFrozenFiles() || cfg.agg.HasNewFrozenFiles() {
			if err := rawdb.WriteSnapshots(tx, cfg.blockReader.FrozenFiles(), cfg.agg.Files()); err != nil {
				return err
			}
		}

		var minBlockNumber uint64

		if cfg.snapshotUploader != nil {
			cfg.snapshotUploader.init(ctx, s, logger)

			minBlockNumber = cfg.snapshotUploader.minBlockNumber()
		}

		cfg.blockRetire.RetireBlocksInBackground(ctx, minBlockNumber, s.ForwardProgress, cfg.chainConfig.Bor != nil, log.LvlInfo, func(downloadRequest []services.DownloadRequest) error {
			if cfg.snapshotDownloader != nil && !reflect.ValueOf(cfg.snapshotDownloader).IsNil() {
				if err := snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader); err != nil {
					return err
				}
			}

			return snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader)
		}, func(l []string) error {
			if cfg.snapshotDownloader == nil || reflect.ValueOf(cfg.snapshotDownloader).IsNil() {
				return nil
			}
			_, err := cfg.snapshotDownloader.Delete(ctx, &proto_downloader.DeleteRequest{Paths: l})
			return err
		})

		//cfg.agg.BuildFilesInBackground()
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
	file            string
	info            *snaptype.FileInfo
	torrent         *torrent.TorrentSpec
	buildingTorrent bool
	uploadRequest   *rcloneRequest
	remote          bool
	remoteHash      string
	local           bool
	localHash       string
}

type snapshotUploader struct {
	version          string
	cfg              *SnapshotsCfg
	state            *PruneState
	files            map[string]*uploadState
	uploadFs         string
	rclone           *exec.Cmd
	rcloneUrl        string
	rcloneClient     *http.Client
	frozenBlockLimit uint64
	remoteHashes     map[string]interface{}
	uploadScheduled  atomic.Bool
	uploading        atomic.Bool
}

func (u *snapshotUploader) init(ctx context.Context, s *PruneState, logger log.Logger) {
	if u.files == nil {
		freezingCfg := u.cfg.blockReader.FreezingCfg()

		if freezingCfg.Enabled && freezingCfg.Produce {
			u.files = map[string]*uploadState{}
			u.start(ctx, logger)
		}
	}

	u.state = s
}

func (u *snapshotUploader) maxUploadedHeader() uint64 {
	var max uint64

	if len(u.files) > 0 {
		for _, state := range u.files {
			if state.local && state.remote {
				if state.info != nil {
					if state.info.T == snaptype.Headers {
						if state.info.To > max {
							max = state.info.To
						}
					}
				} else {
					if info, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, state.file); ok {
						if info.T == snaptype.Headers {
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

func (u *snapshotUploader) maxSeedableHeader() uint64 {
	var max uint64

	if list, err := snaptype.Segments(u.cfg.dirs.Snap); err == nil {
		for _, info := range list {
			if info.Seedable() && info.T == snaptype.Headers && info.To > max {
				max = info.To
			}
		}
	}

	return max
}

func (u *snapshotUploader) minBlockNumber() uint64 {
	var min uint64

	if list, err := snaptype.Segments(u.cfg.dirs.Snap); err == nil {
		for _, info := range list {
			if info.Seedable() && min == 0 || info.From < min {
				min = info.From
			}
		}
	}

	return min
}

func freePort() (port int, err error) {
	if a, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0"); err != nil {
		return 0, err
	} else {
		if l, err := net.ListenTCP("tcp", a); err != nil {
			return 0, err
		} else {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
}

func (u *snapshotUploader) hashesFile() string {
	return "." + u.version + "-torrent-hashes.toml"
}

func (u *snapshotUploader) start(ctx context.Context, logger log.Logger) {
	rclone, _ := exec.LookPath("rclone")

	if len(rclone) == 0 {
		logger.Warn("[snapshot uploader] Uploading disabled: rclone not found in PATH")
		return
	}

	if p, err := freePort(); err == nil {
		addr := fmt.Sprintf("127.0.0.1:%d", p)
		u.rclone = exec.CommandContext(ctx, rclone, "rcd", "--rc-addr", addr, "--rc-no-auth")
		u.rcloneUrl = "http://" + addr
		u.rcloneClient = &http.Client{} // no timeout - we're doing sync calls

		if err := u.rclone.Start(); err != nil {
			logger.Warn("[snapshot uploader] Uploading disabled: rclone didn't start", "err", err)
		} else {
			logger.Info("[snapshot uploader] rclone started", "addr", addr)
		}

		go func() {
			listBody, err := json.Marshal(struct {
				Fs     string `json:"fs"`
				Remote string `json:"remote"`
			}{
				Fs:     u.uploadFs,
				Remote: "",
			})

			if err == nil {
				listRequest, err := http.NewRequestWithContext(ctx, http.MethodPost,
					u.rcloneUrl+"/operations/list", bytes.NewBuffer(listBody))

				if err == nil {
					listRequest.Header.Set("Content-Type", "application/json")

					var response *http.Response

					for i := 0; i < 10; i++ {
						response, err = u.rcloneClient.Do(listRequest)
						if err == nil {
							break
						}
						time.Sleep(2 * time.Second)
					}

					if err == nil && response.StatusCode == http.StatusOK {
						defer response.Body.Close()

						type fileInfo struct {
							Name    string
							Size    uint64
							ModTime time.Time
						}

						responseBody := struct {
							List []fileInfo `json:"list"`
						}{}

						if err := json.NewDecoder(response.Body).Decode(&responseBody); err == nil {
							for _, fi := range responseBody.List {
								var file string

								if hashesFile := u.hashesFile(); fi.Name == hashesFile {
									var hashes map[string]interface{}

									if hashesData, err := exec.Command("rclone", "cat", u.uploadFs+"/"+hashesFile).Output(); err == nil {
										if err = toml.Unmarshal(hashesData, &hashes); err == nil {
											u.remoteHashes = hashes
										}
									}
								}

								if filepath.Ext(fi.Name) != ".torrent" {
									file = fi.Name
								} else {
									file = strings.TrimSuffix(fi.Name, ".torrent")
								}

								// if we have found the file & its torrent we don't
								// need to attmept another sync operation
								if state, ok := u.files[file]; ok {
									state.remote = true
								} else {
									info, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, fi.Name)

									if !ok {
										continue
									}

									u.files[file] = &uploadState{
										file:  file,
										info:  &info,
										local: dir.FileNonZero(info.Path),
									}
								}
							}
						}
					}
				}
			}

			logger.Debug("[snapshot uploader] starting snapshot subscription...")
			newSnCh, newSnClean := u.cfg.notifier.Events.AddNewSnapshotSubscription()
			defer newSnClean()

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
				case <-newSnCh:
					logger.Info("[snapshot uploader] new snapshot received")
					u.scheduleUpload(ctx, logger)
				}
			}
		}()
	}
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

	var toRemove []string

	for _, f := range list {
		if f.To > before {
			switch f.T {
			case snaptype.BorEvents, snaptype.BorSpans:
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

type rcloneFilter struct {
	IncludeRule []string `json:"IncludeRule"`
}

type rcloneRequest struct {
	Async  bool                   `json:"_async,omitempty"`
	Config map[string]interface{} `json:"_config,omitempty"`
	SrcFs  string                 `json:"srcFs"`
	DstFs  string                 `json:"dstFs"`
	Filter rcloneFilter           `json:"_filter"`
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
					if fi.Seedable() {
						state := &uploadState{
							file:  f,
							info:  &fi,
							local: true,
						}

						if fi.TorrentFileExists() {
							state.torrent, _ = downloader.LoadTorrent(fi.Path + ".torrent")
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
						state.torrent, _ = downloader.LoadTorrent(state.info.Path + ".torrent")
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

					torrentPath, err := downloader.BuildTorrentIfNeed(gctx, state.file, u.cfg.dirs.Snap)

					state.Lock()
					state.buildingTorrent = false
					state.Unlock()

					if err != nil {
						return err
					}

					torrent, err := downloader.LoadTorrent(torrentPath)

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
				if !state.remote && state.torrent != nil && state.uploadRequest == nil && u.rclone != nil {
					state.uploadRequest = &rcloneRequest{
						SrcFs: u.cfg.dirs.Snap,
						DstFs: u.uploadFs,
						Filter: rcloneFilter{
							IncludeRule: []string{state.file, state.file + ".torrent"},
						},
					}
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
							state.uploadRequest = nil
							state.Unlock()
						}()

						if err := u.syncRequest(gctx, state.uploadRequest, logger); err != nil {
							f.Add(1)
							return nil
						}

						uploadCount++

						state.Lock()
						state.remote = true
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
		hashesFile := u.hashesFile()

		var hashes map[string]interface{}
		var hashesData []byte

		hashesData, err = os.ReadFile(filepath.Join(u.cfg.dirs.Snap, hashesFile))

		if err == nil {
			err = toml.Unmarshal(hashesData, &hashes)
		}

		if err != nil {
			hashes = map[string]interface{}{}
		}

		for file, hash := range u.remoteHashes {
			hashes[file] = hash
		}

		for file, state := range u.files {
			if len(state.localHash) > 0 && state.remote {
				hashes[file] = state.localHash
			}
		}

		hashesData, err = toml.Marshal(hashes)

		if err == nil {
			_ = os.WriteFile(filepath.Join(u.cfg.dirs.Snap, hashesFile), hashesData, 0644)
		}

		u.syncRequest(ctx, &rcloneRequest{
			SrcFs: u.cfg.dirs.Snap,
			DstFs: u.uploadFs,
			Filter: rcloneFilter{
				IncludeRule: []string{hashesFile},
			},
		}, logger)

		u.remoteHashes = hashes
	}

	if err == nil {
		if maxUploaded := u.maxUploadedHeader(); u.frozenBlockLimit > 0 && maxUploaded > u.frozenBlockLimit {
			u.removeBefore(maxUploaded - u.frozenBlockLimit)
		}
	}
}

func (u *snapshotUploader) syncRequest(ctx context.Context, request *rcloneRequest, logger log.Logger) error {
	requestBody, err := json.Marshal(request)

	if err != nil {
		return err
	}

	uploadRequest, err := http.NewRequestWithContext(ctx, http.MethodPost,
		u.rcloneUrl+"/sync/sync", bytes.NewBuffer(requestBody))

	if err != nil {
		return err
	}

	uploadRequest.Header.Set("Content-Type", "application/json")

	response, err := u.rcloneClient.Do(uploadRequest)

	if err != nil {
		return err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		responseBody := struct {
			Error string `json:"error"`
		}{}

		if err := json.NewDecoder(response.Body).Decode(&responseBody); err == nil && len(responseBody.Error) > 0 {
			logger.Warn("[snapshot uploader] sync request failed", "status", response.Status, "err", responseBody.Error)
			return fmt.Errorf("sync request failed: %s: %s", response.Status, responseBody.Error)
		} else {
			logger.Warn("[snapshot uploader] sync reqiest failed", "status", response.Status)
			return fmt.Errorf("sync request failed: %s", response.Status)
		}
	}

	return nil
}
