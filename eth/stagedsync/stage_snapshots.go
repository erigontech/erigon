package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
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
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
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

	if dbg.UploadSnapshots() {
		cfg.snapshotUploader = &snapshotUploader{}
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

	if err := snapshotsync.WaitForDownloader(s.LogPrefix(), ctx, cfg.historyV3, cfg.agg, tx, cfg.blockReader, cfg.notifier.Events, &cfg.chainConfig, cfg.snapshotDownloader); err != nil {
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

		cfg.blockRetire.RetireBlocksInBackground(ctx, s.ForwardProgress, cfg.chainConfig.Bor != nil, log.LvlInfo, func(downloadRequest []services.DownloadRequest) error {
			if cfg.snapshotDownloader != nil && !reflect.ValueOf(cfg.snapshotDownloader).IsNil() {
				if err := snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader); err != nil {
					return err
				}
			}
			return nil
		})

		if cfg.snapshotUploader != nil {
			cfg.snapshotUploader.init(ctx, s, cfg, logger)
		}

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
	info            snaptype.FileInfo
	torrent         *torrent.TorrentSpec
	buildingTorrent bool
	uploadRequest   *http.Request
	uploaded        bool
}

func (u *uploadState) torrentHash() (string, string) {
	u.Lock()
	torrent := u.torrent
	u.Unlock()

	if torrent == nil {
		return "", ""
	}

	return torrent.DisplayName, torrent.InfoHash.String()
}

type snapshotUploader struct {
	cfg          *SnapshotsCfg
	state        *PruneState
	files        map[string]*uploadState
	uploadFs     string
	rclone       *exec.Cmd
	rcloneUrl    string
	rcloneClient *http.Client
}

func (u *snapshotUploader) init(ctx context.Context, s *PruneState, cfg SnapshotsCfg, logger log.Logger) {
	if u.cfg == nil {
		u.cfg = &cfg
		freezingCfg := cfg.blockReader.FreezingCfg()

		if freezingCfg.Enabled && freezingCfg.Produce {
			u.files = map[string]*uploadState{}
			u.start(ctx, logger)
		}
	}

	u.state = s
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

		u.uploadFs = "r2:erigon-v2-snapshots-mumbai"

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
						for _, info := range responseBody.List {
							var file string

							if filepath.Ext(info.Name) != ".torrent" {
								file = info.Name
							} else {
								file = strings.TrimSuffix(info.Name, ".torrent")
							}

							// if we have found the file & its torrent we don't
							// need to attmpt another sync operation
							if state, ok := u.files[file]; ok {
								state.uploaded = true
							} else {
								u.files[info.Name] = &uploadState{
									file: info.Name,
								}
							}
						}
					}
				}
			}
		}
	}

	go func() {
		logger.Debug("[snapshot uploader] starting snapshot subscription...")
		newSnCh, newSnClean := u.cfg.notifier.Events.AddNewSnapshotSubscription()
		defer newSnClean()

		logger.Info("[snapshot uploader] subscription established")

		var err error

		defer func() {
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Warn("[snapshot uploader] subscription closed", "reason", err)
				}
			} else {
				logger.Warn("[snapshot uploader] subscription closed")
			}
		}()

		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			case <-newSnCh:
				func() {
					logger.Info("[snapshot uploader] new snapshot received")

					var processList []*uploadState

					for _, f := range u.cfg.blockReader.FrozenFiles() {
						if state, ok := u.files[f]; !ok {
							if fi, ok := snaptype.ParseFileName(u.cfg.dirs.Snap, f); ok {
								if fi.To-fi.From == 500_000 {
									state := &uploadState{
										file: f,
										info: fi,
									}

									state.torrent, _ = downloader.LoadTorrent(fi.Path + ".torrent")

									u.files[f] = state
									processList = append(processList, state)
								}
							}
						} else {
							func() {
								state.Lock()
								defer state.Unlock()

								if state.torrent == nil {
									state.torrent, _ = downloader.LoadTorrent(state.info.Path + ".torrent")
								}

								if !state.uploaded {
									processList = append(processList, state)
								}
							}()
						}
					}

					g, gctx := errgroup.WithContext(ctx)
					g.SetLimit(runtime.GOMAXPROCS(-1) * 4)
					var i atomic.Int32
					var pc int

					for _, s := range processList {
						state := s
						func() {
							state.Lock()
							defer state.Unlock()

							if !(state.torrent != nil || state.buildingTorrent) {
								pc++

								state.buildingTorrent = true

								g.Go(func() error {
									defer i.Add(1)
									logger.Info("[snapshot uploader] building torrent", "file", state.file)

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

									return nil
								})
							}
						}()
					}

					logEvery := time.NewTicker(20 * time.Second)
					logEvery.Stop()

				Torrents:
					for int(i.Load()) < pc {
						select {
						case <-gctx.Done():
							break Torrents
						case <-logEvery.C:
							if int(i.Load()) == pc {
								break Torrents
							}
							log.Info("[snapshot uploader] Creating .torrent files", "progress", fmt.Sprintf("%d/%d", i.Load(), pc))
						}
					}

					if err := g.Wait(); err != nil {
						logger.Debug(".torrent file creation failed", "err", err)
					}

					g, gctx = errgroup.WithContext(ctx)
					g.SetLimit(8)
					i.Store(0)
					pc = 0

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

					for _, s := range processList {
						state := s
						err := func() error {
							state.Lock()
							defer state.Unlock()
							if !state.uploaded && state.torrent != nil && state.uploadRequest == nil && u.rclone != nil {
								requestBody, err := json.Marshal(rcloneRequest{
									SrcFs: u.cfg.dirs.Snap,
									DstFs: u.uploadFs,
									Filter: rcloneFilter{
										IncludeRule: []string{state.file, state.file + ".torrent"},
									},
								})

								if err != nil {
									return err
								}

								state.uploadRequest, err = http.NewRequestWithContext(ctx, http.MethodPost,
									u.rcloneUrl+"/sync/sync", bytes.NewBuffer(requestBody))

								if err != nil {
									return err
								}

								state.uploadRequest.Header.Set("Content-Type", "application/json")

								g.Go(func() error {
									defer i.Add(1)
									defer func() {
										state.Lock()
										state.uploadRequest = nil
										state.Unlock()
									}()

									response, err := u.rcloneClient.Do(state.uploadRequest)

									if err != nil {
										return err
									}

									defer response.Body.Close()

									if response.StatusCode != http.StatusOK {

										buff, _ := io.ReadAll(response.Body)

										responseBody := struct {
											Error string `json:"error"`
										}{}

										if err := json.NewDecoder(bytes.NewBuffer(buff)).Decode(&responseBody); err == nil && len(responseBody.Error) > 0 {
											return fmt.Errorf("upload request failed: %s: %s", response.Status, responseBody.Error)
										} else {
											return fmt.Errorf("upload request failed: %s", response.Status)
										}
									}

									state.Lock()
									state.uploaded = true
									state.Unlock()
									return nil
								})
							}

							pc++
							return nil
						}()

						if err != nil {
							logger.Debug("upload failed", "file", state.file, "err", err)
						}
					}

				Uploads:
					for int(i.Load()) < pc {
						select {
						case <-gctx.Done():
							break Uploads
						case <-logEvery.C:
							if int(i.Load()) == pc {
								break Uploads
							}
							log.Info("[snapshot uploader] Uploading files", "progress", fmt.Sprintf("%d/%d", i.Load(), pc))
						}
					}

					if err := g.Wait(); err != nil {
						logger.Debug("upload failed", "err", err)
					}
				}()
			}
		}
	}()
}
