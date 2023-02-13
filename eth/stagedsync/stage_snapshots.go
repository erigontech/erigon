package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"path/filepath"
	"runtime"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/etl"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/parlia"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"github.com/ledgerwatch/log/v3"
)

type SnapshotsCfg struct {
	db          kv.RwDB
	chainConfig chain.Config
	dirs        datadir.Dirs

	snapshots          *snapshotsync.RoSnapshots
	blockRetire        *snapshotsync.BlockRetire
	snapshotDownloader proto_downloader.DownloaderClient
	blockReader        services.FullBlockReader
	dbEventNotifier    snapshotsync.DBEventNotifier
	engine             consensus.Engine

	historyV3 bool
	agg       *state.AggregatorV3
}

func StageSnapshotsCfg(
	db kv.RwDB,
	chainConfig chain.Config,
	dirs datadir.Dirs,
	snapshots *snapshotsync.RoSnapshots,
	blockRetire *snapshotsync.BlockRetire,
	snapshotDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	dbEventNotifier snapshotsync.DBEventNotifier,
	engine consensus.Engine,
	historyV3 bool,
	agg *state.AggregatorV3,
) SnapshotsCfg {
	return SnapshotsCfg{
		db:                 db,
		chainConfig:        chainConfig,
		dirs:               dirs,
		snapshots:          snapshots,
		blockRetire:        blockRetire,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		dbEventNotifier:    dbEventNotifier,
		historyV3:          historyV3,
		agg:                agg,
		engine:             engine,
	}
}

func SpawnStageSnapshots(
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg SnapshotsCfg,
	initialCycle bool,
) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if err := DownloadAndIndexSnapshotsIfNeed(s, ctx, tx, cfg, initialCycle); err != nil {
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

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg SnapshotsCfg, initialCycle bool) error {
	if !initialCycle || cfg.snapshots == nil || !cfg.snapshots.Cfg().Enabled {
		return nil
	}

	if err := WaitForDownloader(s, ctx, cfg, tx); err != nil {
		return err
	}

	cfg.snapshots.LogStat()
	cfg.agg.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
		_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
		return histBlockNumProgress
	})

	// Create .idx files
	if cfg.snapshots.IndicesMax() < cfg.snapshots.SegmentsMax() {
		if !cfg.snapshots.Cfg().Produce && cfg.snapshots.IndicesMax() == 0 {
			return fmt.Errorf("please remove --snap.stop, erigon can't work without creating basic indices")
		}
		if cfg.snapshots.Cfg().Produce {
			if !cfg.snapshots.SegmentsReady() {
				return fmt.Errorf("not all snapshot segments are available")
			}

			// wait for Downloader service to download all expected snapshots
			if cfg.snapshots.IndicesMax() < cfg.snapshots.SegmentsMax() {
				chainID, _ := uint256.FromBig(cfg.chainConfig.ChainID)
				indexWorkers := estimate.IndexSnapshot.Workers()
				if err := snapshotsync.BuildMissedIndices(s.LogPrefix(), ctx, cfg.dirs, *chainID, indexWorkers); err != nil {
					return fmt.Errorf("BuildMissedIndices: %w", err)
				}
			}

			if err := cfg.snapshots.ReopenFolder(); err != nil {
				return err
			}
			if cfg.dbEventNotifier != nil {
				cfg.dbEventNotifier.OnNewSnapshot()
			}
		}
	}

	if cfg.historyV3 {
		indexWorkers := estimate.IndexSnapshot.Workers()
		if err := cfg.agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
			return err
		}
		if cfg.dbEventNotifier != nil {
			cfg.dbEventNotifier.OnNewSnapshot()
		}
	}

	blocksAvailable := cfg.snapshots.BlocksAvailable()
	if s.BlockNumber < blocksAvailable { // allow genesis
		if err := s.Update(tx, blocksAvailable); err != nil {
			return err
		}
		s.BlockNumber = blocksAvailable
	}

	if err := FillDBFromSnapshots(s.LogPrefix(), ctx, tx, cfg.dirs, cfg.snapshots, cfg.blockReader, cfg.chainConfig, cfg.engine, cfg.agg); err != nil {
		return err
	}
	return nil
}

func FillDBFromSnapshots(logPrefix string, ctx context.Context, tx kv.RwTx, dirs datadir.Dirs, sn *snapshotsync.RoSnapshots, blockReader services.FullBlockReader, chainConfig chain.Config, engine consensus.Engine, agg *state.AggregatorV3) error {
	blocksAvailable := sn.BlocksAvailable()
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
			h2n := etl.NewCollector(logPrefix, dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize))
			defer h2n.Close()
			h2n.LogLvl(log.LvlDebug)

			// fill some small tables from snapshots, in future we may store this data in snapshots also, but
			// for now easier just store them in db
			td := big.NewInt(0)
			blockNumBytes := make([]byte, 8)
			chainReader := &ChainReaderImpl{config: &chainConfig, tx: tx, blockReader: blockReader}
			if err := snapshotsync.ForEachHeader(ctx, sn, func(header *types.Header) error {
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

				if engine != nil {
					// consensus may have own database, let's fill it
					// different consensuses may have some conditions for validators snapshots
					need := false
					switch engine.(type) {
					case *parlia.Parlia:
						need = (blockNum-1)%(100*parlia.CheckpointInterval) == 0
					}
					if need {
						if err := engine.VerifyHeader(chainReader, header, true /* seal */); err != nil {
							return err
						}
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[%s] Total difficulty index: %dk/%dk", logPrefix, header.Number.Uint64()/1000, sn.BlocksAvailable()/1000))
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
			// ResetSequence - allow set arbitrary value to sequence (for example to decrement it to exact value)
			ok, err := sn.ViewTxs(blocksAvailable, func(sn *snapshotsync.TxnSegment) error {
				lastTxnID := sn.IdxTxnHash.BaseDataID() + uint64(sn.Seg.Count())
				if err := rawdb.ResetSequence(tx, kv.EthTx, lastTxnID); err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("snapshot not found for block: %d", blocksAvailable)
			}

			historyV3, err := kvcfg.HistoryV3.Enabled(tx)
			if err != nil {
				return err
			}
			if historyV3 {
				var toBlock uint64
				if sn != nil {
					toBlock = sn.BlocksAvailable()
				}
				toBlock = cmp.Max(toBlock, progress)

				_ = tx.ClearBucket(kv.MaxTxNum)
				if err := rawdbv3.TxNums.WriteForGenesis(tx, 1); err != nil {
					return err
				}
				if err := sn.Bodies.View(func(bs []*snapshotsync.BodySegment) error {
					for _, b := range bs {
						if err := b.Iterate(func(blockNum, baseTxNum, txAmount uint64) error {
							if blockNum == 0 || blockNum > toBlock {
								return nil
							}
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-logEvery.C:
								log.Info(fmt.Sprintf("[%s] MaxTxNums index: %dk/%dk", logPrefix, blockNum/1000, sn.BlocksAvailable()/1000))
							default:
							}
							maxTxNum := baseTxNum + txAmount - 1

							if err := rawdbv3.TxNums.Append(tx, blockNum, maxTxNum); err != nil {
								return fmt.Errorf("%w. blockNum=%d, maxTxNum=%d", err, blockNum, maxTxNum)
							}
							return nil
						}); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					return fmt.Errorf("build txNum => blockNum mapping: %w", err)
				}
			}
			if err := rawdb.WriteSnapshots(tx, sn.Files(), agg.Files()); err != nil {
				return err
			}
		}
	}
	return nil
}

// WaitForDownloader - wait for Downloader service to download all expected snapshots
// for MVP we sync with Downloader only once, in future will send new snapshots also
func WaitForDownloader(s *StageState, ctx context.Context, cfg SnapshotsCfg, tx kv.RwTx) error {
	if cfg.snapshots.Cfg().NoDownloader {
		if err := cfg.snapshots.ReopenFolder(); err != nil {
			return err
		}
		if cfg.dbEventNotifier != nil { // can notify right here, even that write txn is not commit
			cfg.dbEventNotifier.OnNewSnapshot()
		}
		return nil
	}
	// Original intent of snInDB was to contain the file names of the snapshot files for the very first run of the Erigon instance
	// Then, we would insist to only download such files, and no others (whitelist)
	// However, at some point later, the code was incorrectly changed to update this record in each iteration of the stage loop (function WriteSnapshots)
	// And so this list cannot be relied upon as the whitelist, because it also includes all the files created by the node itself
	// Not sure what to do it is so far, but the temporary solution is to instead use it as a blacklist (existingFilesMap)
	snInDB, snHistInDB, err := rawdb.ReadSnapshots(tx)
	if err != nil {
		return err
	}
	dbEmpty := len(snInDB) == 0
	var missingSnapshots []snapshotsync.Range
	var existingFiles []snaptype.FileInfo
	if !dbEmpty {
		existingFiles, missingSnapshots, err = snapshotsync.Segments(cfg.snapshots.Dir())
		if err != nil {
			return err
		}
	}
	existingFilesMap := map[string]struct{}{}
	for _, existingFile := range existingFiles {
		_, fname := filepath.Split(existingFile.Path)
		existingFilesMap[fname] = struct{}{}
	}
	if len(missingSnapshots) > 0 {
		log.Warn(fmt.Sprintf("[%s] downloading missing snapshots", s.LogPrefix()))
	}

	// send all hashes to the Downloader service
	preverifiedBlockSnapshots := snapcfg.KnownCfg(cfg.chainConfig.ChainName, snInDB, snHistInDB).Preverified
	downloadRequest := make([]snapshotsync.DownloadRequest, 0, len(preverifiedBlockSnapshots)+len(missingSnapshots))
	// build all download requests
	// builds preverified snapshots request
	for _, p := range preverifiedBlockSnapshots {
		if _, exists := existingFilesMap[p.Name]; !exists { // Not to download existing files "behind the scenes"
			downloadRequest = append(downloadRequest, snapshotsync.NewDownloadRequest(nil, p.Name, p.Hash))
		}
	}
	if cfg.historyV3 {
		preverifiedHistorySnapshots := snapcfg.KnownCfg(cfg.chainConfig.ChainName, snInDB, snHistInDB).PreverifiedHistory
		for _, p := range preverifiedHistorySnapshots {
			downloadRequest = append(downloadRequest, snapshotsync.NewDownloadRequest(nil, p.Name, p.Hash))
		}
	}

	// builds missing snapshots request
	for i := range missingSnapshots {
		downloadRequest = append(downloadRequest, snapshotsync.NewDownloadRequest(&missingSnapshots[i], "", ""))
	}

	log.Info(fmt.Sprintf("[%s] Fetching torrent files metadata", s.LogPrefix()))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader); err != nil {
			log.Error(fmt.Sprintf("[%s] call downloader", s.LogPrefix()), "err", err)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	downloadStartTime := time.Now()
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	var m runtime.MemStats

	// Check once without delay, for faster erigon re-start
	stats, err := cfg.snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{})
	if err == nil && stats.Completed {
		goto Finish
	}

	// Print download progress until all segments are available
Loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			if stats, err := cfg.snapshotDownloader.Stats(ctx, &proto_downloader.StatsRequest{}); err != nil {
				log.Warn("Error while waiting for snapshots progress", "err", err)
			} else if stats.Completed {
				if !cfg.snapshots.Cfg().Verify { // will verify after loop
					if _, err := cfg.snapshotDownloader.Verify(ctx, &proto_downloader.VerifyRequest{}); err != nil {
						return err
					}
				}
				log.Info(fmt.Sprintf("[%s] download finished", s.LogPrefix()), "time", time.Since(downloadStartTime).String())
				break Loop
			} else {
				if stats.MetadataReady < stats.FilesTotal {
					log.Info(fmt.Sprintf("[%s] Waiting for torrents metadata: %d/%d", s.LogPrefix(), stats.MetadataReady, stats.FilesTotal))
					continue
				}
				dbg.ReadMemStats(&m)
				downloadTimeLeft := calculateTime(stats.BytesTotal-stats.BytesCompleted, stats.DownloadRate)
				log.Info(fmt.Sprintf("[%s] download", s.LogPrefix()),
					"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, libcommon.ByteCount(stats.BytesCompleted), libcommon.ByteCount(stats.BytesTotal)),
					"download-time-left", downloadTimeLeft,
					"total-download-time", time.Since(downloadStartTime).Round(time.Second).String(),
					"download", libcommon.ByteCount(stats.DownloadRate)+"/s",
					"upload", libcommon.ByteCount(stats.UploadRate)+"/s",
				)
				log.Info(fmt.Sprintf("[%s] download", s.LogPrefix()),
					"peers", stats.PeersUnique,
					"connections", stats.ConnectionsTotal,
					"files", stats.FilesTotal,
					"alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys),
				)
			}
		}
	}

Finish:
	if cfg.snapshots.Cfg().Verify {
		if _, err := cfg.snapshotDownloader.Verify(ctx, &proto_downloader.VerifyRequest{}); err != nil {
			return err
		}
	}

	if err := cfg.snapshots.ReopenFolder(); err != nil {
		return err
	}
	if err := cfg.agg.OpenFolder(); err != nil {
		return err
	}

	if err := rawdb.WriteSnapshots(tx, cfg.snapshots.Files(), cfg.agg.Files()); err != nil {
		return err
	}
	if cfg.dbEventNotifier != nil { // can notify right here, even that write txn is not commit
		cfg.dbEventNotifier.OnNewSnapshot()
	}

	firstNonGenesis, err := rawdbv3.SecondKey(tx, kv.Headers)
	if err != nil {
		return err
	}
	if firstNonGenesis != nil {
		firstNonGenesisBlockNumber := binary.BigEndian.Uint64(firstNonGenesis)
		if cfg.snapshots.SegmentsMax()+1 < firstNonGenesisBlockNumber {
			log.Warn(fmt.Sprintf("[%s] Some blocks are not in snapshots and not in db", s.LogPrefix()), "max_in_snapshots", cfg.snapshots.SegmentsMax(), "min_in_db", firstNonGenesisBlockNumber)
		}
	}
	return nil
}

func calculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "999hrs:99m"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
}

/* ====== PRUNING ====== */
// snapshots pruning sections works more as a retiring of blocks
// retiring blocks means moving block data from db into snapshots
func SnapshotsPrune(s *PruneState, cfg SnapshotsCfg, ctx context.Context, tx kv.RwTx) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	sn := cfg.blockRetire.Snapshots()
	if sn != nil && sn.Cfg().Enabled && sn.Cfg().Produce {
		br := cfg.blockRetire
		if err := br.PruneAncientBlocks(tx, 100); err != nil {
			return err
		}

		//TODO: initialSync maybe save files progress here
		if cfg.agg.NeedSaveFilesListInDB() || br.NeedSaveFilesListInDB() {
			if err := rawdb.WriteSnapshots(tx, br.Snapshots().Files(), cfg.agg.Files()); err != nil {
				return err
			}
		}

		br.RetireBlocksInBackground(ctx, s.ForwardProgress, log.LvlDebug)
		cfg.agg.BuildFilesInBackground()
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
