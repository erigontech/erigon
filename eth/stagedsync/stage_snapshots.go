package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/etl"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"github.com/ledgerwatch/log/v3"
)

type SnapshotsCfg struct {
	db          kv.RwDB
	chainConfig params.ChainConfig

	tmpdir string

	snapshots          *snapshotsync.RoSnapshots
	blockRetire        *snapshotsync.BlockRetire
	snapshotDownloader proto_downloader.DownloaderClient
	blockReader        services.FullBlockReader
	dbEventNotifier    snapshotsync.DBEventNotifier
	historyV2          bool
	agg                *state.Aggregator22
}

func StageSnapshotsCfg(
	db kv.RwDB,
	chainConfig params.ChainConfig,
	tmpdir string,
	snapshots *snapshotsync.RoSnapshots,
	blockRetire *snapshotsync.BlockRetire,
	snapshotDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	dbEventNotifier snapshotsync.DBEventNotifier,
	historyV2 bool,
	agg *state.Aggregator22,
) SnapshotsCfg {
	return SnapshotsCfg{
		db:                 db,
		chainConfig:        chainConfig,
		tmpdir:             tmpdir,
		snapshots:          snapshots,
		blockRetire:        blockRetire,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		dbEventNotifier:    dbEventNotifier,
		historyV2:          historyV2,
		agg:                agg,
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
				workers := cmp.InRange(1, 2, runtime.GOMAXPROCS(-1)-1)
				if err := snapshotsync.BuildMissedIndices(s.LogPrefix(), ctx, cfg.snapshots.Dir(), *chainID, cfg.tmpdir, workers, log.LvlInfo); err != nil {
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

	blocksAvailable := cfg.snapshots.BlocksAvailable()
	if s.BlockNumber < blocksAvailable { // allow genesis
		if err := s.Update(tx, blocksAvailable); err != nil {
			return err
		}
		s.BlockNumber = blocksAvailable
	}
	if err := FillDBFromSnapshots(s.LogPrefix(), ctx, tx, cfg.tmpdir, cfg.snapshots, cfg.blockReader); err != nil {
		return err
	}
	return nil
}

func FillDBFromSnapshots(logPrefix string, ctx context.Context, tx kv.RwTx, tmpdir string, sn *snapshotsync.RoSnapshots, blockReader services.HeaderAndCanonicalReader) error {
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
			h2n := etl.NewCollector("Snapshots", tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
			defer h2n.Close()
			h2n.LogLvl(log.LvlDebug)

			// fill some small tables from snapshots, in future we may store this data in snapshots also, but
			// for now easier just store them in db
			td := big.NewInt(0)
			if err := snapshotsync.ForEachHeader(ctx, sn, func(header *types.Header) error {
				blockNum, blockHash := header.Number.Uint64(), header.Hash()
				td.Add(td, header.Difficulty)

				if err := rawdb.WriteTd(tx, blockHash, blockNum, td); err != nil {
					return err
				}
				if err := rawdb.WriteCanonicalHash(tx, blockHash, blockNum); err != nil {
					return err
				}
				if err := h2n.Collect(blockHash[:], dbutils.EncodeBlockNumber(blockNum)); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					log.Info(fmt.Sprintf("[%s] Writing total difficulty index", logPrefix), "block_num", header.Number.Uint64())
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
				if err := rawdb.ResetSequence(tx, kv.EthTx, lastTxnID+1); err != nil {
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

			historyV2, err := rawdb.HistoryV2.Enabled(tx)
			if err != nil {
				return err
			}
			if historyV2 {
				var toBlock uint64
				if sn != nil {
					toBlock = sn.BlocksAvailable()
				}
				toBlock = cmp.Max(toBlock, progress)

				if err := rawdb.TxNums.WriteForGenesis(tx, 1); err != nil {
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
								log.Info(fmt.Sprintf("[%s] Writing MaxTxNums index for snapshots", logPrefix), "block_num", blockNum)
							default:
							}
							maxTxNum := baseTxNum + txAmount

							if err := rawdb.TxNums.Append(tx, blockNum, maxTxNum); err != nil {
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

	snInDB, err := rawdb.ReadSnapshots(tx)
	if err != nil {
		return err
	}
	dbEmpty := len(snInDB) == 0
	var missingSnapshots []snapshotsync.Range
	if !dbEmpty {
		_, missingSnapshots, err = snapshotsync.Segments(cfg.snapshots.Dir())
		if err != nil {
			return err
		}
	}
	if len(missingSnapshots) > 0 {
		log.Warn(fmt.Sprintf("[%s] downloading missing snapshots", s.LogPrefix()))
	}
	snHistInDB, err := rawdb.ReadHistorySnapshots(tx)
	if err != nil {
		return err
	}

	// send all hashes to the Downloader service
	preverifiedBlockSnapshots := snapcfg.KnownCfg(cfg.chainConfig.ChainName, snInDB, snHistInDB).Preverified
	downloadRequest := make([]snapshotsync.DownloadRequest, 0, len(preverifiedBlockSnapshots)+len(missingSnapshots))
	// build all download requests
	// builds preverified snapshots request
	for _, p := range preverifiedBlockSnapshots {
		downloadRequest = append(downloadRequest, snapshotsync.NewDownloadRequest(nil, p.Name, p.Hash))
	}
	if cfg.historyV2 {
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
				libcommon.ReadMemStats(&m)
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
	if err := cfg.agg.ReopenFiles(); err != nil {
		return err
	}

	if err := rawdb.WriteSnapshots(tx, cfg.snapshots.Files()); err != nil {
		return err
	}
	if cfg.dbEventNotifier != nil { // can notify right here, even that write txn is not commit
		cfg.dbEventNotifier.OnNewSnapshot()
	}

	firstNonGenesis, err := rawdb.SecondKey(tx, kv.Headers)
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
		return "999hrs:99m:99s"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60
	seconds := timeLeftInSeconds % 60

	return fmt.Sprintf("%dhrs:%dm:%ds", hours, minutes, seconds)
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
		if err := cfg.blockRetire.PruneAncientBlocks(tx); err != nil {
			return err
		}

		if err := retireBlocksInSingleBackgroundThread(s, cfg.blockRetire, ctx, tx); err != nil {
			return fmt.Errorf("retireBlocksInSingleBackgroundThread: %w", err)
		}
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// retiring blocks in a single thread in the brackground
func retireBlocksInSingleBackgroundThread(s *PruneState, blockRetire *snapshotsync.BlockRetire, ctx context.Context, tx kv.RwTx) (err error) {
	// if something already happens in background - noop
	if blockRetire.Working() {
		return nil
	}
	if res := blockRetire.Result(); res != nil {
		if res.Err != nil {
			return fmt.Errorf("[%s] retire blocks last error: %w, fromBlock=%d, toBlock=%d", s.LogPrefix(), res.Err, res.BlockFrom, res.BlockTo)
		}

		if err := rawdb.WriteSnapshots(tx, blockRetire.Snapshots().Files()); err != nil {
			return err
		}
	}

	blockRetire.RetireBlocksInBackground(ctx, s.ForwardProgress, log.LvlInfo)

	return nil
}
