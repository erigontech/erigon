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
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapcfg"
	"github.com/ledgerwatch/log/v3"
)

func SpawnStageSnapshots(
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg HeadersCfg,
	initialCycle bool,
) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
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
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	return nil
}

func DownloadAndIndexSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg HeadersCfg, initialCycle bool) error {
	if !initialCycle || cfg.snapshots == nil || !cfg.snapshots.Cfg().Enabled {
		return nil
	}

	if err := WaitForDownloader(ctx, cfg, tx); err != nil {
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
				if err := snapshotsync.BuildMissedIndices(ctx, cfg.snapshots.Dir(), *chainID, cfg.tmpdir, workers, log.LvlInfo); err != nil {
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

	if s.BlockNumber < cfg.snapshots.BlocksAvailable() { // allow genesis
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()

		h2n := etl.NewCollector("Snapshots", cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer h2n.Close()
		h2n.LogLvl(log.LvlDebug)

		// fill some small tables from snapshots, in future we may store this data in snapshots also, but
		// for now easier just store them in db
		td := big.NewInt(0)
		if err := snapshotsync.ForEachHeader(ctx, cfg.snapshots, func(header *types.Header) error {
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
				log.Info(fmt.Sprintf("[%s] Writing total difficulty index for snapshots", s.LogPrefix()), "block_num", header.Number.Uint64())
			default:
			}
			return nil
		}); err != nil {
			return err
		}
		if err := h2n.Load(tx, kv.HeaderNumber, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
			return err
		}
		// ResetSequence - allow set arbitrary value to sequence (for example to decrement it to exact value)
		ok, err := cfg.snapshots.ViewTxs(cfg.snapshots.BlocksAvailable(), func(sn *snapshotsync.TxnSegment) error {
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
			return fmt.Errorf("snapshot not found for block: %d", cfg.snapshots.BlocksAvailable())
		}
		canonicalHash, err := cfg.blockReader.CanonicalHash(ctx, tx, cfg.snapshots.BlocksAvailable())
		if err != nil {
			return err
		}
		if err = rawdb.WriteHeadHeaderHash(tx, canonicalHash); err != nil {
			return err
		}
		if err := s.Update(tx, cfg.snapshots.BlocksAvailable()); err != nil {
			return err
		}
		_ = stages.SaveStageProgress(tx, stages.Bodies, cfg.snapshots.BlocksAvailable())
		_ = stages.SaveStageProgress(tx, stages.BlockHashes, cfg.snapshots.BlocksAvailable())
		_ = stages.SaveStageProgress(tx, stages.Senders, cfg.snapshots.BlocksAvailable())
		s.BlockNumber = cfg.snapshots.BlocksAvailable()
	}

	if err := cfg.hd.AddHeadersFromSnapshot(tx, cfg.snapshots.BlocksAvailable(), cfg.blockReader); err != nil {
		return err
	}

	return nil
}

// WaitForDownloader - wait for Downloader service to download all expected snapshots
// for MVP we sync with Downloader only once, in future will send new snapshots also
func WaitForDownloader(ctx context.Context, cfg HeadersCfg, tx kv.RwTx) error {
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
		log.Warn("[Snapshots] downloading missing snapshots")
	}

	// send all hashes to the Downloader service
	preverified := snapcfg.KnownCfg(cfg.chainConfig.ChainName, snInDB).Preverified
	downloadRequest := make([]snapshotsync.DownloadRequest, 0, len(preverified)+len(missingSnapshots))
	// build all download requests
	// builds preverified snapshots request
	for _, p := range preverified {
		downloadRequest = append(downloadRequest, snapshotsync.NewDownloadRequest(nil, p.Name, p.Hash))
	}
	// builds missing snapshots request
	for i := range missingSnapshots {
		downloadRequest = append(downloadRequest, snapshotsync.NewDownloadRequest(&missingSnapshots[i], "", ""))
	}

	log.Info("[Snapshots] Fetching torrent files metadata")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := snapshotsync.RequestSnapshotsDownload(ctx, downloadRequest, cfg.snapshotDownloader); err != nil {
			log.Error("[Snapshots] call downloader", "err", err)
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
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
				break Loop
			} else {
				if stats.MetadataReady < stats.FilesTotal {
					log.Info(fmt.Sprintf("[Snapshots] Waiting for torrents metadata: %d/%d", stats.MetadataReady, stats.FilesTotal))
					continue
				}
				libcommon.ReadMemStats(&m)
				downloadTimeLeft := calculateTime(stats.BytesTotal-stats.BytesCompleted, stats.DownloadRate)
				log.Info("[Snapshots] download",
					"progress", fmt.Sprintf("%.2f%% %s/%s", stats.Progress, libcommon.ByteCount(stats.BytesCompleted), libcommon.ByteCount(stats.BytesTotal)),
					"download-time", downloadTimeLeft,
					"download", libcommon.ByteCount(stats.DownloadRate)+"/s",
					"upload", libcommon.ByteCount(stats.UploadRate)+"/s",
				)
				log.Info("[Snapshots] download",
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
			log.Warn("[Snapshshots] Some blocks are not in snapshots and not in db", "max_in_snapshots", cfg.snapshots.SegmentsMax(), "min_in_db", firstNonGenesisBlockNumber)
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
