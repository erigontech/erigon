package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
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
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

type SnapshotsCfg struct {
	db          kv.RwDB
	chainConfig chain.Config
	dirs        datadir.Dirs

	blockRetire        services.BlockRetire
	snapshotDownloader proto_downloader.DownloaderClient
	blockReader        services.FullBlockReader
	dbEventNotifier    services.DBEventNotifier

	historyV3 bool
	agg       *state.AggregatorV3
}

func StageSnapshotsCfg(db kv.RwDB,
	chainConfig chain.Config, dirs datadir.Dirs,
	blockRetire services.BlockRetire,
	snapshotDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader, dbEventNotifier services.DBEventNotifier,
	historyV3 bool, agg *state.AggregatorV3,
) SnapshotsCfg {
	return SnapshotsCfg{
		db:                 db,
		chainConfig:        chainConfig,
		dirs:               dirs,
		blockRetire:        blockRetire,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		dbEventNotifier:    dbEventNotifier,
		historyV3:          historyV3,
		agg:                agg,
	}
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

	if err := snapshotsync.WaitForDownloader(s.LogPrefix(), ctx, cfg.historyV3, cfg.agg, tx, cfg.blockReader, cfg.dbEventNotifier, &cfg.chainConfig, cfg.snapshotDownloader); err != nil {
		return err
	}

	cfg.agg.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
		_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
		return histBlockNumProgress
	})

	if err := cfg.blockRetire.BuildMissedIndicesIfNeed(ctx, s.LogPrefix(), cfg.dbEventNotifier, &cfg.chainConfig); err != nil {
		return err
	}

	if cfg.historyV3 {
		cfg.agg.CleanDir()

		indexWorkers := estimate.IndexSnapshot.Workers()
		if err := cfg.agg.BuildMissedIndices(ctx, indexWorkers); err != nil {
			return err
		}
		if cfg.dbEventNotifier != nil {
			cfg.dbEventNotifier.OnNewSnapshot()
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
func SnapshotsPrune(s *PruneState, initialCycle bool, cfg SnapshotsCfg, ctx context.Context, tx kv.RwTx) (err error) {
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
		//cfg.agg.BuildFilesInBackground()
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
