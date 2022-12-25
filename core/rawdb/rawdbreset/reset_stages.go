package rawdbreset

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

func ResetState(db kv.RwDB, ctx context.Context, chain string) error {
	// don't reset senders here
	if err := Reset(ctx, db, stages.HashState); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.IntermediateHashes); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.AccountHistoryIndex, stages.StorageHistoryIndex); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.LogIndex); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.CallTraces); err != nil {
		return err
	}
	if err := db.Update(ctx, ResetTxLookup); err != nil {
		return err
	}
	if err := Reset(ctx, db, stages.Finish); err != nil {
		return err
	}

	if err := ResetExec(ctx, db, chain); err != nil {
		return err
	}
	return nil
}

func ResetBlocks(tx kv.RwTx, db kv.RoDB, snapshots *snapshotsync.RoSnapshots, br services.HeaderAndCanonicalReader, tmpdir string) error {
	// keep Genesis
	if err := rawdb.TruncateBlocks(context.Background(), tx, 1); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Bodies, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Headers, 1); err != nil {
		return fmt.Errorf("saving Bodies progress failed: %w", err)
	}
	if err := stages.SaveStageProgress(tx, stages.Snapshots, 0); err != nil {
		return fmt.Errorf("saving Snapshots progress failed: %w", err)
	}

	// remove all canonical markers from this point
	if err := rawdb.TruncateCanonicalHash(tx, 1, false); err != nil {
		return err
	}
	if err := rawdb.TruncateTd(tx, 1); err != nil {
		return err
	}
	hash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return err
	}
	if err = rawdb.WriteHeadHeaderHash(tx, hash); err != nil {
		return err
	}

	// ensure no garbage records left (it may happen if db is inconsistent)
	if err := tx.ForEach(kv.BlockBody, common2.EncodeTs(2), func(k, _ []byte) error { return tx.Delete(kv.BlockBody, k) }); err != nil {
		return err
	}

	if err := clearTables(context.Background(), db, tx,
		kv.NonCanonicalTxs,
		kv.EthTx,
		kv.MaxTxNum,
	); err != nil {
		return err
	}
	if err := rawdb.ResetSequence(tx, kv.EthTx, 0); err != nil {
		return err
	}
	if err := rawdb.ResetSequence(tx, kv.NonCanonicalTxs, 0); err != nil {
		return err
	}

	if snapshots != nil && snapshots.Cfg().Enabled && snapshots.BlocksAvailable() > 0 {
		if err := stagedsync.FillDBFromSnapshots("fillind_db_from_snapshots", context.Background(), tx, tmpdir, snapshots, br); err != nil {
			return err
		}
		_ = stages.SaveStageProgress(tx, stages.Snapshots, snapshots.BlocksAvailable())
		_ = stages.SaveStageProgress(tx, stages.Headers, snapshots.BlocksAvailable())
		_ = stages.SaveStageProgress(tx, stages.Bodies, snapshots.BlocksAvailable())
		_ = stages.SaveStageProgress(tx, stages.Senders, snapshots.BlocksAvailable())
	}

	return nil
}
func ResetSenders(ctx context.Context, db kv.RwDB, tx kv.RwTx) error {
	if err := clearTables(ctx, db, tx, kv.Senders); err != nil {
		return nil
	}
	return clearStageProgress(tx, stages.Senders)
}

func ResetExec(ctx context.Context, db kv.RwDB, chain string) (err error) {
	return db.Update(ctx, func(tx kv.RwTx) error {
		if err := clearStageProgress(tx, stages.Execution, stages.HashState, stages.IntermediateHashes); err != nil {
			return err
		}

		stateBuckets := []string{
			kv.PlainState, kv.HashedAccounts, kv.HashedStorage, kv.TrieOfAccounts, kv.TrieOfStorage,
			kv.Epoch, kv.PendingEpoch, kv.BorReceipts,
			kv.Code, kv.PlainContractCode, kv.ContractCode, kv.IncarnationMap,
		}
		if err := clearTables(ctx, db, tx, stateBuckets...); err != nil {
			return nil
		}
		for _, b := range stateBuckets {
			if err := tx.ClearBucket(b); err != nil {
				return err
			}
		}

		historyV3, err := kvcfg.HistoryV3.Enabled(tx)
		if err != nil {
			return err
		}
		if historyV3 {
			buckets := []string{
				kv.AccountHistoryKeys, kv.AccountIdx, kv.AccountHistoryVals, kv.AccountSettings,
				kv.StorageKeys, kv.StorageVals, kv.StorageHistoryKeys, kv.StorageHistoryVals, kv.StorageSettings, kv.StorageIdx,
				kv.CodeKeys, kv.CodeVals, kv.CodeHistoryKeys, kv.CodeHistoryVals, kv.CodeSettings, kv.CodeIdx,
				kv.AccountHistoryKeys, kv.AccountIdx, kv.AccountHistoryVals, kv.AccountSettings,
				kv.StorageHistoryKeys, kv.StorageIdx, kv.StorageHistoryVals, kv.StorageSettings,
				kv.CodeHistoryKeys, kv.CodeIdx, kv.CodeHistoryVals, kv.CodeSettings,
				kv.LogAddressKeys, kv.LogAddressIdx,
				kv.LogTopicsKeys, kv.LogTopicsIdx,
				kv.TracesFromKeys, kv.TracesFromIdx,
				kv.TracesToKeys, kv.TracesToIdx,

				kv.AccountChangeSet,
				kv.StorageChangeSet,
				kv.Receipts,
				kv.Log,
				kv.CallTraceSet,
			}
			if err := clearTables(ctx, db, tx, buckets...); err != nil {
				return nil
			}
		} else {
			if err := clearTables(ctx, db, tx,
				kv.AccountChangeSet,
				kv.StorageChangeSet,
				kv.Receipts,
				kv.Log,
				kv.CallTraceSet,
			); err != nil {
				return nil
			}

			genesis := core.DefaultGenesisBlockByChainName(chain)
			if _, _, err := genesis.WriteGenesisState(tx); err != nil {
				return err
			}
		}

		return nil
	})
}

func ResetTxLookup(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.TxLookup); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	return nil
}

var Tables = map[stages.SyncStage][]string{
	stages.HashState:           {kv.HashedAccounts, kv.HashedStorage, kv.ContractCode},
	stages.IntermediateHashes:  {kv.TrieOfAccounts, kv.TrieOfStorage},
	stages.CallTraces:          {kv.CallFromIndex, kv.CallToIndex},
	stages.LogIndex:            {kv.LogAddressIndex, kv.LogTopicIndex},
	stages.AccountHistoryIndex: {kv.AccountsHistory},
	stages.StorageHistoryIndex: {kv.StorageHistory},
	stages.Finish:              {},
}

func WarmupTable(ctx context.Context, db kv.RwDB, bucket string) {
	const ThreadsLimit = 5_000
	var total uint64
	db.View(ctx, func(tx kv.Tx) error {
		c, _ := tx.Cursor(bucket)
		total, _ = c.Count()
		return nil
	})
	progress := atomic.NewInt64(0)

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(ThreadsLimit)
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			i := i
			j := j
			g.Go(func() error {
				return db.View(ctx, func(tx kv.Tx) error {
					return tx.ForPrefix(bucket, []byte{byte(i), byte(j)}, func(k, v []byte) error {
						progress.Inc()

						select {
						case <-logEvery.C:
							log.Info(fmt.Sprintf("Progress: %s %.2f%%", bucket, 100*float64(progress.Load())/float64(total)))
						default:
						}
						return nil
					})
				})
			})
		}
	}
	_ = g.Wait()
}
func Warmup(ctx context.Context, db kv.RwDB, stList ...stages.SyncStage) error {
	for _, st := range stList {
		for _, tbl := range Tables[st] {
			WarmupTable(ctx, db, tbl)
		}
	}
	return nil
}

func Reset(ctx context.Context, db kv.RwDB, stagesList ...stages.SyncStage) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		for _, st := range stagesList {
			if err := clearTables(ctx, db, tx, Tables[st]...); err != nil {
				return nil
			}
			if err := clearStageProgress(tx, stagesList...); err != nil {
				return err
			}
		}
		return nil
	})
}

func warmup(ctx context.Context, db kv.RoDB, bucket string) func() {
	wg := sync.WaitGroup{}
	for i := 0; i < 256; i++ {
		prefix := []byte{byte(i)}
		wg.Add(1)
		go func(perfix []byte) {
			defer wg.Done()
			if err := db.View(ctx, func(tx kv.Tx) error {
				return tx.ForEach(bucket, prefix, func(k, v []byte) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}
					return nil
				})
			}); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Warn("warmup", "err", err)
				}
			}
		}(prefix)
	}
	return func() { wg.Wait() }
}

func clearTables(ctx context.Context, db kv.RoDB, tx kv.RwTx, tables ...string) error {
	for _, tbl := range tables {
		if err := clearTable(ctx, db, tx, tbl); err != nil {
			return err
		}
	}
	return nil
}

func clearTable(ctx context.Context, db kv.RoDB, tx kv.RwTx, table string) error {
	ctx, cancel := context.WithCancel(ctx)
	clean := warmup(ctx, db, table)
	defer func() {
		cancel()
		clean()
	}()
	log.Info("Clear", "table", table)
	return tx.ClearBucket(table)
}

func clearStageProgress(tx kv.RwTx, stagesList ...stages.SyncStage) error {
	for _, stage := range stagesList {
		if err := stages.SaveStageProgress(tx, stage, 0); err != nil {
			return err
		}
		if err := stages.SaveStagePruneProgress(tx, stage, 0); err != nil {
			return err
		}
	}
	return nil
}
