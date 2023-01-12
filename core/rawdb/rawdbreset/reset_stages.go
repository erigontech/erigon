package rawdbreset

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
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

func ResetBlocks(tx kv.RwTx, db kv.RoDB, snapshots *snapshotsync.RoSnapshots, br services.FullBlockReader, dirs datadir.Dirs, cc params.ChainConfig, engine consensus.Engine) error {
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
		if err := stagedsync.FillDBFromSnapshots("fillind_db_from_snapshots", context.Background(), tx, dirs, snapshots, br, cc, engine); err != nil {
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

func WarmupExec(ctx context.Context, db kv.RwDB) (err error) {
	for _, tbl := range stateBuckets {
		WarmupTable(ctx, db, tbl, log.LvlInfo)
	}
	historyV3 := kvcfg.HistoryV3.FromDB(db)
	if historyV3 { //hist v2 is too big, if you have so much ram, just use `cat mdbx.dat > /dev/null` to warmup
		for _, tbl := range stateHistoryV3Buckets {
			WarmupTable(ctx, db, tbl, log.LvlInfo)
		}
	}
	return
}

func ResetExec(ctx context.Context, db kv.RwDB, chain string) (err error) {
	historyV3 := kvcfg.HistoryV3.FromDB(db)
	if historyV3 {
		stateHistoryBuckets = append(stateHistoryBuckets, stateHistoryV3Buckets...)
	}

	return db.Update(ctx, func(tx kv.RwTx) error {
		if err := clearStageProgress(tx, stages.Execution, stages.HashState, stages.IntermediateHashes); err != nil {
			return err
		}

		if err := clearTables(ctx, db, tx, stateBuckets...); err != nil {
			return nil
		}
		for _, b := range stateBuckets {
			if err := tx.ClearBucket(b); err != nil {
				return err
			}
		}

		if err := clearTables(ctx, db, tx, stateHistoryBuckets...); err != nil {
			return nil
		}
		if !historyV3 {
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
var stateBuckets = []string{
	kv.PlainState, kv.HashedAccounts, kv.HashedStorage, kv.TrieOfAccounts, kv.TrieOfStorage,
	kv.Epoch, kv.PendingEpoch, kv.BorReceipts,
	kv.Code, kv.PlainContractCode, kv.ContractCode, kv.IncarnationMap,
}
var stateHistoryBuckets = []string{
	kv.AccountChangeSet,
	kv.StorageChangeSet,
	kv.Receipts,
	kv.Log,
	kv.CallTraceSet,
}
var stateHistoryV3Buckets = []string{
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
}

func WarmupTable(ctx context.Context, db kv.RoDB, bucket string, lvl log.Lvl) {
	const ThreadsLimit = 128
	var total uint64
	db.View(ctx, func(tx kv.Tx) error {
		c, _ := tx.Cursor(bucket)
		total, _ = c.Count()
		return nil
	})
	if total < 10_000 {
		return
	}
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
					it, err := tx.Prefix(bucket, []byte{byte(i), byte(j)})
					if err != nil {
						return err
					}
					for it.HasNext() {
						_, _, err = it.Next()
						if err != nil {
							return err
						}
						progress.Inc()
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-logEvery.C:
							log.Log(lvl, fmt.Sprintf("Progress: %s %.2f%%", bucket, 100*float64(progress.Load())/float64(total)))
						default:
						}
					}
					return nil
				})
			})
		}
	}
	for i := 0; i < 1_000; i++ {
		i := i
		g.Go(func() error {
			return db.View(ctx, func(tx kv.Tx) error {
				seek := make([]byte, 8)
				binary.BigEndian.PutUint64(seek, uint64(i*100_000))
				it, err := tx.Prefix(bucket, seek)
				if err != nil {
					return err
				}
				for it.HasNext() {
					_, _, err = it.Next()
					if err != nil {
						return err
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-logEvery.C:
						log.Log(lvl, fmt.Sprintf("Progress: %s %.2f%%", bucket, 100*float64(progress.Load())/float64(total)))
					default:
					}
				}
				return nil
			})
		})
	}
	_ = g.Wait()
}
func Warmup(ctx context.Context, db kv.RwDB, lvl log.Lvl, stList ...stages.SyncStage) error {
	for _, st := range stList {
		for _, tbl := range Tables[st] {
			WarmupTable(ctx, db, tbl, lvl)
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		WarmupTable(ctx, db, bucket, log.LvlInfo)
	}()
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
