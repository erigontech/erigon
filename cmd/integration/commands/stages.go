package commands

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/c2h5oh/datasize"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	reset2 "github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
	"github.com/ledgerwatch/secp256k1"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
)

var cmdStageSnapshots = &cobra.Command{
	Use:   "stage_snapshots",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageSnapshots(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageHeaders = &cobra.Command{
	Use:   "stage_headers",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageHeaders(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageBodies = &cobra.Command{
	Use:   "stage_bodies",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageBodies(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageSenders = &cobra.Command{
	Use:   "stage_senders",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageSenders(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageExec = &cobra.Command{
	Use:   "stage_exec",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageExec(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageTrie = &cobra.Command{
	Use:   "stage_trie",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageTrie(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageHashState = &cobra.Command{
	Use:   "stage_hash_state",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageHashState(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageHistory = &cobra.Command{
	Use:   "stage_history",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageHistory(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdLogIndex = &cobra.Command{
	Use:   "stage_log_index",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageLogIndex(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdCallTraces = &cobra.Command{
	Use:   "stage_call_traces",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageCallTraces(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageTxLookup = &cobra.Command{
	Use:   "stage_tx_lookup",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageTxLookup(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}
var cmdPrintStages = &cobra.Command{
	Use:   "print_stages",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata).Readonly(), false)
		defer db.Close()

		if err := printAllStages(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdPrintMigrations = &cobra.Command{
	Use:   "print_migrations",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), false)
		defer db.Close()
		if err := printAppliedMigrations(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdRemoveMigration = &cobra.Command{
	Use:   "remove_migration",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		db := openDB(dbCfg(kv.ChainDB, chaindata), false)
		defer db.Close()
		if err := removeMigration(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdRunMigrations = &cobra.Command{
	Use:   "run_migrations",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
		return nil
	},
}

var cmdSetPrune = &cobra.Command{
	Use:   "force_set_prune",
	Short: "Override existing --prune flag value (if you know what you are doing)",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		return overrideStorageMode(db)
	},
}

var cmdSetSnap = &cobra.Command{
	Use:   "force_set_snapshot",
	Short: "Override existing --snapshots flag value (if you know what you are doing)",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		snapshots := allSnapshots(db)
		if err := db.Update(context.Background(), func(tx kv.RwTx) error {
			return snap.ForceSetFlags(tx, snapshots.Cfg())
		}); err != nil {
			return err
		}
		return nil
	},
}

var cmdForceSetHistoryV2 = &cobra.Command{
	Use:   "force_set_history_v2",
	Short: "Override existing --history.v2 flag value (if you know what you are doing)",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		return db.Update(context.Background(), func(tx kv.RwTx) error {
			return rawdb.HistoryV2.ForceWrite(tx, _forceSetHistoryV2)
		})
	},
}

func init() {
	withDataDir(cmdPrintStages)
	withChain(cmdPrintStages)
	withHeimdall(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	withIntegrityChecks(cmdStageSenders)
	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDataDir(cmdStageSenders)
	withChain(cmdStageSenders)
	withHeimdall(cmdStageSenders)

	rootCmd.AddCommand(cmdStageSenders)

	withDataDir(cmdStageSnapshots)
	withReset(cmdStageSnapshots)

	rootCmd.AddCommand(cmdStageSnapshots)

	withDataDir(cmdStageHeaders)
	withUnwind(cmdStageHeaders)
	withReset(cmdStageHeaders)
	withChain(cmdStageHeaders)
	withHeimdall(cmdStageHeaders)

	rootCmd.AddCommand(cmdStageHeaders)

	withDataDir(cmdStageBodies)
	withUnwind(cmdStageBodies)
	withChain(cmdStageBodies)
	withHeimdall(cmdStageBodies)

	rootCmd.AddCommand(cmdStageBodies)

	withDataDir(cmdStageExec)
	withReset(cmdStageExec)
	withBlock(cmdStageExec)
	withUnwind(cmdStageExec)
	withPruneTo(cmdStageExec)
	withBatchSize(cmdStageExec)
	withTxTrace(cmdStageExec)
	withChain(cmdStageExec)
	withHeimdall(cmdStageExec)
	withWorkers(cmdStageExec)

	rootCmd.AddCommand(cmdStageExec)

	withDataDir(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withPruneTo(cmdStageHashState)
	withBatchSize(cmdStageHashState)
	withChain(cmdStageHashState)
	withHeimdall(cmdStageHashState)

	rootCmd.AddCommand(cmdStageHashState)

	withDataDir(cmdStageTrie)
	withReset(cmdStageTrie)
	withBlock(cmdStageTrie)
	withUnwind(cmdStageTrie)
	withPruneTo(cmdStageTrie)
	withIntegrityChecks(cmdStageTrie)
	withChain(cmdStageTrie)
	withHeimdall(cmdStageTrie)

	rootCmd.AddCommand(cmdStageTrie)

	withDataDir(cmdStageHistory)
	withReset(cmdStageHistory)
	withBlock(cmdStageHistory)
	withUnwind(cmdStageHistory)
	withPruneTo(cmdStageHistory)
	withChain(cmdStageHistory)
	withHeimdall(cmdStageHistory)

	rootCmd.AddCommand(cmdStageHistory)

	withDataDir(cmdLogIndex)
	withReset(cmdLogIndex)
	withBlock(cmdLogIndex)
	withUnwind(cmdLogIndex)
	withPruneTo(cmdLogIndex)
	withChain(cmdLogIndex)
	withHeimdall(cmdLogIndex)

	rootCmd.AddCommand(cmdLogIndex)

	withDataDir(cmdCallTraces)
	withReset(cmdCallTraces)
	withBlock(cmdCallTraces)
	withUnwind(cmdCallTraces)
	withPruneTo(cmdCallTraces)
	withChain(cmdCallTraces)
	withHeimdall(cmdCallTraces)

	rootCmd.AddCommand(cmdCallTraces)

	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDataDir(cmdStageTxLookup)
	withPruneTo(cmdStageTxLookup)
	withChain(cmdStageTxLookup)
	withHeimdall(cmdStageTxLookup)

	rootCmd.AddCommand(cmdStageTxLookup)

	withDataDir(cmdPrintMigrations)
	rootCmd.AddCommand(cmdPrintMigrations)

	withDataDir(cmdRemoveMigration)
	withMigration(cmdRemoveMigration)
	withChain(cmdRemoveMigration)
	withHeimdall(cmdRemoveMigration)
	rootCmd.AddCommand(cmdRemoveMigration)

	withDataDir(cmdRunMigrations)
	withChain(cmdRunMigrations)
	withHeimdall(cmdRunMigrations)
	rootCmd.AddCommand(cmdRunMigrations)

	withDataDir2(cmdSetSnap)
	withChain(cmdSetSnap)
	rootCmd.AddCommand(cmdSetSnap)

	withDataDir2(cmdForceSetHistoryV2)
	cmdForceSetHistoryV2.Flags().BoolVar(&_forceSetHistoryV2, "history.v2", false, "")
	rootCmd.AddCommand(cmdForceSetHistoryV2)

	withDataDir(cmdSetPrune)
	withChain(cmdSetPrune)
	cmdSetPrune.Flags().StringVar(&pruneFlag, "prune", "hrtc", "")
	cmdSetPrune.Flags().Uint64Var(&pruneH, "prune.h.older", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneR, "prune.r.older", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneT, "prune.t.older", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneC, "prune.c.older", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneHBefore, "prune.h.before", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneRBefore, "prune.r.before", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneTBefore, "prune.t.before", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneCBefore, "prune.c.before", 0, "")
	cmdSetPrune.Flags().StringSliceVar(&experiments, "experiments", nil, "Storage mode to override database")
	rootCmd.AddCommand(cmdSetPrune)
}

func stageSnapshots(db kv.RwDB, ctx context.Context) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		if reset {
			if err := stages.SaveStageProgress(tx, stages.Snapshots, 0); err != nil {
				return fmt.Errorf("saving Snapshots progress failed: %w", err)
			}
		}
		progress, err := stages.GetStageProgress(tx, stages.Snapshots)
		if err != nil {
			return fmt.Errorf("re-read Snapshots progress: %w", err)
		}
		log.Info("Progress", "snapshots", progress)
		return nil
	})
}

func stageHeaders(db kv.RwDB, ctx context.Context) error {
	sn, br := allSnapshots(db), getBlockReader(db)
	return db.Update(ctx, func(tx kv.RwTx) error {
		if !(unwind > 0 || reset) {
			log.Info("This command only works with --unwind or --reset options")
		}

		if reset {
			if err := reset2.ResetBlocks(tx, sn, br); err != nil {
				return err
			}
			return nil
		}

		progress, err := stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return fmt.Errorf("read Bodies progress: %w", err)
		}
		var unwindTo uint64
		if unwind > progress {
			unwindTo = 1 // keep genesis
		} else {
			unwindTo = uint64(cmp.Max(1, int(progress)-int(unwind)))
		}

		if err = stages.SaveStageProgress(tx, stages.Headers, unwindTo); err != nil {
			return fmt.Errorf("saving Headers progress failed: %w", err)
		}
		progress, err = stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return fmt.Errorf("re-read Headers progress: %w", err)
		}
		{ // hard-unwind stage_body also
			if err := rawdb.TruncateBlocks(ctx, tx, progress+1); err != nil {
				return err
			}
			progressBodies, err := stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return fmt.Errorf("read Bodies progress: %w", err)
			}
			if progress < progressBodies {
				if err = stages.SaveStageProgress(tx, stages.Bodies, progress); err != nil {
					return fmt.Errorf("saving Bodies progress failed: %w", err)
				}
			}
		}
		// remove all canonical markers from this point
		if err = rawdb.TruncateCanonicalHash(tx, progress+1, false); err != nil {
			return err
		}
		if err = rawdb.TruncateTd(tx, progress+1); err != nil {
			return err
		}
		hash, err := rawdb.ReadCanonicalHash(tx, progress-1)
		if err != nil {
			return err
		}
		if err = rawdb.WriteHeadHeaderHash(tx, hash); err != nil {
			return err
		}

		log.Info("Progress", "headers", progress)
		return nil
	})
}

func stageBodies(db kv.RwDB, ctx context.Context) error {
	_, _, sync, _, _ := newSync(ctx, db, nil)
	chainConfig, historyV2 := fromdb.ChainConfig(db), fromdb.HistoryV2(db)

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		s := stage(sync, tx, nil, stages.Bodies)

		if unwind > 0 {
			if unwind > s.BlockNumber {
				return fmt.Errorf("cannot unwind past 0")
			}

			u := sync.NewUnwindState(stages.Bodies, s.BlockNumber-unwind, s.BlockNumber)
			if err := stagedsync.UnwindBodiesStage(u, tx, stagedsync.StageBodiesCfg(db, nil, nil, nil, nil, 0, *chainConfig, 0, allSnapshots(db), getBlockReader(db), historyV2), ctx); err != nil {
				return err
			}

			progress, err := stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return fmt.Errorf("re-read Bodies progress: %w", err)
			}
			log.Info("Progress", "bodies", progress)
			return nil
		}
		log.Info("This command only works with --unwind option")
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func stageSenders(db kv.RwDB, ctx context.Context) error {
	tmpdir := filepath.Join(datadirCli, etl.TmpDirName)
	_, _, sync, _, _ := newSync(ctx, db, nil)
	chainConfig := fromdb.ChainConfig(db)

	must(sync.SetCurrentStage(stages.Senders))

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if integritySlow {
		secp256k1.ContextForThread(1)
		for i := block; ; i++ {
			if err := common2.Stopped(ctx.Done()); err != nil {
				return err
			}
			withoutSenders, _ := rawdb.ReadBlockByNumber(tx, i)
			if withoutSenders == nil {
				break
			}
			txs := withoutSenders.Transactions()
			_, senders, _ := rawdb.CanonicalBlockByNumberWithSenders(tx, i)
			if txs.Len() != len(senders) {
				log.Error("not equal amount of senders", "block", i, "db", len(senders), "expect", txs.Len())
				return nil
			}
			if txs.Len() == 0 {
				continue
			}
			signer := types.MakeSigner(chainConfig, i)
			for j := 0; j < txs.Len(); j++ {
				from, err := signer.Sender(txs[j])
				if err != nil {
					return err
				}
				if !bytes.Equal(from[:], senders[j][:]) {
					log.Error("wrong sender", "block", i, "tx", j, "db", fmt.Sprintf("%x", senders[j]), "expect", fmt.Sprintf("%x", from))
				}
			}
			if i%10 == 0 {
				log.Info("checked", "block", i)
			}
		}
		return nil
	}

	if reset {
		err = reset2.ResetSenders(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	s := stage(sync, tx, nil, stages.Senders)
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	var br *snapshotsync.BlockRetire
	snapshots := allSnapshots(db)
	if snapshots.Cfg().Enabled {
		workers := runtime.GOMAXPROCS(-1) - 1
		if workers < 1 {
			workers = 1
		}
		br = snapshotsync.NewBlockRetire(workers, tmpdir, snapshots, db, nil, nil)
	}

	pm, err := prune.Get(tx)
	if err != nil {
		return err
	}

	cfg := stagedsync.StageSendersCfg(db, chainConfig, false, tmpdir, pm, br, nil)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.Senders, s.BlockNumber-unwind, s.BlockNumber)
		if err = stagedsync.UnwindSendersStage(u, tx, cfg, ctx); err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.Senders, s.BlockNumber, tx, db)
		if err != nil {
			return err
		}
		if err = stagedsync.PruneSendersStage(p, tx, cfg, ctx); err != nil {
			return err
		}
		return nil
	} else {
		if err = stagedsync.SpawnRecoverSendersStage(cfg, s, sync, tx, block, ctx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageExec(db kv.RwDB, ctx context.Context) error {
	chainConfig, historyV2, pm := fromdb.ChainConfig(db), fromdb.HistoryV2(db), fromdb.PruneMode(db)
	dirs := datadir.New(datadirCli)
	engine, vmConfig, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.Execution))

	if reset {
		//if historyV2 {
		//	dir.Recreate(path.Join(dirs.DataDir, "agg22"))
		//	dir.Recreate(path.Join(dirs.DataDir, "db22"))
		//	dir.Recreate(path.Join(dirs.DataDir, "erigon22"))
		//}
		if err := db.Update(ctx, func(tx kv.RwTx) error { return reset2.ResetExec(tx, chain) }); err != nil {
			return err
		}
		return nil
	}

	if txtrace {
		// Activate tracing and writing into json files for each transaction
		vmConfig.Tracer = nil
		vmConfig.Debug = true
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	s := stage(sync, nil, db, stages.Execution)

	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	genesis := core.DefaultGenesisBlockByChainName(chain)
	cfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil,
		/*stateStream=*/ false,
		/*badBlockHalt=*/ false, historyV2, dirs, getBlockReader(db), nil, genesis, int(workers), agg())
	if unwind > 0 {
		u := sync.NewUnwindState(stages.Execution, s.BlockNumber-unwind, s.BlockNumber)
		err := stagedsync.UnwindExecutionStage(u, s, nil, ctx, cfg, true)
		if err != nil {
			return err
		}
		return nil
	}

	if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.Execution, s.BlockNumber, nil, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneExecutionStage(p, nil, cfg, ctx, true)
		if err != nil {
			return err
		}
		return nil
	}

	err := stagedsync.SpawnExecuteBlocksStage(s, sync, nil, block, ctx, cfg, true)
	if err != nil {
		return err
	}
	return nil
}

func stageTrie(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV2 := datadir.New(datadirCli), fromdb.PruneMode(db), fromdb.HistoryV2(db)
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.IntermediateHashes))

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		if err := stagedsync.ResetIH(tx); err != nil {
			return err
		}
		return tx.Commit()
	}
	execStage := stage(sync, tx, nil, stages.Execution)
	s := stage(sync, tx, nil, stages.IntermediateHashes)

	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	log.Info("StageExec", "progress", execStage.BlockNumber)
	log.Info("StageTrie", "progress", s.BlockNumber)
	cfg := stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, getBlockReader(db), nil, historyV2, agg())
	if unwind > 0 {
		u := sync.NewUnwindState(stages.IntermediateHashes, s.BlockNumber-unwind, s.BlockNumber)
		if err := stagedsync.UnwindIntermediateHashesStage(u, s, tx, cfg, ctx); err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.IntermediateHashes, s.BlockNumber, tx, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneIntermediateHashesStage(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		if _, err := stagedsync.SpawnIntermediateHashesStage(s, sync /* Unwinder */, tx, cfg, ctx); err != nil {
			return err
		}
	}
	integrity.Trie(db, tx, integritySlow, ctx)
	return tx.Commit()
}

func stageHashState(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV2 := datadir.New(datadirCli), fromdb.PruneMode(db), fromdb.HistoryV2(db)
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.HashState))

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = stagedsync.ResetHashState(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	s := stage(sync, tx, nil, stages.HashState)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	cfg := stagedsync.StageHashStateCfg(db, dirs, historyV2, agg())
	if unwind > 0 {
		u := sync.NewUnwindState(stages.HashState, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindHashStateStage(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.HashState, s.BlockNumber, tx, nil)
		if err != nil {
			return err
		}
		err = stagedsync.PruneHashStateStage(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnHashStateStage(s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageLogIndex(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV2 := datadir.New(datadirCli), fromdb.PruneMode(db), fromdb.HistoryV2(db)
	if historyV2 {
		return fmt.Errorf("this stage is disable in --history.v2=true")
	}
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.LogIndex))
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = reset2.ResetLogIndex(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	execAt := progress(tx, stages.Execution)
	s := stage(sync, tx, nil, stages.LogIndex)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	log.Info("Stage exec", "progress", execAt)
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	cfg := stagedsync.StageLogIndexCfg(db, pm, dirs.Tmp)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.LogIndex, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindLogIndex(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.LogIndex, s.BlockNumber, nil, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneLogIndex(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnLogIndex(s, tx, cfg, ctx, block); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageCallTraces(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV2 := datadir.New(datadirCli), fromdb.PruneMode(db), fromdb.HistoryV2(db)
	if historyV2 {
		return fmt.Errorf("this stage is disable in --history.v2=true")
	}
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.CallTraces))
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = reset2.ResetCallTraces(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	execStage := progress(tx, stages.Execution)
	s := stage(sync, tx, nil, stages.CallTraces)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}
	log.Info("ID exec", "progress", execStage)
	if block != 0 {
		s.BlockNumber = block
		log.Info("Overriding initial state", "block", block)
	}
	log.Info("ID call traces", "progress", s.BlockNumber)

	cfg := stagedsync.StageCallTracesCfg(db, pm, block, dirs.Tmp)

	if unwind > 0 {
		u := sync.NewUnwindState(stages.CallTraces, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindCallTraces(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.CallTraces, s.BlockNumber, tx, nil)
		if err != nil {
			return err
		}
		err = stagedsync.PruneCallTraces(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnCallTraces(s, tx, cfg, ctx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageHistory(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV2 := datadir.New(datadirCli), fromdb.PruneMode(db), fromdb.HistoryV2(db)
	if historyV2 {
		return fmt.Errorf("this stage is disable in --history.v2=true")
	}
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.AccountHistoryIndex))

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = reset2.ResetHistory(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	execStage := progress(tx, stages.Execution)
	stageStorage := stage(sync, tx, nil, stages.StorageHistoryIndex)
	stageAcc := stage(sync, tx, nil, stages.AccountHistoryIndex)
	if pruneTo > 0 {
		pm.History = prune.Distance(stageAcc.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(stageAcc.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(stageAcc.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(stageAcc.BlockNumber - pruneTo)
	}
	log.Info("ID exec", "progress", execStage)
	log.Info("ID acc history", "progress", stageAcc.BlockNumber)
	log.Info("ID storage history", "progress", stageStorage.BlockNumber)

	cfg := stagedsync.StageHistoryCfg(db, pm, dirs.Tmp)
	if unwind > 0 { //nolint:staticcheck
		u := sync.NewUnwindState(stages.StorageHistoryIndex, stageStorage.BlockNumber-unwind, stageStorage.BlockNumber)
		if err := stagedsync.UnwindStorageHistoryIndex(u, stageStorage, tx, cfg, ctx); err != nil {
			return err
		}
		u = sync.NewUnwindState(stages.AccountHistoryIndex, stageAcc.BlockNumber-unwind, stageAcc.BlockNumber)
		if err := stagedsync.UnwindAccountHistoryIndex(u, stageAcc, tx, cfg, ctx); err != nil {
			return err
		}
	} else if pruneTo > 0 {
		pa, err := sync.PruneStageState(stages.AccountHistoryIndex, stageAcc.BlockNumber, tx, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneAccountHistoryIndex(pa, tx, cfg, ctx)
		if err != nil {
			return err
		}
		ps, err := sync.PruneStageState(stages.StorageHistoryIndex, stageStorage.BlockNumber, tx, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneStorageHistoryIndex(ps, tx, cfg, ctx)
		if err != nil {
			return err
		}
		_ = printStages(tx, allSnapshots(db))
	} else {
		if err := stagedsync.SpawnAccountHistoryIndex(stageAcc, tx, cfg, ctx); err != nil {
			return err
		}
		if err := stagedsync.SpawnStorageHistoryIndex(stageStorage, tx, cfg, ctx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageTxLookup(db kv.RwDB, ctx context.Context) error {
	dirs, pm := datadir.New(datadirCli), fromdb.PruneMode(db)
	_, _, sync, _, _ := newSync(ctx, db, nil)
	chainConfig := fromdb.ChainConfig(db)
	must(sync.SetCurrentStage(stages.TxLookup))

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = reset2.ResetTxLookup(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	s := stage(sync, tx, nil, stages.TxLookup)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)
	isBor := chainConfig.Bor != nil
	var sprint uint64
	if isBor {
		sprint = chainConfig.Bor.Sprint
	}

	cfg := stagedsync.StageTxLookupCfg(db, pm, dirs.Tmp, allSnapshots(db), isBor, sprint)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.TxLookup, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindTxLookup(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.TxLookup, s.BlockNumber, tx, nil)
		if err != nil {
			return err
		}
		err = stagedsync.PruneTxLookup(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnTxLookup(s, tx, block, cfg, ctx)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func printAllStages(db kv.RoDB, ctx context.Context) error {
	return db.View(ctx, func(tx kv.Tx) error { return printStages(tx, allSnapshots(db)) })
}

func printAppliedMigrations(db kv.RwDB, ctx context.Context) error {
	return db.View(ctx, func(tx kv.Tx) error {
		applied, err := migrations.AppliedMigrations(tx, false /* withPayload */)
		if err != nil {
			return err
		}
		var appliedStrs = make([]string, len(applied))
		i := 0
		for k := range applied {
			appliedStrs[i] = k
			i++
		}
		slices.Sort(appliedStrs)
		log.Info("Applied", "migrations", strings.Join(appliedStrs, " "))
		return nil
	})
}

func removeMigration(db kv.RwDB, ctx context.Context) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Delete(kv.Migrations, []byte(migration))
	})
}

var openSnapshotOnce sync.Once
var _allSnapshotsSingleton *snapshotsync.RoSnapshots

func allSnapshots(db kv.RoDB) *snapshotsync.RoSnapshots {
	openSnapshotOnce.Do(func() {
		var useSnapshots bool
		_ = db.View(context.Background(), func(tx kv.Tx) error {
			useSnapshots, _ = snap.Enabled(tx)
			return nil
		})
		snapCfg := ethconfig.NewSnapCfg(useSnapshots, true, true)
		_allSnapshotsSingleton = snapshotsync.NewRoSnapshots(snapCfg, filepath.Join(datadirCli, "snapshots"))
		if useSnapshots {
			if err := _allSnapshotsSingleton.ReopenFolder(); err != nil {
				panic(err)
			}
		}
	})
	return _allSnapshotsSingleton
}

var openAggOnce sync.Once
var _aggSingleton *libstate.Aggregator22

func agg() *libstate.Aggregator22 {
	openAggOnce.Do(func() {
		aggDir := path.Join(datadirCli, "snapshots", "history")
		dir.MustExist(aggDir)
		var err error
		_aggSingleton, err = libstate.NewAggregator22(aggDir, ethconfig.HistoryV2AggregationStep)
		if err != nil {
			panic(err)
		}
		err = _aggSingleton.ReopenFiles()
		if err != nil {
			panic(err)
		}
	})

	return _aggSingleton
}

var openBlockReaderOnce sync.Once
var _blockReaderSingleton services.FullBlockReader

func getBlockReader(db kv.RoDB) (blockReader services.FullBlockReader) {
	openBlockReaderOnce.Do(func() {
		_blockReaderSingleton = snapshotsync.NewBlockReader()
		if sn := allSnapshots(db); sn.Cfg().Enabled {
			x := snapshotsync.NewBlockReaderWithSnapshots(sn)
			_blockReaderSingleton = x
		}
	})
	return _blockReaderSingleton
}

func newSync(ctx context.Context, db kv.RwDB, miningConfig *params.MiningConfig) (consensus.Engine, *vm.Config, *stagedsync.Sync, *stagedsync.Sync, stagedsync.MiningState) {
	logger := log.New()
	dirs, historyV2, pm := datadir.New(datadirCli), fromdb.HistoryV2(db), fromdb.PruneMode(db)

	vmConfig := &vm.Config{}

	events := privateapi.NewEvents()

	genesis := core.DefaultGenesisBlockByChainName(chain)
	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	//log.Info("Initialised chain configuration", "config", chainConfig)

	// Apply special hacks for BSC params
	if chainConfig.Parlia != nil {
		params.ApplyBinanceSmartChainParams()
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	cfg := ethconfig.Defaults
	cfg.HistoryV2 = historyV2
	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.DeprecatedTxPool.Disable = true
	if miningConfig != nil {
		cfg.Miner = *miningConfig
	}
	cfg.Dirs = datadir.New(datadirCli)
	allSn := allSnapshots(db)
	cfg.Snapshot = allSn.Cfg()

	engine := initConsensusEngine(chainConfig, logger, allSn, cfg.Dirs.DataDir, db)

	br := getBlockReader(db)
	sentryControlServer, err := sentry.NewMultiClient(
		db,
		"",
		chainConfig,
		genesisBlock.Hash(),
		engine,
		1,
		nil,
		ethconfig.Defaults.Sync,
		br,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	sync, err := stages2.NewStagedSync(context.Background(), db, p2p.Config{}, &cfg, sentryControlServer, &stagedsync.Notifications{}, nil, allSn, agg(), nil)
	if err != nil {
		panic(err)
	}
	miner := stagedsync.NewMiningState(&cfg.Miner)
	miningCancel := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(miningCancel)
	}()
	miningSync := stagedsync.New(
		stagedsync.MiningStages(ctx,
			stagedsync.StageMiningCreateBlockCfg(db, miner, *chainConfig, engine, nil, nil, nil, dirs.Tmp),
			stagedsync.StageMiningExecCfg(db, miner, events, *chainConfig, engine, &vm.Config{}, dirs.Tmp, nil),
			stagedsync.StageHashStateCfg(db, dirs, historyV2, agg()),
			stagedsync.StageTrieCfg(db, false, true, false, dirs.Tmp, br, nil, historyV2, agg()),
			stagedsync.StageMiningFinishCfg(db, *chainConfig, engine, miner, miningCancel),
		),
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
	)

	return engine, vmConfig, sync, miningSync, miner
}

func progress(tx kv.Getter, stage stages.SyncStage) uint64 {
	res, err := stages.GetStageProgress(tx, stage)
	if err != nil {
		panic(err)
	}
	return res
}

func stage(st *stagedsync.Sync, tx kv.Tx, db kv.RoDB, stage stages.SyncStage) *stagedsync.StageState {
	res, err := st.StageState(stage, tx, db)
	if err != nil {
		panic(err)
	}
	return res
}

func overrideStorageMode(db kv.RwDB) error {
	chainConfig := fromdb.ChainConfig(db)
	pm, err := prune.FromCli(chainConfig.ChainID.Uint64(), pruneFlag, pruneH, pruneR, pruneT, pruneC,
		pruneHBefore, pruneRBefore, pruneTBefore, pruneCBefore, experiments)
	if err != nil {
		return err
	}
	return db.Update(context.Background(), func(tx kv.RwTx) error {
		if err = prune.Override(tx, pm); err != nil {
			return err
		}
		pm, err = prune.Get(tx)
		if err != nil {
			return err
		}
		log.Info("Storage mode in DB", "mode", pm.String())
		return nil
	})
}

func initConsensusEngine(chainConfig *params.ChainConfig, logger log.Logger, snapshots *snapshotsync.RoSnapshots, datadir string, db kv.RwDB) (engine consensus.Engine) {
	config := ethconfig.Defaults

	switch {
	case chainConfig.Clique != nil:
		c := params.CliqueSnapshot
		c.DBPath = filepath.Join(datadir, "clique", "db")
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, c, config.Miner.Notify, config.Miner.Noverify, "", true, datadir, snapshots, true /* readonly */, db)
	case chainConfig.Aura != nil:
		consensusConfig := &params.AuRaConfig{DBPath: filepath.Join(datadir, "aura")}
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "", true, datadir, snapshots, true /* readonly */, db)
	case chainConfig.Parlia != nil:
		// Apply special hacks for BSC params
		params.ApplyBinanceSmartChainParams()
		consensusConfig := &params.ParliaConfig{DBPath: filepath.Join(datadir, "parlia")}
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "", true, datadir, snapshots, true /* readonly */, db)
	case chainConfig.Bor != nil:
		consensusConfig := &config.Bor
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "http://localhost:1317", false, datadir, snapshots, true /* readonly */, db)
	default: //ethash
		engine = ethash.NewFaker()
	}
	return
}
