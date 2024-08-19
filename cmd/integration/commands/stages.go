package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	"github.com/ledgerwatch/secp256k1"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/gateway-fm/cdk-erigon-lib/commitment"
	common2 "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/cmp"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/common/dir"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcfg"
	"github.com/gateway-fm/cdk-erigon-lib/kv/rawdbv3"
	libstate "github.com/gateway-fm/cdk-erigon-lib/state"
	chain2 "github.com/ledgerwatch/erigon/chain"

	chain3 "github.com/gateway-fm/cdk-erigon-lib/chain"
	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	reset2 "github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
)

var cmdStageSnapshots = &cobra.Command{
	Use:   "stage_snapshots",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageSnapshots(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageHeaders = &cobra.Command{
	Use:   "stage_headers",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageHeaders(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageBodies = &cobra.Command{
	Use:   "stage_bodies",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageBodies(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageSenders = &cobra.Command{
	Use:   "stage_senders",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageSenders(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageExec = &cobra.Command{
	Use:   "stage_exec",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		defer func(t time.Time) { log.Info("total", "took", time.Since(t)) }(time.Now())

		if err := stageExec(db, cmd.Context()); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageTrie = &cobra.Command{
	Use:   "stage_trie",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageTrie(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageHashState = &cobra.Command{
	Use:   "stage_hash_state",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageHashState(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageHistory = &cobra.Command{
	Use:   "stage_history",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageHistory(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdLogIndex = &cobra.Command{
	Use:   "stage_log_index",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageLogIndex(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdCallTraces = &cobra.Command{
	Use:   "stage_call_traces",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageCallTraces(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageTxLookup = &cobra.Command{
	Use:   "stage_tx_lookup",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()

		if err := stageTxLookup(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}
var cmdPrintStages = &cobra.Command{
	Use:   "print_stages",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata).Readonly(), false)
		defer db.Close()

		if err := printAllStages(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdPrintMigrations = &cobra.Command{
	Use:   "print_migrations",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), false)
		defer db.Close()
		if err := printAppliedMigrations(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdRemoveMigration = &cobra.Command{
	Use:   "remove_migration",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		db := openDB(dbCfg(kv.ChainDB, chaindata), false)
		defer db.Close()
		if err := removeMigration(db, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdRunMigrations = &cobra.Command{
	Use:   "run_migrations",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
	},
}

var cmdSetPrune = &cobra.Command{
	Use:   "force_set_prune",
	Short: "Override existing --prune flag value (if you know what you are doing)",
	Run: func(cmd *cobra.Command, args []string) {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		if err := overrideStorageMode(db); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdSetSnap = &cobra.Command{
	Use:   "force_set_snapshot",
	Short: "Override existing --snapshots flag value (if you know what you are doing)",
	Run: func(cmd *cobra.Command, args []string) {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		sn, agg := allSnapshots(cmd.Context(), db)
		defer sn.Close()
		defer agg.Close()

		if err := db.Update(context.Background(), func(tx kv.RwTx) error {
			return snap.ForceSetFlags(tx, sn.Cfg())
		}); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

var cmdForceSetHistoryV3 = &cobra.Command{
	Use:   "force_set_history_v3",
	Short: "Override existing --history.v3 flag value (if you know what you are doing)",
	Run: func(cmd *cobra.Command, args []string) {
		db := openDB(dbCfg(kv.ChainDB, chaindata), true)
		defer db.Close()
		if err := db.Update(context.Background(), func(tx kv.RwTx) error {
			return kvcfg.HistoryV3.ForceWrite(tx, _forceSetHistoryV3)
		}); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

func init() {
	withConfig(cmdPrintStages)
	withDataDir(cmdPrintStages)
	withChain(cmdPrintStages)
	withHeimdall(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	withConfig(cmdStageSenders)
	withIntegrityChecks(cmdStageSenders)
	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDataDir(cmdStageSenders)
	withChain(cmdStageSenders)
	withHeimdall(cmdStageSenders)
	rootCmd.AddCommand(cmdStageSenders)

	withConfig(cmdStageSnapshots)
	withDataDir(cmdStageSnapshots)
	withReset(cmdStageSnapshots)
	rootCmd.AddCommand(cmdStageSnapshots)

	withConfig(cmdStageHeaders)
	withDataDir(cmdStageHeaders)
	withUnwind(cmdStageHeaders)
	withReset(cmdStageHeaders)
	withChain(cmdStageHeaders)
	withHeimdall(cmdStageHeaders)
	rootCmd.AddCommand(cmdStageHeaders)

	withConfig(cmdStageBodies)
	withDataDir(cmdStageBodies)
	withUnwind(cmdStageBodies)
	withChain(cmdStageBodies)
	withHeimdall(cmdStageBodies)
	rootCmd.AddCommand(cmdStageBodies)

	withConfig(cmdStageExec)
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

	withConfig(cmdStageHashState)
	withDataDir(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withPruneTo(cmdStageHashState)
	withBatchSize(cmdStageHashState)
	withChain(cmdStageHashState)
	withHeimdall(cmdStageHashState)
	rootCmd.AddCommand(cmdStageHashState)

	withConfig(cmdStageTrie)
	withDataDir(cmdStageTrie)
	withReset(cmdStageTrie)
	withBlock(cmdStageTrie)
	withUnwind(cmdStageTrie)
	withPruneTo(cmdStageTrie)
	withIntegrityChecks(cmdStageTrie)
	withChain(cmdStageTrie)
	withHeimdall(cmdStageTrie)
	rootCmd.AddCommand(cmdStageTrie)

	withConfig(cmdStageHistory)
	withDataDir(cmdStageHistory)
	withReset(cmdStageHistory)
	withBlock(cmdStageHistory)
	withUnwind(cmdStageHistory)
	withPruneTo(cmdStageHistory)
	withChain(cmdStageHistory)
	withHeimdall(cmdStageHistory)
	rootCmd.AddCommand(cmdStageHistory)

	withConfig(cmdLogIndex)
	withDataDir(cmdLogIndex)
	withReset(cmdLogIndex)
	withBlock(cmdLogIndex)
	withUnwind(cmdLogIndex)
	withPruneTo(cmdLogIndex)
	withChain(cmdLogIndex)
	withHeimdall(cmdLogIndex)
	rootCmd.AddCommand(cmdLogIndex)

	withConfig(cmdCallTraces)
	withDataDir(cmdCallTraces)
	withReset(cmdCallTraces)
	withBlock(cmdCallTraces)
	withUnwind(cmdCallTraces)
	withPruneTo(cmdCallTraces)
	withChain(cmdCallTraces)
	withHeimdall(cmdCallTraces)
	rootCmd.AddCommand(cmdCallTraces)

	withConfig(cmdStageTxLookup)
	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDataDir(cmdStageTxLookup)
	withPruneTo(cmdStageTxLookup)
	withChain(cmdStageTxLookup)
	withHeimdall(cmdStageTxLookup)
	rootCmd.AddCommand(cmdStageTxLookup)

	withConfig(cmdPrintMigrations)
	withDataDir(cmdPrintMigrations)
	rootCmd.AddCommand(cmdPrintMigrations)

	withConfig(cmdRemoveMigration)
	withDataDir(cmdRemoveMigration)
	withMigration(cmdRemoveMigration)
	withChain(cmdRemoveMigration)
	withHeimdall(cmdRemoveMigration)
	rootCmd.AddCommand(cmdRemoveMigration)

	withConfig(cmdRunMigrations)
	withDataDir(cmdRunMigrations)
	withChain(cmdRunMigrations)
	withHeimdall(cmdRunMigrations)
	rootCmd.AddCommand(cmdRunMigrations)

	withConfig(cmdSetSnap)
	withDataDir2(cmdSetSnap)
	withChain(cmdSetSnap)
	rootCmd.AddCommand(cmdSetSnap)

	withConfig(cmdForceSetHistoryV3)
	withDataDir2(cmdForceSetHistoryV3)
	cmdForceSetHistoryV3.Flags().BoolVar(&_forceSetHistoryV3, "history.v3", false, "")
	rootCmd.AddCommand(cmdForceSetHistoryV3)

	withConfig(cmdSetPrune)
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
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()
	br := getBlockReader(db)
	engine, _, _, _, _ := newSync(ctx, db, nil)
	chainConfig, _, _ := fromdb.ChainConfig(db), kvcfg.HistoryV3.FromDB(db), fromdb.PruneMode(db)

	return db.Update(ctx, func(tx kv.RwTx) error {
		if !(unwind > 0 || reset) {
			log.Info("This command only works with --unwind or --reset options")
		}

		if reset {
			dirs := datadir.New(datadirCli)
			if err := reset2.ResetBlocks(tx, db, sn, agg, br, dirs, *chainConfig, engine); err != nil {
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
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()
	chainConfig, historyV3, transactionsV3 := fromdb.ChainConfig(db), kvcfg.HistoryV3.FromDB(db), kvcfg.TransactionsV3.FromDB(db)
	_, _, sync, _, _ := newSync(ctx, db, nil)

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		s := stage(sync, tx, nil, stages.Bodies)

		if unwind > 0 {
			if unwind > s.BlockNumber {
				return fmt.Errorf("cannot unwind past 0")
			}

			u := sync.NewUnwindState(stages.Bodies, s.BlockNumber-unwind, s.BlockNumber)
			if err := stagedsync.UnwindBodiesStage(u, tx, stagedsync.StageBodiesCfg(db, nil, nil, nil, nil, 0, *chainConfig, sn, getBlockReader(db), historyV3, transactionsV3), ctx); err != nil {
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
	tmpdir := datadir.New(datadirCli).Tmp
	chainConfig := fromdb.ChainConfig(db)
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()
	_, _, sync, _, _ := newSync(ctx, db, nil)

	must(sync.SetCurrentStage(stages.Senders))

	if reset {
		return db.Update(ctx, func(tx kv.RwTx) error { return reset2.ResetSenders(ctx, db, tx) })
	}

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

	s := stage(sync, tx, nil, stages.Senders)
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	var br *snapshotsync.BlockRetire
	if sn.Cfg().Enabled {
		br = snapshotsync.NewBlockRetire(estimate.CompressSnapshot.Workers(), tmpdir, sn, db, nil, nil)
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
		if err = stagedsync.SpawnRecoverSendersStage(cfg, s, sync, tx, block, ctx, false /* quiet */); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageExec(db kv.RwDB, ctx context.Context) error {
	chainConfig, historyV3, pm := fromdb.ChainConfig(db), kvcfg.HistoryV3.FromDB(db), fromdb.PruneMode(db)
	dirs := datadir.New(datadirCli)
	engine, vmConfig, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.Execution))
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()

	if warmup {
		return reset2.WarmupExec(ctx, db)
	}
	if reset {
		return reset2.ResetExec(ctx, db, chain, "")
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

	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = int(workers)
	syncCfg.ReconWorkerCount = int(reconWorkers)

	genesis := core.GenesisBlockByChainName(chain)
	cfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil,
		/*stateStream=*/ false,
		/*badBlockHalt=*/ false, historyV3, dirs, getBlockReader(db), nil, genesis, syncCfg, agg, nil)
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

	err := stagedsync.SpawnExecuteBlocksStage(s, sync, nil, block, ctx, cfg, true /* initialCycle */, false /* quiet */)
	if err != nil {
		return err
	}
	return nil
}

func stageTrie(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV3 := datadir.New(datadirCli), fromdb.PruneMode(db), kvcfg.HistoryV3.FromDB(db)
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.IntermediateHashes))

	if warmup {
		return reset2.Warmup(ctx, db, log.LvlInfo, stages.IntermediateHashes)
	}
	if reset {
		return reset2.Reset(ctx, db, stages.IntermediateHashes)
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
	cfg := stagedsync.StageTrieCfg(db, true /* checkRoot */, true /* saveHashesToDb */, false /* badBlockHalt */, dirs.Tmp, getBlockReader(db), nil /* hd */, historyV3, agg)
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
		if _, err := stagedsync.SpawnIntermediateHashesStage(s, sync /* Unwinder */, tx, cfg, ctx, false /* quiet */); err != nil {
			return err
		}
	}
	integrity.Trie(db, tx, integritySlow, ctx)
	return tx.Commit()
}

func stageHashState(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV3 := datadir.New(datadirCli), fromdb.PruneMode(db), kvcfg.HistoryV3.FromDB(db)
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.HashState))

	if warmup {
		return reset2.Warmup(ctx, db, log.LvlInfo, stages.HashState)
	}
	if reset {
		return reset2.Reset(ctx, db, stages.HashState)
	}

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	s := stage(sync, tx, nil, stages.HashState)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	cfg := stagedsync.StageHashStateCfg(db, dirs, historyV3, agg)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.HashState, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindHashStateStage(u, s, tx, cfg, ctx, false)
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
		err = stagedsync.SpawnHashStateStage(s, tx, cfg, ctx, false /* quiet */)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageLogIndex(db kv.RwDB, ctx context.Context) error {
	dirs, pm, historyV3 := datadir.New(datadirCli), fromdb.PruneMode(db), kvcfg.HistoryV3.FromDB(db)
	if historyV3 {
		return fmt.Errorf("this stage is disable in --history.v3=true")
	}
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.LogIndex))
	if warmup {
		return reset2.Warmup(ctx, db, log.LvlInfo, stages.LogIndex)
	}
	if reset {
		return reset2.Reset(ctx, db, stages.LogIndex)
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
	dirs, pm, historyV3 := datadir.New(datadirCli), fromdb.PruneMode(db), kvcfg.HistoryV3.FromDB(db)
	if historyV3 {
		return fmt.Errorf("this stage is disable in --history.v3=true")
	}
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.CallTraces))

	if warmup {
		return reset2.Warmup(ctx, db, log.LvlInfo, stages.CallTraces)
	}
	if reset {
		return reset2.Reset(ctx, db, stages.CallTraces)
	}

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
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
	dirs, pm, historyV3 := datadir.New(datadirCli), fromdb.PruneMode(db), kvcfg.HistoryV3.FromDB(db)
	if historyV3 {
		return fmt.Errorf("this stage is disable in --history.v3=true")
	}
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()
	_, _, sync, _, _ := newSync(ctx, db, nil)
	must(sync.SetCurrentStage(stages.AccountHistoryIndex))

	if warmup {
		return reset2.Warmup(ctx, db, log.LvlInfo, stages.AccountHistoryIndex, stages.StorageHistoryIndex)
	}
	if reset {
		return reset2.Reset(ctx, db, stages.AccountHistoryIndex, stages.StorageHistoryIndex)
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
		_ = printStages(tx, sn, agg)
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
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()

	if reset {
		return db.Update(ctx, func(tx kv.RwTx) error { return reset2.ResetTxLookup(tx) })
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	s := stage(sync, tx, nil, stages.TxLookup)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	cfg := stagedsync.StageTxLookupCfg(db, pm, dirs.Tmp, sn, chainConfig.Bor)
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
		err = stagedsync.PruneTxLookup(p, tx, cfg, ctx, true)
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
	sn, agg := allSnapshots(ctx, db)
	defer sn.Close()
	defer agg.Close()
	return db.View(ctx, func(tx kv.Tx) error { return printStages(tx, sn, agg) })
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
var _aggSingleton *libstate.AggregatorV3

func allSnapshots(ctx context.Context, db kv.RoDB) (*snapshotsync.RoSnapshots, *libstate.AggregatorV3) {
	openSnapshotOnce.Do(func() {
		var useSnapshots bool
		_ = db.View(context.Background(), func(tx kv.Tx) error {
			useSnapshots, _ = snap.Enabled(tx)
			return nil
		})
		dirs := datadir.New(datadirCli)
		dir.MustExist(dirs.SnapHistory)

		snapCfg := ethconfig.NewSnapCfg(useSnapshots, true, true)
		_allSnapshotsSingleton = snapshotsync.NewRoSnapshots(snapCfg, dirs.Snap)

		var err error
		_aggSingleton, err = libstate.NewAggregatorV3(ctx, dirs.SnapHistory, dirs.Tmp, ethconfig.HistoryV3AggregationStep, db)
		if err != nil {
			panic(err)
		}
		err = _aggSingleton.OpenFolder()
		if err != nil {
			panic(err)
		}

		if useSnapshots {
			if err := _allSnapshotsSingleton.ReopenFolder(); err != nil {
				panic(err)
			}
			_allSnapshotsSingleton.LogStat()
			db.View(context.Background(), func(tx kv.Tx) error {
				_aggSingleton.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
					_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
					return histBlockNumProgress
				})
				return nil
			})
		}
	})
	return _allSnapshotsSingleton, _aggSingleton
}

var openBlockReaderOnce sync.Once
var _blockReaderSingleton services.FullBlockReader

func getBlockReader(db kv.RoDB) (blockReader services.FullBlockReader) {
	openBlockReaderOnce.Do(func() {
		sn, _ := allSnapshots(context.Background(), db)
		transactionsV3 := kvcfg.TransactionsV3.FromDB(db)
		_blockReaderSingleton = snapshotsync.NewBlockReaderWithSnapshots(sn, transactionsV3)
	})
	return _blockReaderSingleton
}

var openDomainsOnce sync.Once
var _aggDomainSingleton *libstate.Aggregator

func allDomains(ctx context.Context, db kv.RoDB, stepSize uint64, mode libstate.CommitmentMode, trie commitment.TrieVariant) (*snapshotsync.RoSnapshots, *libstate.Aggregator) {
	openDomainsOnce.Do(func() {
		var useSnapshots bool
		_ = db.View(context.Background(), func(tx kv.Tx) error {
			useSnapshots, _ = snap.Enabled(tx)
			return nil
		})
		dirs := datadir.New(datadirCli)
		dir.MustExist(dirs.SnapHistory)

		snapCfg := ethconfig.NewSnapCfg(useSnapshots, true, true)
		_allSnapshotsSingleton = snapshotsync.NewRoSnapshots(snapCfg, dirs.Snap)

		var err error
		_aggDomainSingleton, err = libstate.NewAggregator(filepath.Join(dirs.DataDir, "state"), dirs.Tmp, stepSize, mode, trie)
		if err != nil {
			panic(err)
		}
		if err = _aggDomainSingleton.ReopenFolder(); err != nil {
			panic(err)
		}

		if useSnapshots {
			if err := _allSnapshotsSingleton.ReopenFolder(); err != nil {
				panic(err)
			}
			_allSnapshotsSingleton.LogStat()
			//db.View(context.Background(), func(tx kv.Tx) error {
			//	_aggSingleton.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
			//		_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
			//		return histBlockNumProgress
			//	})
			//	return nil
			//})
		}
	})
	return _allSnapshotsSingleton, _aggDomainSingleton
}

func newDomains(ctx context.Context, db kv.RwDB, stepSize uint64, mode libstate.CommitmentMode, trie commitment.TrieVariant) (consensus.Engine, ethconfig.Config, *snapshotsync.RoSnapshots, *libstate.Aggregator) {
	historyV3, pm := kvcfg.HistoryV3.FromDB(db), fromdb.PruneMode(db)
	//events := shards.NewEvents()
	genesis := core.GenesisBlockByChainName(chain)

	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis, "")
	_ = genesisBlock // TODO apply if needed here

	if _, ok := genesisErr.(*chain3.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	//log.Info("Initialised chain configuration", "config", chainConfig)

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	cfg := ethconfig.Defaults
	cfg.HistoryV3 = historyV3
	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.DeprecatedTxPool.Disable = true
	cfg.Genesis = core.GenesisBlockByChainName(chain)
	//if miningConfig != nil {
	//	cfg.Miner = *miningConfig
	//}
	cfg.Dirs = datadir.New(datadirCli)

	allSn, agg := allDomains(ctx, db, stepSize, mode, trie)
	cfg.Snapshot = allSn.Cfg()

	engine := initConsensusEngine(chainConfig, cfg.Dirs.DataDir, db)
	return engine, cfg, allSn, agg
}

func newSync(ctx context.Context, db kv.RwDB, miningConfig *params.MiningConfig) (consensus.Engine, *vm.Config, *stagedsync.Sync, *stagedsync.Sync, stagedsync.MiningState) {
	dirs, historyV3, pm := datadir.New(datadirCli), kvcfg.HistoryV3.FromDB(db), fromdb.PruneMode(db)

	vmConfig := &vm.Config{}

	events := shards.NewEvents()

	genesis := core.GenesisBlockByChainName(chain)
	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis, "")
	if _, ok := genesisErr.(*chain3.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	//log.Info("Initialised chain configuration", "config", chainConfig)

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	cfg := ethconfig.Defaults
	cfg.HistoryV3 = historyV3
	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.DeprecatedTxPool.Disable = true
	cfg.Genesis = core.GenesisBlockByChainName(chain)
	if miningConfig != nil {
		cfg.Miner = *miningConfig
	}
	cfg.Dirs = datadir.New(datadirCli)
	allSn, agg := allSnapshots(ctx, db)
	cfg.Snapshot = allSn.Cfg()

	engine := initConsensusEngine(chainConfig, cfg.Dirs.DataDir, db)

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
		ethconfig.Defaults.DropUselessPeers,
	)
	if err != nil {
		panic(err)
	}

	stages := stages2.NewDefaultStages(context.Background(), db, p2p.Config{}, &cfg, sentryControlServer, &shards.Notifications{}, nil, allSn, agg, nil, engine)
	sync := stagedsync.New(stages, stagedsync.DefaultUnwindOrder, stagedsync.DefaultPruneOrder)

	miner := stagedsync.NewMiningState(&cfg.Miner)
	miningCancel := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(miningCancel)
	}()
	miningSync := stagedsync.New(
		stagedsync.MiningStages(ctx,
			stagedsync.StageMiningCreateBlockCfg(db, miner, *chainConfig, engine, nil, nil, nil, dirs.Tmp),
			stagedsync.StageMiningExecCfg(db, miner, events, *chainConfig, engine, &vm.Config{}, dirs.Tmp, nil, 0, nil, nil, allSn, cfg.TransactionsV3),
			stagedsync.StageHashStateCfg(db, dirs, historyV3, agg),
			stagedsync.StageTrieCfg(db, false, true, false, dirs.Tmp, br, nil, historyV3, agg),
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

func initConsensusEngine(cc *chain2.Config, datadir string, db kv.RwDB) (engine consensus.Engine) {
	snapshots, _ := allSnapshots(context.Background(), db)
	config := ethconfig.Defaults

	var consensusConfig interface{}

	if cc.Clique != nil {
		consensusConfig = params.CliqueSnapshot
	} else if cc.Aura != nil {
		consensusConfig = &config.Aura
	} else if cc.Bor != nil {
		consensusConfig = &config.Bor
	} else {
		consensusConfig = &config.Ethash
	}
	return ethconsensusconfig.CreateConsensusEngine(cc, consensusConfig, config.Miner.Notify, config.Miner.Noverify, HeimdallgRPCAddress, HeimdallURL, config.WithoutHeimdall, datadir, snapshots, db.ReadOnly(), db)
}
