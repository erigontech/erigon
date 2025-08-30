// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/debugprint"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	chain2 "github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/nodecfg"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/shards"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

var stateStages = &cobra.Command{
	Use: "state_stages",
	Short: `Run all StateStages (which happen after senders) in loop.
Examples: 
--unwind=1 --unwind.every=10  # 10 blocks forward, 1 block back, 10 blocks forward, ...
--unwind=10 --unwind.every=1  # 1 block forward, 10 blocks back, 1 blocks forward, ...
--unwind=10  # 10 blocks back, then stop
--integrity.fast=false --integrity.slow=false # Performs DB integrity checks each step. You can disable slow or fast checks.
--block # Stop at exact blocks
--chaindata.reference # When finish all cycles, does comparison to this db file.
		`,
	Example: "go run ./cmd/integration state_stages --datadir=... --verbosity=3 --unwind=100 --unwind.every=100000 --block=2000000",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		spec, err := chainspec.ChainSpecByName(chain)
		if err != nil {
			utils.Fatalf("unknown chain %s", chain)
		}
		ethConfig.Genesis = spec.Genesis
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)
		miningConfig := buildercfg.MiningConfig{}
		utils.SetupMinerCobra(cmd, &miningConfig)
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := syncBySmallSteps(db, miningConfig, ctx, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}

		if referenceChaindata != "" {
			if err := compareStates(ctx, chaindata, referenceChaindata); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Error(err.Error())
				}
				return
			}
		}
	},
}

var loopExecCmd = &cobra.Command{
	Use: "loop_exec",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()
		if unwind == 0 {
			unwind = 1
		}
		if err := loopExec(db, ctx, unwind, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func init() {
	withConfig(stateStages)
	withDataDir2(stateStages)
	withReferenceChaindata(stateStages)
	withUnwind(stateStages)
	withUnwindEvery(stateStages)
	withBlock(stateStages)
	withIntegrityChecks(stateStages)
	withMining(stateStages)
	withChain(stateStages)
	withHeimdall(stateStages)
	withWorkers(stateStages)
	withChaosMonkey(stateStages)
	rootCmd.AddCommand(stateStages)

	withConfig(loopExecCmd)
	withDataDir(loopExecCmd)
	withBatchSize(loopExecCmd)
	withUnwind(loopExecCmd)
	withChain(loopExecCmd)
	withHeimdall(loopExecCmd)
	withWorkers(loopExecCmd)
	withChaosMonkey(loopExecCmd)
	rootCmd.AddCommand(loopExecCmd)
}

func syncBySmallSteps(db kv.TemporalRwDB, miningConfig buildercfg.MiningConfig, ctx context.Context, logger1 log.Logger) error {
	dirs := datadir.New(datadirCli)
	if err := datadir.ApplyMigrations(dirs); err != nil {
		return err
	}

	_, engine, vmConfig, stateStages, miningStages, miner := newSync(ctx, db, &miningConfig, logger1)
	chainConfig, pm := fromdb.ChainConfig(db), fromdb.PruneMode(db)

	ttx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer ttx.Rollback()
	var tx kv.RwTx = ttx

	sd, err := state.NewSharedDomains(ttx, logger1)
	if err != nil {
		return err
	}
	defer sd.Close()

	quit := ctx.Done()

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	stateStages.DisableStages(stages.Snapshots, stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders)
	notifications := shards.NewNotifications(nil)

	spec, err := chainspec.ChainSpecByName(chain)
	if err != nil {
		return err
	}

	br, _ := blocksIO(db, logger1)
	execCfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, chainConfig, engine, vmConfig, notifications, false, true, dirs, br, nil, spec.Genesis, syncCfg, nil)

	execUntilFunc := func(execToBlock uint64) stagedsync.ExecFunc {
		return func(badBlockUnwind bool, s *stagedsync.StageState, unwinder stagedsync.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
			if err := stagedsync.SpawnExecuteBlocksStage(s, unwinder, txc, execToBlock, ctx, execCfg, logger); err != nil {
				return fmt.Errorf("spawnExecuteBlocksStage: %w", err)
			}
			return nil
		}
	}
	senderAtBlock := progress(tx, stages.Senders)
	execAtBlock := progress(tx, stages.Execution)

	var stopAt = senderAtBlock
	onlyOneUnwind := block == 0 && unwindEvery == 0 && unwind > 0
	backward := unwindEvery < unwind
	if onlyOneUnwind {
		stopAt = progress(tx, stages.Execution) - unwind
	} else if block > 0 && block < senderAtBlock {
		stopAt = block
	} else if backward {
		stopAt = 1
	}

	var structLogger *logger.StructLogger
	traceStart := func() {
		structLogger = logger.NewStructLogger(&logger.LogConfig{})
		vmConfig.Tracer = structLogger.Hooks()
	}
	traceStop := func(id int) {
		if vmConfig.Tracer == nil {
			return
		}
		w, err3 := os.Create(fmt.Sprintf("trace_%d.txt", id))
		if err3 != nil {
			panic(err3)
		}
		encoder := json.NewEncoder(w)
		encoder.SetIndent(" ", " ")
		for _, l := range logger.FormatLogs(structLogger.StructLogs()) {
			if err2 := encoder.Encode(l); err2 != nil {
				panic(err2)
			}
		}
		if err2 := w.Close(); err2 != nil {
			panic(err2)
		}

		vmConfig.Tracer = nil
	}
	_, _ = traceStart, traceStop

	for (!backward && execAtBlock < stopAt) || (backward && execAtBlock > stopAt) {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// All stages forward to `execStage + unwindEvery` block
		execAtBlock = progress(tx, stages.Execution)
		execToBlock := block
		if unwindEvery > 0 || unwind > 0 {
			if execAtBlock+unwindEvery > unwind {
				execToBlock = execAtBlock + unwindEvery - unwind
			} else {
				break
			}
		}
		if backward {
			if execToBlock < stopAt {
				execToBlock = stopAt
			}
		} else {
			if execToBlock > stopAt {
				execToBlock = stopAt + 1
				unwind = 0
			}
		}

		stateStages.MockExecFunc(stages.Execution, execUntilFunc(execToBlock))
		_ = stateStages.SetCurrentStage(stages.Execution)
		if _, err := stateStages.Run(db, wrap.NewTxContainer(tx, sd), false /* firstCycle */, false); err != nil {
			return err
		}

		//receiptsInDB := rawdb.ReadReceiptsByNumber(tx, progress(tx, stages.Execution)+1)

		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		execAtBlock = progress(tx, stages.Execution)

		if execAtBlock == stopAt {
			break
		}

		nextBlock, err := br.BlockByNumber(context.Background(), tx, execAtBlock+1)
		if err != nil {
			panic(err)
		}

		if miner.MiningConfig.Enabled && nextBlock != nil && nextBlock.Coinbase() != (common.Address{}) {
			miner.MiningConfig.Etherbase = nextBlock.Coinbase()
			miner.MiningConfig.ExtraData = nextBlock.Extra()
			miningStages.MockExecFunc(stages.MiningCreateBlock, func(badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				err = stagedsync.SpawnMiningCreateBlockStage(s, txc,
					stagedsync.StageMiningCreateBlockCfg(db, miner, chainConfig, engine, nil, dirs.Tmp, br),
					quit, logger)
				if err != nil {
					return err
				}
				miner.MiningBlock.Uncles = nextBlock.Uncles()
				miner.MiningBlock.Header.Time = nextBlock.Time()
				miner.MiningBlock.Header.GasLimit = nextBlock.GasLimit()
				miner.MiningBlock.Header.Difficulty = nextBlock.Difficulty()
				miner.MiningBlock.Header.Nonce = nextBlock.Nonce()
				miner.MiningBlock.PreparedTxns = nextBlock.Transactions()
				//debugprint.Headers(miningWorld.Block.Header, nextBlock.Header())
				return err
			})
			//miningStages.MockExecFunc(stages.MiningFinish, func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
			//debugprint.Transactions(nextBlock.Transactions(), miningWorld.Block.Txns)
			//debugprint.Receipts(miningWorld.Block.Receipts, receiptsInDB)
			//return stagedsync.SpawnMiningFinishStage(s, tx, miningWorld.Block, cc.Engine(), chainConfig, quit)
			//})

			_ = miningStages.SetCurrentStage(stages.MiningCreateBlock)
			if _, err := miningStages.Run(db, wrap.NewTxContainer(tx, sd), false /* firstCycle */, false); err != nil {
				return err
			}
			tx.Rollback()
			tx, err = db.BeginRw(context.Background())
			if err != nil {
				return err
			}
			defer tx.Rollback()
			minedBlock := <-miner.MiningResultCh
			checkMinedBlock(nextBlock, minedBlock.Block, chainConfig)
		}

		// Unwind all stages to `execStage - unwind` block
		if unwind == 0 {
			continue
		}

		to := execAtBlock - unwind
		if err := stateStages.UnwindTo(to, stagedsync.StagedUnwind, tx); err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// allow backward loop
		if unwind > 0 && unwindEvery > 0 {
			stopAt -= unwind
		}
	}

	return nil
}

func checkMinedBlock(b1, b2 *types.Block, chainConfig *chain2.Config) {
	if b1.Root() != b2.Root() ||
		(chainConfig.IsByzantium(b1.NumberU64()) && b1.ReceiptHash() != b2.ReceiptHash()) ||
		b1.TxHash() != b2.TxHash() ||
		b1.ParentHash() != b2.ParentHash() ||
		b1.UncleHash() != b2.UncleHash() ||
		b1.GasUsed() != b2.GasUsed() ||
		!bytes.Equal(b1.Extra(), b2.Extra()) { // TODO: Extra() doesn't need to be a copy for a read-only compare
		// Header()'s deep-copy doesn't matter here since it will panic anyway
		debugprint.Headers(b1.Header(), b2.Header())
		panic("blocks are not same")
	}
}

func loopExec(db kv.TemporalRwDB, ctx context.Context, unwind uint64, logger log.Logger) error {
	chainConfig := fromdb.ChainConfig(db)
	dirs, pm := datadir.New(datadirCli), fromdb.PruneMode(db)
	_, engine, vmConfig, sync, _, _ := newSync(ctx, db, nil, logger)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	must(tx.Commit())
	tx, err = db.BeginRw(ctx)
	must(err)
	defer tx.Rollback()
	sync.DisableAllStages()
	sync.EnableStages(stages.Execution)
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	from := progress(tx, stages.Execution)
	to := from + unwind

	spec, err := chainspec.ChainSpecByName(chain)
	if err != nil {
		return fmt.Errorf("unknown chain %s", chain)
	}

	initialCycle := false
	br, _ := blocksIO(db, logger)
	notifications := shards.NewNotifications(nil)
	cfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, chainConfig, engine, vmConfig, notifications, false, true, dirs, br, nil, spec.Genesis, syncCfg, nil)

	// set block limit of execute stage
	sync.MockExecFunc(stages.Execution, func(badBlockUnwind bool, stageState *stagedsync.StageState, unwinder stagedsync.Unwinder, txc wrap.TxContainer, logger log.Logger) error {
		if err = stagedsync.SpawnExecuteBlocksStage(stageState, sync, txc, to, ctx, cfg, logger); err != nil {
			return fmt.Errorf("spawnExecuteBlocksStage: %w", err)
		}
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_ = sync.SetCurrentStage(stages.Execution)
		t := time.Now()
		if _, err = sync.Run(db, wrap.NewTxContainer(tx, nil), initialCycle, false); err != nil {
			return err
		}
		logger.Info("[Integration] ", "loop time", time.Since(t))
		tx.Rollback()
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
}
