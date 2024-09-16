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

package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type CustomTraceCfg struct {
	tmpdir   string
	db       kv.RwDB
	prune    prune.Mode
	execArgs *exec3.ExecArgs
}

func StageCustomTraceCfg(db kv.RwDB, prune prune.Mode, dirs datadir.Dirs, br services.FullBlockReader, cc *chain.Config,
	engine consensus.Engine, genesis *types.Genesis, syncCfg *ethconfig.Sync) CustomTraceCfg {
	execArgs := &exec3.ExecArgs{
		ChainDB:     db,
		BlockReader: br,
		Prune:       prune,
		ChainConfig: cc,
		Dirs:        dirs,
		Engine:      engine,
		Genesis:     genesis,
		Workers:     syncCfg.ExecWorkerCount,
	}
	return CustomTraceCfg{
		db:       db,
		prune:    prune,
		execArgs: execArgs,
	}
}

func SpawnCustomTrace(cfg CustomTraceCfg, ctx context.Context, logger log.Logger) error {
	var endBlock uint64
	if err := cfg.db.View(ctx, func(tx kv.Tx) (err error) {
		endBlock, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return fmt.Errorf("getting last executed block: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	for startBlock := uint64(0); startBlock < endBlock; startBlock += 1000 {
		if err := customTraceBatchProduce(ctx, cfg.execArgs, cfg.db, startBlock, startBlock+1000, "custom_trace", logger); err != nil {
			return err
		}
	}

	return nil
}

func customTraceBatchProduce(ctx context.Context, cfg *exec3.ExecArgs, db kv.RwDB, fromBlock, toBlock uint64, logPrefix string, logger log.Logger) error {
	var lastTxNum uint64
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		ttx := tx.(kv.TemporalRwTx)
		doms, err := state2.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer doms.Close()

		if err := customTraceBatch(ctx, cfg, ttx, doms, fromBlock, toBlock, logPrefix, logger); err != nil {
			return err
		}
		doms.SetTx(tx)
		if err := doms.Flush(ctx, tx); err != nil {
			return err
		}
		lastTxNum = doms.TxNum()
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	agg := db.(state2.HasAgg).Agg().(*state2.Aggregator)
	var fromStep uint64
	toStep := (lastTxNum / agg.StepSize()) - 1
	if err := db.View(ctx, func(tx kv.Tx) error {
		ac := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
		fromStep = ac.Appendable(kv.ReceiptsAppendable).FirstStepNotInFiles()
		return nil
	}); err != nil {
		return err
	}
	if err := agg.BuildFiles2(ctx, fromStep, toStep); err != nil {
		return err
	}
	if err := agg.MergeLoop(ctx); err != nil {
		return err
	}
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		ac := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
		if _, err := ac.PruneSmallBatches(ctx, 10*time.Hour, tx); err != nil { // prune part of retired data, before commit
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func customTraceBatch(ctx context.Context, cfg *exec3.ExecArgs, tx kv.TemporalRwTx, doms *state2.SharedDomains, fromBlock, toBlock uint64, logPrefix string, logger log.Logger) error {
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	var cumulative uint64
	var lastBlockNum uint64

	canonicalReader := rawdb.NewCanonicalReader(rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.BlockReader)))
	lastFrozenID, err := canonicalReader.LastFrozenTxNum(tx)
	if err != nil {
		return err
	}

	var baseBlockTxnID, txnID kv.TxnId
	//TODO: new tracer may get tracer from pool, maybe add it to TxTask field
	/// maybe need startTxNum/endTxNum
	var prevTxNumLog = fromBlock
	var m runtime.MemStats
	if err = exec3.CustomTraceMapReduce(fromBlock, toBlock, exec3.TraceConsumer{
		NewTracer: func() exec3.GenericTracer { return nil },
		Reduce: func(txTask *state.TxTask, tx kv.Tx) error {
			if txTask.Error != nil {
				return err
			}

			if lastBlockNum != txTask.BlockNum {
				cumulative = 0
				lastBlockNum = txTask.BlockNum

				if txTask.TxNum < uint64(lastFrozenID) {
					txnID = kv.TxnId(txTask.TxNum)
				} else {
					h, err := rawdb.ReadCanonicalHash(tx, txTask.BlockNum)
					baseBlockTxnID, err = canonicalReader.BaseTxnID(tx, txTask.BlockNum, h)
					if err != nil {
						return err
					}
					txnID = baseBlockTxnID
				}
			} else {
				txnID++
			}
			cumulative += txTask.UsedGas
			doms.SetTxNum(txTask.TxNum)

			select {
			case <-logEvery.C:
				dbg.ReadMemStats(&m)
				log.Info(fmt.Sprintf("[%s] Scanned", logPrefix), "block", txTask.BlockNum, "txs/sec", txTask.TxNum-prevTxNumLog, "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
				prevTxNumLog = txTask.TxNum
			default:
			}

			if txTask.Final || txTask.TxIndex < 0 {
				r := txTask.CreateReceipt(cumulative)
				if err := rawtemporaldb.AppendReceipts(doms, txnID, r); err != nil {
					return err
				}
				return nil
			}

			r := txTask.CreateReceipt(cumulative)
			if err := rawtemporaldb.AppendReceipts(doms, txnID, r); err != nil {
				return err
			}
			return nil
		},
	}, ctx, tx, cfg, logger); err != nil {
		return err
	}

	return nil
}
