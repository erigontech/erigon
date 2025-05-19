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
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"time"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type CustomTraceCfg struct {
	tmpdir   string
	db       kv.TemporalRwDB
	ExecArgs *exec3.ExecArgs

	Produce Produce
}
type Produce struct {
	ReceiptDomain bool
	RCacheDomain  bool
	LogAddr       bool
	LogTopic      bool
	TraceFrom     bool
	TraceTo       bool
}

func NewProduce(produceList []string) Produce {
	var produce Produce
	for _, p := range produceList {
		p = strings.TrimSpace(p)
		switch p {
		case kv.ReceiptDomain.String():
			produce.ReceiptDomain = true
		case kv.RCacheDomain.String():
			produce.RCacheDomain = true
		case kv.LogAddrIdx.String():
			produce.LogAddr = true
		case kv.LogTopicIdx.String():
			produce.LogTopic = true
		case kv.TracesFromIdx.String():
			produce.TraceFrom = true
		case kv.TracesToIdx.String():
			produce.TraceTo = true
		default:
			panic(fmt.Errorf("assert: unknown Produce %#v", p))
		}
	}
	return produce
}

func StageCustomTraceCfg(produce []string, db kv.TemporalRwDB, dirs datadir.Dirs, br services.FullBlockReader, cc *chain.Config, engine consensus.Engine, genesis *types.Genesis, syncCfg *ethconfig.Sync) CustomTraceCfg {
	execArgs := &exec3.ExecArgs{
		ChainDB:     db,
		BlockReader: br,
		ChainConfig: cc,
		Dirs:        dirs,
		Engine:      engine,
		Genesis:     genesis,
		Workers:     syncCfg.ExecWorkerCount,
	}
	return CustomTraceCfg{
		db:       db,
		ExecArgs: execArgs,
		Produce:  NewProduce(produce),
	}
}

func SpawnCustomTrace(cfg CustomTraceCfg, ctx context.Context, logger log.Logger) error {
	log.Info("[stage_custom_trace] start params", "produce", cfg.Produce)
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.ExecArgs.BlockReader))

	// 1. Require stage_exec > 0: means don't need handle "half-block execution case here"
	// 2. Require stage_exec > 0: means has enough state-history
	var execProgress uint64
	var startBlock, endBlock uint64
	if err := cfg.db.View(ctx, func(tx kv.Tx) (err error) {
		execProgress, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		if execProgress == 0 {
			return errors.New("stage_exec progress is 0. please run `integration stage_exec --batchSize=1m` for couple minutes")
		}

		ac := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)

		//TODO: need better way to detect start point. What if domain/index is sparse (has rare events).
		txNum := uint64(math.MaxUint64)
		if cfg.Produce.ReceiptDomain {
			txNum = min(txNum, ac.HistoryProgress(kv.ReceiptDomain, tx))
		}
		if cfg.Produce.RCacheDomain {
			txNum = min(txNum, ac.HistoryProgress(kv.RCacheDomain, tx))
		}
		if cfg.Produce.LogAddr {
			txNum = min(txNum, ac.ProgressII(kv.LogAddrIdx, tx))
		}
		if cfg.Produce.LogTopic {
			txNum = min(txNum, ac.ProgressII(kv.LogTopicIdx, tx))
		}
		if cfg.Produce.TraceFrom {
			txNum = min(txNum, ac.ProgressII(kv.TracesFromIdx, tx))
		}
		if cfg.Produce.TraceTo {
			txNum = min(txNum, ac.ProgressII(kv.TracesToIdx, tx))
		}
		fromTxNum := txNum
		var ok bool
		ok, startBlock, err = txNumsReader.FindBlockNum(tx, fromTxNum)
		if err != nil {
			return fmt.Errorf("getting last executed block: %w", err)
		}
		if !ok {
			panic(ok)
		}
		return nil
	}); err != nil {
		return err
	}
	endBlock = execProgress

	log.Info("SpawnCustomTrace", "startBlock", startBlock, "endBlock", endBlock)

	var producingDomain kv.Domain
	if cfg.Produce.ReceiptDomain {
		producingDomain = kv.ReceiptDomain
	}
	if cfg.Produce.RCacheDomain {
		producingDomain = kv.RCacheDomain
	}

	batchSize := uint64(50_000)
	for ; startBlock < endBlock; startBlock += batchSize {
		to := min(endBlock+1, startBlock+batchSize)
		if err := customTraceBatchProduce(ctx, cfg.Produce, cfg.ExecArgs, cfg.db, startBlock, to, "custom_trace", producingDomain, logger); err != nil {
			return err
		}
	}

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	chkEvery := time.NewTicker(3 * time.Second)
	defer chkEvery.Stop()

Loop:
	for {
		select {
		case <-cfg.db.(state2.HasAgg).Agg().(*state2.Aggregator).WaitForBuildAndMerge():
			break Loop
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			//TODO: log progress and list of domains/files
			logger.Info("[snapshots] Building files", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		}
	}

	log.Info("SpawnCustomTrace finish")
	if err := cfg.db.View(ctx, func(tx kv.Tx) error {
		ac := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
		receiptProgress := ac.HistoryProgress(producingDomain, tx)
		accProgress := ac.HistoryProgress(kv.AccountsDomain, tx)
		if accProgress != receiptProgress {
			_, e1, _ := txNumsReader.FindBlockNum(tx, receiptProgress)
			_, e2, _ := txNumsReader.FindBlockNum(tx, accProgress)

			err := fmt.Errorf("[integrity] %s=%d (%d) is behind AccountDomain=%d(%d)", producingDomain.String(), receiptProgress, e1, accProgress, e2)
			log.Warn(err.Error())
			return nil
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func customTraceBatchProduce(ctx context.Context, produce Produce, cfg *exec3.ExecArgs, db kv.TemporalRwDB, fromBlock, toBlock uint64, logPrefix string, producingDomain kv.Domain, logger log.Logger) error {
	var lastTxNum uint64
	{
		tx, err := db.BeginTemporalRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		doms, err := state2.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer doms.Close()

		if err := customTraceBatch(ctx, produce, cfg, tx, doms, fromBlock, toBlock, logPrefix, logger); err != nil {
			return err
		}

		doms.SetTx(tx)
		if err := doms.Flush(ctx, tx); err != nil {
			return err
		}

		//asserts
		if produce.ReceiptDomain {
			if err = AssertReceipts(ctx, cfg, tx, fromBlock, toBlock); err != nil {
				return err
			}
		}

		lastTxNum = doms.TxNum()
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	agg := db.(state2.HasAgg).Agg().(*state2.Aggregator)
	var fromStep, toStep uint64
	if lastTxNum/agg.StepSize() > 0 {
		toStep = lastTxNum / agg.StepSize()
	}
	if err := db.View(ctx, func(tx kv.Tx) error {
		ac := tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx)
		fromStep = ac.DbgDomain(producingDomain).FirstStepNotInFiles()
		return nil
	}); err != nil {
		return err
	}
	if err := agg.BuildFiles2(ctx, fromStep, toStep); err != nil {
		return err
	}

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := tx.(kv.TemporalRwTx).Debug().GreedyPruneHistory(ctx, kv.CommitmentDomain); err != nil {
			return err
		}
		if _, err := tx.(kv.TemporalRwTx).Debug().PruneSmallBatches(ctx, 10*time.Hour); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func AssertReceipts(ctx context.Context, cfg *exec3.ExecArgs, tx kv.TemporalRwTx, fromBlock, toBlock uint64) (err error) {
	if !dbg.AssertEnabled {
		return nil
	}
	if cfg.ChainConfig.Bor != nil { //TODO: enable me
		return nil
	}

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.BlockReader))
	fromTxNum, err := txNumsReader.Min(tx, fromBlock)
	if err != nil {
		return err
	}
	if fromTxNum < 2 {
		fromTxNum = 2 //i don't remember why need this
	}

	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}
	toTxNum, err := txNumsReader.Max(tx, toBlock)
	if err != nil {
		return err
	}
	prevCumGasUsed := -1
	prevBN := uint64(1)
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		cumGasUsed, _, _, err := rawtemporaldb.ReceiptAsOf(tx, txNum)
		if err != nil {
			return err
		}
		blockNum := badFoundBlockNum(tx, prevBN-1, txNumsReader, txNum)
		//fmt.Printf("[dbg.integrity] cumGasUsed=%d, txNum=%d, blockNum=%d, prevCumGasUsed=%d\n", cumGasUsed, txNum, blockNum, prevCumGasUsed)
		if int(cumGasUsed) == prevCumGasUsed && cumGasUsed != 0 && blockNum == prevBN {
			_min, _ := txNumsReader.Min(tx, blockNum)
			_max, _ := txNumsReader.Max(tx, blockNum)
			err := fmt.Errorf("bad receipt at txnum: %d, block: %d(%d-%d), cumGasUsed=%d, prevCumGasUsed=%d", txNum, blockNum, _min, _max, cumGasUsed, prevCumGasUsed)
			log.Warn(err.Error())
			return err
			//panic(err)
		}
		prevCumGasUsed = int(cumGasUsed)
		prevBN = blockNum

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logEvery.C:
			log.Info("[integrity] ReceiptsNoDuplicates", "progress", fmt.Sprintf("%dk/%dk", txNum/1_000, toTxNum/1_000))
		default:
		}
	}
	return nil
}

func badFoundBlockNum(tx kv.Tx, fromBlock uint64, txNumsReader rawdbv3.TxNumsReader, curTxNum uint64) uint64 {
	txNumMax, _ := txNumsReader.Max(tx, fromBlock)
	i := uint64(0)
	for txNumMax < curTxNum {
		i++
		txNumMax, _ = txNumsReader.Max(tx, fromBlock+i)
	}
	return fromBlock + i
}

func customTraceBatch(ctx context.Context, produce Produce, cfg *exec3.ExecArgs, tx kv.TemporalRwTx, doms *state2.SharedDomains, fromBlock, toBlock uint64, logPrefix string, logger log.Logger) error {
	defer cfg.BlockReader.Snapshots().(*freezeblocks.RoSnapshots).EnableReadAhead().DisableReadAhead()
	//defer tx.(state2.HasAggTx).AggTx().(*state2.AggregatorRoTx).MadvNormal().DisableReadAhead()

	const logPeriod = 5 * time.Second
	logEvery := time.NewTicker(logPeriod)
	defer logEvery.Stop()

	var cumulativeBlobGasUsedInBlock uint64

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.BlockReader))
	fromTxNum, _ := txNumsReader.Min(tx, fromBlock)
	prevTxNumLog := fromTxNum

	var m runtime.MemStats
	if err := exec3.CustomTraceMapReduce(fromBlock, toBlock, exec3.TraceConsumer{
		Reduce: func(txTask *state.TxTask, tx kv.TemporalTx) error {
			if txTask.Error != nil {
				return txTask.Error
			}

			if txTask.Tx != nil {
				cumulativeBlobGasUsedInBlock += txTask.Tx.GetBlobGas()
			}

			doms.SetTx(tx)
			doms.SetTxNum(txTask.TxNum)

			if produce.ReceiptDomain {
				if !txTask.Final {
					var receipt *types.Receipt
					if txTask.TxIndex >= 0 {
						receipt = txTask.BlockReceipts[txTask.TxIndex]
					}
					if err := rawtemporaldb.AppendReceipt(doms, receipt, cumulativeBlobGasUsedInBlock); err != nil {
						return err
					}
				}

				if txTask.Final { // block changed
					if cfg.ChainConfig.Bor != nil && txTask.TxIndex >= 1 {
						// get last receipt and store the last log index + 1
						lastReceipt := txTask.BlockReceipts[txTask.TxIndex-1]
						if lastReceipt == nil {
							return fmt.Errorf("receipt is nil but should be populated, txIndex=%d, block=%d", txTask.TxIndex-1, txTask.BlockNum)
						}
						if len(lastReceipt.Logs) > 0 {
							firstIndex := lastReceipt.Logs[len(lastReceipt.Logs)-1].Index + 1
							receipt := types.Receipt{
								CumulativeGasUsed:        lastReceipt.CumulativeGasUsed,
								FirstLogIndexWithinBlock: uint32(firstIndex),
							}

							if err := rawtemporaldb.AppendReceipt(doms, &receipt, cumulativeBlobGasUsedInBlock); err != nil {
								return err
							}
						}
					}

					cumulativeBlobGasUsedInBlock = 0
				}
			}

			if produce.RCacheDomain {
				var receipt *types.Receipt
				if !txTask.Final {
					if txTask.TxIndex >= 0 && txTask.BlockReceipts != nil {
						receipt = txTask.BlockReceipts[txTask.TxIndex]
					}
				} else {
					if cfg.ChainConfig.Bor != nil && txTask.TxIndex >= 1 {
						receipt = txTask.BlockReceipts[txTask.TxIndex-1]
						if receipt == nil {
							return fmt.Errorf("receipt is nil but should be populated, txIndex=%d, block=%d", txTask.TxIndex-1, txTask.BlockNum)
						}
					}
				}
				if err := rawdb.WriteReceiptCacheV2(doms, receipt); err != nil {
					return err
				}
			}

			if produce.LogAddr {
				for _, lg := range txTask.Logs {
					if err := doms.IndexAdd(kv.LogAddrIdx, lg.Address[:]); err != nil {
						return err
					}
				}
			}
			if produce.LogTopic {
				for _, lg := range txTask.Logs {
					for _, topic := range lg.Topics {
						if err := doms.IndexAdd(kv.LogTopicIdx, topic[:]); err != nil {
							return err
						}
					}
				}
			}
			if produce.TraceFrom {
				for addr := range txTask.TraceFroms {
					if err := doms.IndexAdd(kv.TracesFromIdx, addr[:]); err != nil {
						return err
					}
				}
			}
			if produce.TraceTo {
				for addr := range txTask.TraceTos {
					if err := doms.IndexAdd(kv.TracesToIdx, addr[:]); err != nil {
						return err
					}
				}
			}

			select {
			case <-logEvery.C:
				if prevTxNumLog > 0 {
					dbg.ReadMemStats(&m)
					log.Info(fmt.Sprintf("[%s] Scanned", logPrefix), "block", txTask.BlockNum, "txs/sec", (txTask.TxNum-prevTxNumLog)/uint64(logPeriod.Seconds()), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
				}
				prevTxNumLog = txTask.TxNum
			default:
			}
			return nil
		},
	}, ctx, tx, cfg, logger); err != nil {
		return err
	}

	return nil
}
