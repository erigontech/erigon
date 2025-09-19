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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/integrity"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
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

func StageCustomTraceCfg(produce []string, db kv.TemporalRwDB, dirs datadir.Dirs, br services.FullBlockReader,
	cc *chain.Config, engine consensus.Engine,
	genesis *types.Genesis, syncCfg ethconfig.Sync) CustomTraceCfg {
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
	if cfg.Produce.RCacheDomain {
		if err := cfg.db.View(context.Background(), func(tx kv.Tx) error {
			return kvcfg.PersistReceipts.MustBeEnabled(tx, "you must enable `--persist.receipts` flag in db. remove chaindata and start erigon with this flag")
		}); err != nil {
			panic(err)
		}
	}

	log.Info("[stage_custom_trace] start params", "produce", cfg.Produce)
	txNumsReader := cfg.ExecArgs.BlockReader.TxnumReader(ctx)

	//agg := cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	//stepSize := agg.StepSize()

	// 1. Require stage_exec > 0: means don't need handle "half-block execution case here"
	// 2. Require stage_exec > 0: means has enough state-history
	var execProgress uint64
	var startBlock, endBlock uint64
	if err := cfg.db.ViewTemporal(ctx, func(tx kv.TemporalTx) (err error) {
		execProgress, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		if execProgress == 0 {
			return errors.New("stage_exec progress is 0. please run `integration stage_exec --batchSize=1m` for couple minutes")
		}

		fromTxNum := progressOfDomains(tx, cfg.Produce)
		var ok bool
		startBlock, ok, err = txNumsReader.FindBlockNum(tx, fromTxNum)
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

	defer cfg.ExecArgs.BlockReader.Snapshots().(*freezeblocks.RoSnapshots).MadvNormal().DisableReadAhead()
	//defer tx.(dbstate.HasAggTx).AggTx().(*dbstate.AggregatorRoTx).MadvNormal().DisableReadAhead()

	log.Info("SpawnCustomTrace", "startBlock", startBlock, "endBlock", endBlock)
	batchSize := uint64(50_000)
	for startBlock < endBlock {
		to := min(endBlock+1, startBlock+batchSize)
		if err := customTraceBatchProduce(ctx, cfg.Produce, cfg.ExecArgs, cfg.db, startBlock, to, "custom_trace", logger); err != nil {
			return err
		}
		startBlock = to
	}

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	chkEvery := time.NewTicker(3 * time.Second)
	defer chkEvery.Stop()

Loop:
	for {
		select {
		case <-ctx.Done():
			logger.Warn("[snapshots] user has interrupted process but anyway waiting for build & merge files")
		case <-cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).WaitForBuildAndMerge(context.Background()):
			break Loop // Here we don't quit due to ctx because it's not safe for files.
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			//TODO: log progress and list of domains/files
			logger.Info("[snapshots] Building files", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		}
	}

	log.Info("SpawnCustomTrace finish")
	if cfg.Produce.ReceiptDomain {
		if err := AssertNotBehindAccounts(cfg.db, kv.ReceiptDomain, txNumsReader); err != nil {
			return err
		}
	}
	if cfg.Produce.RCacheDomain {
		if err := AssertNotBehindAccounts(cfg.db, kv.RCacheDomain, txNumsReader); err != nil {
			return err
		}
	}

	return nil
}

func customTraceBatchProduce(ctx context.Context, produce Produce, cfg *exec3.ExecArgs, db kv.TemporalRwDB, fromBlock, toBlock uint64, logPrefix string, logger log.Logger) error {
	if err := db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		if err := tx.GreedyPruneHistory(ctx, kv.CommitmentDomain); err != nil {
			return err
		}
		if _, err := tx.PruneSmallBatches(ctx, 10*time.Hour); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	var lastTxNum uint64
	{
		tx, err := db.BeginTemporalRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		doms, err := dbstate.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer doms.Close()

		if err := customTraceBatch(ctx, produce, cfg, tx, doms, fromBlock, toBlock, logPrefix, logger); err != nil {
			return err
		}

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

	agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	var fromStep, toStep kv.Step
	if err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		fromStep = firstStepNotInFiles(tx, produce)
		if lastTxNum/agg.StepSize() > 0 {
			toStep = kv.Step(lastTxNum / agg.StepSize())
		}
		return nil
	}); err != nil {
		return err
	}
	if err := agg.BuildFiles2(ctx, fromStep, toStep); err != nil {
		return err
	}
	if err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := tx.(kv.TemporalRwTx).GreedyPruneHistory(ctx, kv.CommitmentDomain); err != nil {
			return err
		}
		if _, err := tx.(kv.TemporalRwTx).PruneSmallBatches(ctx, 10*time.Hour); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func AssertNotBehindAccounts(db kv.TemporalRoDB, domain kv.Domain, txNumsReader rawdbv3.TxNumsReader) (err error) {
	tx, err := db.BeginTemporalRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	receiptProgress := tx.Debug().DomainProgress(domain)
	accProgress := tx.Debug().DomainProgress(kv.AccountsDomain)
	if accProgress != receiptProgress {
		e1, _, _ := txNumsReader.FindBlockNum(tx, receiptProgress)
		e2, _, _ := txNumsReader.FindBlockNum(tx, accProgress)

		err := fmt.Errorf("[integrity] %s=%d (%d) is behind AccountDomain=%d(%d)", domain.String(), receiptProgress, e1, accProgress, e2)
		log.Warn(err.Error())
		return nil
	}
	return nil
}

func AssertReceipts(ctx context.Context, cfg *exec3.ExecArgs, tx kv.TemporalTx, fromBlock, toBlock uint64) (err error) {
	if !dbg.AssertEnabled {
		return
	}
	if cfg.ChainConfig.Bor != nil { //TODO: enable me
		return nil
	}
	return integrity.ReceiptsNoDupsRange(ctx, fromBlock, toBlock, tx, cfg.BlockReader, true)
}

func customTraceBatch(ctx context.Context, produce Produce, cfg *exec3.ExecArgs, tx kv.TemporalRwTx, doms *dbstate.SharedDomains, fromBlock, toBlock uint64, logPrefix string, logger log.Logger) error {
	const logPeriod = 5 * time.Second
	logEvery := time.NewTicker(logPeriod)
	defer logEvery.Stop()

	var cumulativeBlobGasUsedInBlock uint64

	txNumsReader := cfg.BlockReader.TxnumReader(ctx)
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

			doms.SetTxNum(txTask.TxNum)
			putter := doms.AsPutDel(tx)

			if produce.ReceiptDomain {
				var logIndexAfterTx uint32
				var cumGasUsed uint64
				if !txTask.Final {
					if txTask.TxIndex >= 0 {
						receipt := txTask.BlockReceipts[txTask.TxIndex]
						if receipt != nil {
							logIndexAfterTx = receipt.FirstLogIndexWithinBlock + uint32(len(txTask.Logs))
							cumGasUsed = receipt.CumulativeGasUsed
						}
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
							logIndexAfterTx = uint32(firstIndex) + uint32(len(txTask.Logs))
							cumGasUsed = lastReceipt.CumulativeGasUsed
						}
					}
				}

				if err := rawtemporaldb.AppendReceipt(putter, logIndexAfterTx, cumGasUsed, cumulativeBlobGasUsedInBlock, txTask.TxNum); err != nil {
					return err
				}
				if txTask.Final { // block changed
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
				if err := rawdb.WriteReceiptCacheV2(putter, receipt, txTask.TxNum); err != nil {
					return err
				}
			}

			if produce.LogAddr {
				for _, lg := range txTask.Logs {
					if err := doms.IndexAdd(kv.LogAddrIdx, lg.Address[:], txTask.TxNum); err != nil {
						return err
					}
				}
			}
			if produce.LogTopic {
				for _, lg := range txTask.Logs {
					for _, topic := range lg.Topics {
						if err := doms.IndexAdd(kv.LogTopicIdx, topic[:], txTask.TxNum); err != nil {
							return err
						}
					}
				}
			}
			if produce.TraceFrom {
				for addr := range txTask.TraceFroms {
					if err := doms.IndexAdd(kv.TracesFromIdx, addr[:], txTask.TxNum); err != nil {
						return err
					}
				}
			}
			if produce.TraceTo {
				for addr := range txTask.TraceTos {
					if err := doms.IndexAdd(kv.TracesToIdx, addr[:], txTask.TxNum); err != nil {
						return err
					}
				}
			}

			select {
			case <-logEvery.C:
				if prevTxNumLog > 0 {
					dbg.ReadMemStats(&m)
					txsPerSec := (txTask.TxNum - prevTxNumLog) / uint64(logPeriod.Seconds())
					log.Info(fmt.Sprintf("[%s] Scanned", logPrefix), "block", fmt.Sprintf("%.3fm", float64(txTask.BlockNum)/1_000_000), "tx/s", fmt.Sprintf("%.1fK", float64(txsPerSec)/1_000.0), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
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

func progressOfDomains(tx kv.TemporalTx, produce Produce) uint64 {
	//TODO: need better way to detect start point. What if domain/index is sparse (has rare events).
	dbg := tx.Debug()
	txNum := uint64(math.MaxUint64)
	if produce.ReceiptDomain {
		txNum = min(txNum, dbg.DomainProgress(kv.ReceiptDomain))
	}
	if produce.RCacheDomain {
		txNum = min(txNum, dbg.DomainProgress(kv.RCacheDomain))
	}
	if produce.LogAddr {
		txNum = min(txNum, dbg.IIProgress(kv.LogAddrIdx))
	}
	if produce.LogTopic {
		txNum = min(txNum, dbg.IIProgress(kv.LogTopicIdx))
	}
	if produce.TraceFrom {
		txNum = min(txNum, dbg.IIProgress(kv.TracesFromIdx))
	}
	if produce.TraceTo {
		txNum = min(txNum, dbg.IIProgress(kv.TracesToIdx))
	}
	return txNum
}

func firstStepNotInFiles(tx kv.Tx, produce Produce) kv.Step {
	//TODO: need better way to detect start point. What if domain/index is sparse (has rare events).
	ac := dbstate.AggTx(tx)
	fromStep := kv.Step(math.MaxUint64)
	if produce.ReceiptDomain {
		fromStep = min(fromStep, ac.DbgDomain(kv.ReceiptDomain).FirstStepNotInFiles())
	}
	if produce.RCacheDomain {
		fromStep = min(fromStep, ac.DbgDomain(kv.RCacheDomain).FirstStepNotInFiles())
	}
	if produce.LogAddr {
		fromStep = min(fromStep, ac.DbgII(kv.LogAddrIdx).FirstStepNotInFiles())
	}
	if produce.LogTopic {
		fromStep = min(fromStep, ac.DbgII(kv.LogTopicIdx).FirstStepNotInFiles())
	}
	if produce.TraceFrom {
		fromStep = min(fromStep, ac.DbgII(kv.TracesFromIdx).FirstStepNotInFiles())
	}
	if produce.TraceTo {
		fromStep = min(fromStep, ac.DbgII(kv.TracesToIdx).FirstStepNotInFiles())
	}
	return fromStep
}

func StageCustomTraceReset(ctx context.Context, db kv.TemporalRwDB, produce Produce) error {
	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var tables []string
	if produce.ReceiptDomain {
		tables = append(tables, db.Debug().DomainTables(kv.ReceiptDomain)...)
	}
	if produce.RCacheDomain {
		tables = append(tables, db.Debug().DomainTables(kv.RCacheDomain)...)
	}
	if produce.LogAddr {
		tables = append(tables, db.Debug().InvertedIdxTables(kv.LogAddrIdx)...)
	}
	if produce.LogTopic {
		tables = append(tables, db.Debug().InvertedIdxTables(kv.LogTopicIdx)...)
	}
	if produce.TraceFrom {
		tables = append(tables, db.Debug().InvertedIdxTables(kv.TracesFromIdx)...)
	}
	if produce.TraceTo {
		tables = append(tables, db.Debug().InvertedIdxTables(kv.TracesToIdx)...)
	}
	if err := backup.ClearTables(ctx, tx, tables...); err != nil {
		return err
	}
	return tx.Commit()
}
