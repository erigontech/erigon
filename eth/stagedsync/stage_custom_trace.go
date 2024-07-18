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

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/turbo/services"
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

func SpawnCustomTrace(s *StageState, txc wrap.TxContainer, cfg CustomTraceCfg, ctx context.Context, prematureEndBlock uint64, logger log.Logger) error {
	useExternalTx := txc.Ttx != nil
	var tx kv.TemporalRwTx
	if !useExternalTx {
		_tx, err := cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer _tx.Rollback()
		tx = _tx.(kv.TemporalRwTx)
	} else {
		tx = txc.Ttx.(kv.TemporalRwTx)
	}

	//endBlock, err := s.ExecutionAt(tx)
	//if err != nil {
	//	return fmt.Errorf("getting last executed block: %w", err)
	//}
	//if s.BlockNumber > endBlock { // Erigon will self-heal (download missed blocks) eventually
	//	return nil
	//}
	//// if prematureEndBlock is nonzero and less than the latest executed block,
	//// then we only run the log index stage until prematureEndBlock
	//if prematureEndBlock != 0 && prematureEndBlock < endBlock {
	//	endBlock = prematureEndBlock
	//}
	//// It is possible that prematureEndBlock < s.BlockNumber,
	//// in which case it is important that we skip this stage,
	//// or else we could overwrite stage_at with prematureEndBlock
	//if endBlock <= s.BlockNumber {
	//	return nil
	//}
	//
	//startBlock := s.BlockNumber
	//if startBlock > 0 {
	//	startBlock++
	//}
	//
	//logEvery := time.NewTicker(10 * time.Second)
	//defer logEvery.Stop()
	//var m runtime.MemStats
	//var prevBlockNumLog uint64 = startBlock
	//
	//doms, err := state2.NewSharedDomains(tx, logger)
	//if err != nil {
	//	return err
	//}
	//defer doms.Close()
	//
	//cumulative := uint256.NewInt(0)
	//var lastBlockNum uint64
	//
	//canonicalReader := doms.CanonicalReader()
	//lastFrozenID, err := canonicalReader.LastFrozenTxNum(tx)
	//if err != nil {
	//	return err
	//}
	//
	//var baseBlockTxnID, txnID kv.TxnId
	//fmt.Printf("dbg1: %s\n", tx.ViewID())
	////TODO: new tracer may get tracer from pool, maybe add it to TxTask field
	///// maybe need startTxNum/endTxNum
	//if err = exec3.CustomTraceMapReduce(startBlock, endBlock, exec3.TraceConsumer{
	//	NewTracer: func() exec3.GenericTracer { return nil },
	//	Reduce: func(txTask *state.TxTask, tx kv.Tx) error {
	//		if txTask.Error != nil {
	//			return err
	//		}
	//
	//		if lastBlockNum != txTask.BlockNum {
	//			cumulative.Set(u256.N0)
	//			lastBlockNum = txTask.BlockNum
	//
	//			if txTask.TxNum < uint64(lastFrozenID) {
	//				txnID = kv.TxnId(txTask.TxNum)
	//			} else {
	//				h, err := rawdb.ReadCanonicalHash(tx, txTask.BlockNum)
	//				baseBlockTxnID, err = canonicalReader.BaseTxnID(tx, txTask.BlockNum, h)
	//				if err != nil {
	//					return err
	//				}
	//				txnID = baseBlockTxnID
	//			}
	//		} else {
	//			txnID++
	//		}
	//		cumulative.AddUint64(cumulative, txTask.UsedGas)
	//
	//		if txTask.Final || txTask.TxIndex < 0 {
	//			return nil
	//		}
	//		r := txTask.CreateReceipt(cumulative.Uint64())
	//		v, err := rlp.EncodeToBytes(r)
	//		if err != nil {
	//			return err
	//		}
	//		doms.SetTx(tx)
	//		err = doms.AppendablePut(kv.ReceiptsAppendable, txnID, v)
	//		if err != nil {
	//			return err
	//		}
	//
	//		select {
	//		case <-logEvery.C:
	//			dbg.ReadMemStats(&m)
	//			log.Info("Scanned", "block", txTask.BlockNum, "blk/sec", float64(txTask.BlockNum-prevBlockNumLog)/10, "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
	//			prevBlockNumLog = txTask.BlockNum
	//		default:
	//		}
	//
	//		return nil
	//	},
	//}, ctx, tx, cfg.execArgs, logger); err != nil {
	//	return err
	//}
	//if err := doms.Flush(ctx, tx); err != nil {
	//	return err
	//}
	//
	//if err = s.Update(tx.(kv.RwTx), endBlock); err != nil {
	//	return err
	//}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindCustomTrace(u *UnwindState, s *StageState, txc wrap.TxContainer, cfg CustomTraceCfg, ctx context.Context, logger log.Logger) (err error) {
	useExternalTx := txc.Ttx != nil
	var tx kv.TemporalTx
	if !useExternalTx {
		_tx, err := cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer _tx.Rollback()
		tx = _tx.(kv.TemporalTx)
	} else {
		tx = txc.Ttx
	}

	if err := u.Done(tx.(kv.RwTx)); err != nil {
		return fmt.Errorf("%w", err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneCustomTrace(s *PruneState, tx kv.RwTx, cfg CustomTraceCfg, ctx context.Context, logger log.Logger) (err error) {
	return nil
}
