package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/erigon/cmd/state/exec3"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
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

func SpawnCustomTrace(s *StageState, txc wrap.TxContainer, cfg CustomTraceCfg, ctx context.Context, initialCycle bool, prematureEndBlock uint64, logger log.Logger) error {
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

	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return fmt.Errorf("getting last executed block: %w", err)
	}
	if s.BlockNumber > endBlock { // Erigon will self-heal (download missed blocks) eventually
		return nil
	}
	// if prematureEndBlock is nonzero and less than the latest executed block,
	// then we only run the log index stage until prematureEndBlock
	if prematureEndBlock != 0 && prematureEndBlock < endBlock {
		endBlock = prematureEndBlock
	}
	// It is possible that prematureEndBlock < s.BlockNumber,
	// in which case it is important that we skip this stage,
	// or else we could overwrite stage_at with prematureEndBlock
	if endBlock <= s.BlockNumber {
		return nil
	}

	startBlock := s.BlockNumber
	if startBlock > 0 {
		startBlock++
	}

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()
	var m runtime.MemStats
	var prevBlockNumLog uint64 = startBlock

	doms, err := state2.NewSharedDomains(txc.Tx, logger)
	if err != nil {
		return err
	}
	defer doms.Close()

	key := []byte{0}
	total := uint256.NewInt(0)

	it, err := tx.IndexRange(kv.GasUsedHistoryIdx, key, -1, -1, order.Desc, 1)
	if err != nil {
		return err
	}
	if it.HasNext() {
		lastTxNum, err := it.Next()
		if err != nil {
			return err
		}
		lastTotal, ok, err := tx.HistoryGet(kv.GasUsedHistory, key, lastTxNum)
		if err != nil {
			return err
		}
		if ok {
			total.SetBytes(lastTotal)
		}
	}

	//TODO: new tracer may get tracer from pool, maybe add it to TxTask field
	/// maybe need startTxNum/endTxNum
	if err = exec3.CustomTraceMapReduce(startBlock, endBlock, exec3.TraceConsumer{
		NewTracer: func() exec3.GenericTracer { return nil },
		Collect: func(txTask *state.TxTask) error {
			if txTask.Error != nil {
				return err
			}

			total.AddUint64(total, txTask.UsedGas)
			doms.SetTxNum(txTask.TxNum)
			v := total.Bytes()
			err = doms.DomainPut(kv.GasUsedDomain, key, nil, v, nil, 0)
			if err != nil {
				return err
			}

			select {
			default:
			case <-logEvery.C:
				dbg.ReadMemStats(&m)
				log.Info("Scanned", "block", txTask.BlockNum, "blk/sec", float64(txTask.BlockNum-prevBlockNumLog)/10, "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
				prevBlockNumLog = txTask.BlockNum
			}

			return nil
		},
	}, ctx, tx, cfg.execArgs, logger); err != nil {
		return err
	}
	if err = s.Update(txc.Tx, endBlock); err != nil {
		return err
	}

	if err := doms.Flush(ctx, txc.Tx); err != nil {
		return err
	}

	if !useExternalTx {
		if err = txc.Tx.Commit(); err != nil {
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

func PruneCustomTrace(s *PruneState, tx kv.RwTx, cfg CustomTraceCfg, ctx context.Context, initialCycle bool, logger log.Logger) (err error) {
	return nil
}
