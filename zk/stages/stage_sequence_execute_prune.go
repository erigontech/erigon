package stages

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
)

func PruneSequenceExecutionStage(s *stagedsync.PruneState, tx kv.RwTx, cfg SequenceBlockCfg, ctx context.Context, initialCycle bool) (err error) {
	logPrefix := s.LogPrefix()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	if cfg.historyV3 {
		cfg.agg.SetTx(tx)
		if initialCycle {
			if err = cfg.agg.Prune(ctx, config3.HistoryV3AggregationStep/10); err != nil { // prune part of retired data, before commit
				return err
			}
		} else {
			if err = cfg.agg.PruneWithTiemout(ctx, 1*time.Second); err != nil { // prune part of retired data, before commit
				return err
			}
		}
	} else {
		if cfg.prune.History.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.AccountChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
			if err = rawdb.PruneTableDupSort(tx, kv.StorageChangeSet, logPrefix, cfg.prune.History.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}

		if cfg.prune.Receipts.Enabled() {
			if err = rawdb.PruneTable(tx, kv.Receipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
			if err = rawdb.PruneTable(tx, kv.BorReceipts, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxUint32); err != nil {
				return err
			}
			// LogIndex.Prune will read everything what not pruned here
			if err = rawdb.PruneTable(tx, kv.Log, cfg.prune.Receipts.PruneTo(s.ForwardProgress), ctx, math.MaxInt32); err != nil {
				return err
			}
		}
		if cfg.prune.CallTraces.Enabled() {
			if err = rawdb.PruneTableDupSort(tx, kv.CallTraceSet, logPrefix, cfg.prune.CallTraces.PruneTo(s.ForwardProgress), logEvery, ctx); err != nil {
				return err
			}
		}
	}

	if err = s.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
