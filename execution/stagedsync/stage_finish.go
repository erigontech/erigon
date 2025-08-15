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
	"encoding/binary"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
)

type FinishCfg struct {
	db                kv.RwDB
	tmpDir            string
	forkValidator     *engine_helpers.ForkValidator
	initialCycleStart *time.Time
}

func StageFinishCfg(db kv.RwDB, tmpDir string, forkValidator *engine_helpers.ForkValidator) FinishCfg {
	initialCycleStart := time.Now()
	return FinishCfg{
		db:                db,
		tmpDir:            tmpDir,
		forkValidator:     forkValidator,
		initialCycleStart: &initialCycleStart,
	}
}

func FinishForward(s *StageState, tx kv.RwTx, cfg FinishCfg) error {
	defer updateInitialCycleDuration(s, cfg)
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	var executionAt uint64
	var err error
	if executionAt, err = s.ExecutionAt(tx); err != nil {
		return err
	}
	if s.BlockNumber > executionAt { // Erigon will self-heal (download missed blocks) eventually
		return nil
	}
	if executionAt <= s.BlockNumber {
		return nil
	}

	rawdb.WriteHeadBlockHash(tx, rawdb.ReadHeadHeaderHash(tx))
	err = s.Update(tx, executionAt)
	if err != nil {
		return err
	}
	if cfg.forkValidator != nil {
		cfg.forkValidator.NotifyCurrentHeight(executionAt)
	}

	if s.CurrentSyncCycle.IsInitialCycle {
		if err := rawdb.SetErigonVersion(tx, version.VersionKeyFinished); err != nil {
			return err
		}
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func updateInitialCycleDuration(s *StageState, cfg FinishCfg) {
	if s.CurrentSyncCycle.IsInitialCycle {
		initialCycleDurationSecs.Set(time.Since(*cfg.initialCycleStart).Seconds())
	} else {
		*cfg.initialCycleStart = time.Now()
		initialCycleDurationSecs.Set(0)
	}
}

func UnwindFinish(u *UnwindState, tx kv.RwTx, cfg FinishCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err = u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneFinish(u *PruneState, tx kv.RwTx, cfg FinishCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// [from,to)
func NotifyNewHeaders(ctx context.Context, notifyFrom, notifyTo uint64, notifier ChainEventNotifier, tx kv.Tx, logger log.Logger) error {
	if notifier == nil {
		logger.Trace("RPC Daemon notification channel not set. No headers notifications will be sent")
		return nil
	}
	// Notify all headers we have (either canonical or not) in a maximum range span of 1024
	var headersRlp [][]byte
	if err := tx.ForEach(kv.HeaderCanonical, hexutil.EncodeTs(notifyFrom), func(k, hash []byte) (err error) {
		if len(hash) == 0 {
			return nil
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum >= notifyTo { //[from,to)
			return nil
		}
		headerRLP := rawdb.ReadHeaderRLP(tx, common.BytesToHash(hash), blockNum)
		if headerRLP != nil {
			headersRlp = append(headersRlp, common.CopyBytes(headerRLP))
		}
		return common.Stopped(ctx.Done())
	}); err != nil {
		logger.Error("RPC Daemon notification failed", "err", err)
		return err
	}

	if len(headersRlp) > 0 {
		notifier.OnNewHeader(headersRlp)
		logger.Debug("RPC Daemon notified of new headers", "from", notifyFrom-1, "to", notifyTo, "amount", len(headersRlp))
	}
	return nil
}
