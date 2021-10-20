package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"reflect"

	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

type FinishCfg struct {
	db        kv.RwDB
	tmpDir    string
	btClient  *snapshotsync.Client
	snBuilder *snapshotsync.SnapshotMigrator
	log       log.Logger
}

func StageFinishCfg(db kv.RwDB, tmpDir string, btClient *snapshotsync.Client, snBuilder *snapshotsync.SnapshotMigrator, logger log.Logger) FinishCfg {
	return FinishCfg{
		db:        db,
		log:       logger,
		tmpDir:    tmpDir,
		btClient:  btClient,
		snBuilder: snBuilder,
	}
}

func FinishForward(s *StageState, tx kv.RwTx, cfg FinishCfg, initialCycle bool) error {
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
	if executionAt <= s.BlockNumber {
		return nil
	}

	if cfg.snBuilder != nil && useExternalTx {
		snBlock := snapshotsync.CalculateEpoch(executionAt, snapshotsync.EpochSize)
		err = cfg.snBuilder.AsyncStages(snBlock, cfg.log, cfg.db, tx, cfg.btClient, true)
		if err != nil {
			return err
		}
		if cfg.snBuilder.Replaced() {
			err = cfg.snBuilder.SyncStages(snBlock, cfg.db, tx)
			if err != nil {
				return err
			}
		}
	}
	rawdb.WriteHeadBlockHash(tx, rawdb.ReadHeadHeaderHash(tx))
	err = s.Update(tx, executionAt)
	if err != nil {
		return err
	}

	if err := stages.SaveSyncTime(tx); err != nil {
		return err
	}

	if initialCycle {
		if err := params.SetErigonVersion(tx, params.VersionKeyFinished); err != nil {
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

func NotifyNewHeaders(ctx context.Context, finishStageBeforeSync uint64, finishStageAfterSync uint64, unwindTo *uint64, notifier ChainEventNotifier, tx kv.Tx) error {
	if notifier == nil {
		log.Trace("RPC Daemon notification channel not set. No headers notifications will be sent")
		return nil
	}

	// Notify all headers we have (either canonical or not) in a maximum range span of 1024
	var notifyFrom uint64
	if unwindTo != nil && *unwindTo != 0 && (*unwindTo) < finishStageBeforeSync {
		notifyFrom = *unwindTo
	} else {
		heightSpan := finishStageAfterSync - finishStageBeforeSync
		if heightSpan > 1024 {
			heightSpan = 1024
		}
		notifyFrom = finishStageAfterSync - heightSpan
	}
	notifyFrom++

	startKey := make([]byte, reflect.TypeOf(notifyFrom).Size()+32)
	var notifyTo uint64
	binary.BigEndian.PutUint64(startKey, notifyFrom)
	if err := tx.ForEach(kv.Headers, startKey, func(k, headerRLP []byte) error {
		if len(headerRLP) == 0 {
			return nil
		}
		header := new(types.Header)
		if err := rlp.Decode(bytes.NewReader(headerRLP), header); err != nil {
			log.Error("Invalid block header RLP", "err", err)
			return err
		}
		notifyTo = header.Number.Uint64()
		notifier.OnNewHeader(header)
		return libcommon.Stopped(ctx.Done())
	}); err != nil {
		log.Error("RPC Daemon notification failed", "error", err)
		return err
	}

	log.Info("RPC Daemon notified of new headers", "from", notifyFrom-1, "to", notifyTo)
	return nil
}
