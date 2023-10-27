package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/params"
)

type FinishCfg struct {
	db            kv.RwDB
	tmpDir        string
	forkValidator *engine_helpers.ForkValidator
}

func StageFinishCfg(db kv.RwDB, tmpDir string, forkValidator *engine_helpers.ForkValidator) FinishCfg {
	return FinishCfg{
		db:            db,
		tmpDir:        tmpDir,
		forkValidator: forkValidator,
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

func NotifyNewHeaders(ctx context.Context, finishStageBeforeSync uint64, finishStageAfterSync uint64, unwindTo *uint64, notifier ChainEventNotifier, tx kv.Tx, logger log.Logger, blockReader services.FullBlockReader) error {
	t := time.Now()
	if notifier == nil {
		logger.Trace("RPC Daemon notification channel not set. No headers notifications will be sent")
		return nil
	}
	// Notify all headers we have (either canonical or not) in a maximum range span of 1024
	var notifyFrom uint64
	var isUnwind bool
	if unwindTo != nil && *unwindTo != 0 && (*unwindTo) < finishStageBeforeSync {
		notifyFrom = *unwindTo
		isUnwind = true
	} else {
		heightSpan := finishStageAfterSync - finishStageBeforeSync
		if heightSpan > 1024 {
			heightSpan = 1024
		}
		notifyFrom = finishStageAfterSync - heightSpan
	}
	notifyFrom++

	var notifyTo = notifyFrom
	var notifyToHash libcommon.Hash
	var headersRlp [][]byte
	if err := tx.ForEach(kv.Headers, hexutility.EncodeTs(notifyFrom), func(k, headerRLP []byte) error {
		if len(headerRLP) == 0 {
			return nil
		}
		notifyTo = binary.BigEndian.Uint64(k)
		var err error
		if notifyToHash, err = blockReader.CanonicalHash(ctx, tx, notifyTo); err != nil {
			logger.Warn("[Finish] failed checking if header is cannonical")
		}

		headerHash := libcommon.BytesToHash(k[8:])
		if notifyToHash == headerHash {
			headersRlp = append(headersRlp, libcommon.CopyBytes(headerRLP))
		}

		return libcommon.Stopped(ctx.Done())
	}); err != nil {
		logger.Error("RPC Daemon notification failed", "err", err)
		return err
	}

	if len(headersRlp) > 0 {
		notifier.OnNewHeader(headersRlp)
		headerTiming := time.Since(t)

		t = time.Now()
		if notifier.HasLogSubsriptions() {
			logs, err := ReadLogs(tx, notifyFrom, isUnwind, blockReader)
			if err != nil {
				return err
			}
			notifier.OnLogs(logs)
		}
		logTiming := time.Since(t)
		logger.Info("RPC Daemon notified of new headers", "from", notifyFrom-1, "to", notifyTo, "hash", notifyToHash, "header sending", headerTiming, "log sending", logTiming)
	}
	return nil
}

func ReadLogs(tx kv.Tx, from uint64, isUnwind bool, blockReader services.FullBlockReader) ([]*remote.SubscribeLogsReply, error) {
	logs, err := tx.Cursor(kv.Log)
	if err != nil {
		return nil, err
	}
	defer logs.Close()
	reply := make([]*remote.SubscribeLogsReply, 0)
	reader := bytes.NewReader(nil)

	var prevBlockNum uint64
	var block *types.Block
	var logIndex uint64
	for k, v, err := logs.Seek(dbutils.LogKey(from, 0)); k != nil; k, v, err = logs.Next() {
		if err != nil {
			return nil, err
		}
		blockNum := binary.BigEndian.Uint64(k[:8])
		if block == nil || blockNum != prevBlockNum {
			logIndex = 0
			prevBlockNum = blockNum
			if block, err = blockReader.BlockByNumber(context.Background(), tx, blockNum); err != nil {
				return nil, err
			}
		}

		txIndex := uint64(binary.BigEndian.Uint32(k[8:]))

		var txHash libcommon.Hash

		// bor transactions are at the end of the bodies transactions (added manually but not actually part of the block)
		if txIndex == uint64(len(block.Transactions())) {
			txHash = types.ComputeBorTxHash(blockNum, block.Hash())
		} else {
			txHash = block.Transactions()[txIndex].Hash()
		}

		var ll types.Logs
		reader.Reset(v)
		if err := cbor.Unmarshal(&ll, reader); err != nil {
			return nil, fmt.Errorf("receipt unmarshal failed: %w, blocl=%d", err, blockNum)
		}
		for _, l := range ll {
			r := &remote.SubscribeLogsReply{
				Address:          gointerfaces.ConvertAddressToH160(l.Address),
				BlockHash:        gointerfaces.ConvertHashToH256(block.Hash()),
				BlockNumber:      blockNum,
				Data:             l.Data,
				LogIndex:         logIndex,
				Topics:           make([]*types2.H256, 0, len(l.Topics)),
				TransactionHash:  gointerfaces.ConvertHashToH256(txHash),
				TransactionIndex: txIndex,
				Removed:          isUnwind,
			}
			logIndex++
			for _, topic := range l.Topics {
				r.Topics = append(r.Topics, gointerfaces.ConvertHashToH256(topic))
			}
			reply = append(reply, r)
		}
	}

	return reply, nil
}
