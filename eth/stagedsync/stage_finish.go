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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/kv/dbutils"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/turbo/engineapi/engine_helpers"
	"github.com/erigontech/erigon/turbo/services"

	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/ethdb/cbor"
	"github.com/erigontech/erigon/params"
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

func FinishForward(s *StageState, tx kv.RwTx, cfg FinishCfg) error {
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
	if err := tx.ForEach(kv.HeaderCanonical, hexutility.EncodeTs(notifyFrom), func(k, hash []byte) (err error) {
		if len(hash) == 0 {
			return nil
		}
		blockNum := binary.BigEndian.Uint64(k)
		if blockNum > finishStageAfterSync { //[from,to)
			return nil
		}
		notifyTo = blockNum
		notifyToHash = libcommon.BytesToHash(hash)
		headerRLP := rawdb.ReadHeaderRLP(tx, notifyToHash, notifyTo)
		if headerRLP != nil {
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
		logger.Debug("RPC Daemon notified of new headers", "from", notifyFrom-1, "to", notifyTo, "amount", len(headersRlp), "hash", notifyToHash, "header sending", headerTiming, "log sending", logTiming)
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
			txHash = bortypes.ComputeBorTxHash(blockNum, block.Hash())
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

// Requirements:
// - Erigon3 doesn't store logs in db (yet)
// - need support unwind of receipts
// - need send notification after `rwtx.Commit` (or user will recv notification, but can't request new data by RPC)
type RecentLogs struct {
	receipts map[uint64][]*remote.SubscribeLogsReply
	limit    uint64
	mu       sync.Mutex
}

func NewRecentLogs(limit uint64) *RecentLogs {
	return &RecentLogs{receipts: make(map[uint64][]*remote.SubscribeLogsReply, limit), limit: limit}
}

// [from,to)
func (r *RecentLogs) Notify(n ChainEventNotifier, isUnwind bool, from, to uint64) {
	if !n.HasLogSubsriptions() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for bn, res := range r.receipts {
		if bn+r.limit < from { //evict old
			delete(r.receipts, bn)
			continue
		}
		if bn < from || bn >= to {
			continue
		}
		for i := range res {
			res[i].Removed = isUnwind
		}
		n.OnLogs(res)
	}
}

func (r *RecentLogs) Add(block *types.Block, receipts types.Receipts) {
	blockNum := block.NumberU64()
	var txIndex, logIndex uint64
	var txHash libcommon.Hash
	reply := make([]*remote.SubscribeLogsReply, 0, len(receipts))
	for _, receipt := range receipts {
		txIndex++
		// bor transactions are at the end of the bodies transactions (added manually but not actually part of the block)
		if txIndex == uint64(len(block.Transactions())) {
			txHash = bortypes.ComputeBorTxHash(blockNum, block.Hash())
		} else {
			txHash = block.Transactions()[txIndex].Hash()
		}

		for _, l := range receipt.Logs {
			r := &remote.SubscribeLogsReply{
				Address:          gointerfaces.ConvertAddressToH160(l.Address),
				BlockHash:        gointerfaces.ConvertHashToH256(block.Hash()),
				BlockNumber:      blockNum,
				Data:             l.Data,
				LogIndex:         logIndex,
				Topics:           make([]*types2.H256, 0, len(l.Topics)),
				TransactionHash:  gointerfaces.ConvertHashToH256(txHash),
				TransactionIndex: txIndex,
			}
			logIndex++
			for _, topic := range l.Topics {
				r.Topics = append(r.Topics, gointerfaces.ConvertHashToH256(topic))
			}
			reply = append(reply, r)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.receipts[blockNum] = reply
}
