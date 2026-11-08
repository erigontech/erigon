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

package notifications

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

type recordingChainEventNotifier struct {
	logs           []*LogNotification
	logBatches     [][]*LogNotification
	receipts       []*ReceiptNotification
	receiptBatches [][]*ReceiptNotification
}

func (n *recordingChainEventNotifier) OnNewHeader([][]byte) {}

func (n *recordingChainEventNotifier) OnNewPendingLogs(types.Logs) {}

func (n *recordingChainEventNotifier) OnLogs(logs []*LogNotification) {
	n.logs = append(n.logs, logs...)
	n.logBatches = append(n.logBatches, logs)
}

func (n *recordingChainEventNotifier) HasLogSubscriptions() bool { return true }

func (n *recordingChainEventNotifier) OnReceipts(receipts []*ReceiptNotification) {
	n.receipts = append(n.receipts, receipts...)
	n.receiptBatches = append(n.receiptBatches, receipts)
}

func (n *recordingChainEventNotifier) HasReceiptSubscriptions() bool { return true }

type noSubscriptionNotifier struct {
	recordingChainEventNotifier
}

func (n *noSubscriptionNotifier) HasLogSubscriptions() bool { return false }

func (n *noSubscriptionNotifier) HasReceiptSubscriptions() bool { return false }

func TestRecentReceipts(t *testing.T) {
	t.Parallel()
	t.Run("Evict", func(t *testing.T) {
		e := NewRecentReceipts(3)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(11)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(21)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 3)

		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(31)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 1)
	})
	t.Run("Nil", func(t *testing.T) {
		e := NewRecentReceipts(3)
		e.Add(types.Receipts{nil, {BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(21)}, nil}, []types.Transaction{}, nil)
		e.Add(types.Receipts{nil, nil, {BlockNumber: uint256.NewInt(31)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 3)
	})
	t.Run("Order", func(t *testing.T) {
		e := NewRecentReceipts(3)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(11)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 2)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(11)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 2)
	})
}

func TestRecentReceiptsNotifyLogsIncludesBlockTimestamp(t *testing.T) {
	recent := NewRecentReceipts(3)
	recent.Add(types.Receipts{{
		BlockNumber: uint256.NewInt(1),
		Logs:        types.Logs{{}},
	}}, nil, &types.Header{Time: 123})
	notifier := &recordingChainEventNotifier{}

	recent.NotifyLogs(notifier, 1, 2)

	require.Len(t, notifier.logs, 1)
	require.Equal(t, uint64(123), notifier.logs[0].BlockTimestamp)
}

func TestRecentReceiptsNotifyLogsAfterUnwind(t *testing.T) {
	recent := NewRecentReceipts(3)
	oldLog := &types.Log{Data: []byte("old")}
	newLog := &types.Log{Data: []byte("new")}
	recent.Add(types.Receipts{{
		BlockNumber: uint256.NewInt(2),
		Logs:        types.Logs{oldLog},
	}}, nil, nil)
	recent.Unwind(1)
	recent.Add(types.Receipts{{
		BlockNumber: uint256.NewInt(2),
		Logs:        types.Logs{newLog},
	}}, nil, nil)
	notifier := &recordingChainEventNotifier{}

	recent.NotifyLogs(notifier, 2, 3)

	require.Len(t, notifier.logBatches, 1)
	require.Len(t, notifier.logBatches[0], 2)
	require.Same(t, oldLog, notifier.logBatches[0][0].Log)
	require.True(t, notifier.logBatches[0][0].Removed)
	require.Same(t, newLog, notifier.logBatches[0][1].Log)
	require.False(t, notifier.logBatches[0][1].Removed)
}

func TestRecentReceiptsNotifiesRemovedLogsBelowAddedRange(t *testing.T) {
	recent := NewRecentReceipts(3)
	removedLog := &types.Log{Data: []byte("removed")}
	recent.Add(types.Receipts{{
		BlockNumber: uint256.NewInt(2),
		Logs:        types.Logs{removedLog},
	}}, nil, nil)
	recent.Unwind(1)
	notifier := &recordingChainEventNotifier{}

	recent.NotifyLogs(notifier, 1_000, 1_001)

	require.Len(t, notifier.logs, 1)
	require.Same(t, removedLog, notifier.logs[0].Log)
	require.True(t, notifier.logs[0].Removed)
}

func TestRecentReceiptsBatchesRemovedBlocksInOrder(t *testing.T) {
	recent := NewRecentReceipts(3)
	for _, blockNum := range []uint64{3, 1, 2} {
		log := &types.Log{BlockNumber: hexutil.Uint64(blockNum)}
		recent.Add(types.Receipts{{
			BlockNumber: uint256.NewInt(blockNum),
			Logs:        types.Logs{log},
		}}, nil, nil)
	}
	recent.Unwind(0)
	notifier := &recordingChainEventNotifier{}

	recent.NotifyLogs(notifier, 4, 4)

	require.Len(t, notifier.logBatches, 1)
	require.Len(t, notifier.logBatches[0], 3)
	for i, blockNum := range []uint64{1, 2, 3} {
		require.Equal(t, blockNum, uint64(notifier.logBatches[0][i].Log.BlockNumber))
	}
}

func TestRecentReceiptsNotifiesAddedBlocksInOrder(t *testing.T) {
	const blockCount = 32
	recent := NewRecentReceipts(blockCount)
	tx := types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), nil)
	for blockNum := uint64(blockCount); blockNum > 0; blockNum-- {
		log := &types.Log{BlockNumber: hexutil.Uint64(blockNum)}
		recent.Add(types.Receipts{{
			BlockNumber:      uint256.NewInt(blockNum),
			TransactionIndex: 0,
			Logs:             types.Logs{log},
		}}, []types.Transaction{tx}, &types.Header{Number: *uint256.NewInt(blockNum)})
	}
	notifier := &recordingChainEventNotifier{}

	recent.NotifyLogs(notifier, 1, blockCount+1)
	recent.NotifyReceipts(notifier, 1, blockCount+1)

	require.Len(t, notifier.logBatches, 1)
	require.Len(t, notifier.logBatches[0], blockCount)
	require.Len(t, notifier.receiptBatches, 1)
	require.Len(t, notifier.receiptBatches[0], blockCount)
	for i := range blockCount {
		blockNum := uint64(i + 1)
		require.Equal(t, blockNum, uint64(notifier.logBatches[0][i].Log.BlockNumber))
		require.Equal(t, blockNum, notifier.receiptBatches[0][i].Header.Number.Uint64())
	}
}

func TestRecentReceiptsDoesNotSendEmptyLogBatch(t *testing.T) {
	recent := NewRecentReceipts(1)
	recent.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, nil, nil)
	recent.Unwind(0)
	recent.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, nil, nil)
	notifier := &recordingChainEventNotifier{}

	recent.NotifyLogs(notifier, 1, 2)

	require.Empty(t, notifier.logBatches)
}

func TestRecentReceiptsDropsUnwindWithoutSubscriptions(t *testing.T) {
	recent := NewRecentReceipts(3)
	recent.Add(types.Receipts{{
		BlockNumber: uint256.NewInt(2),
		Logs:        types.Logs{{}},
	}}, nil, nil)
	recent.Unwind(1)
	notifier := &noSubscriptionNotifier{}

	recent.NotifyLogs(notifier, 2, 3)

	require.Empty(t, recent.removedLogs)
}
