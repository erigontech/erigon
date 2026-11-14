// Copyright 2026 The Erigon Authors
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

package execmodule

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

type stateChangesCapture struct {
	batch *remoteproto.StateChangeBatch
}

func (c *stateChangesCapture) SendStateChanges(_ context.Context, batch *remoteproto.StateChangeBatch) {
	c.batch = batch
}

func TestDispatcherUsesSuppliedStateVersion(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{
		Number:   *uint256.NewInt(1),
		GasLimit: 30_000_000,
		BaseFee:  uint256.NewInt(1_000_000_000),
	}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))

	accumulator := notifications.NewAccumulator()
	accumulator.StartChange(header, nil, false)
	capture := new(stateChangesCapture)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, nil, capture, nil, log.New())

	const projectedStateVersion = uint64(7)
	require.NoError(t, dispatcher.Dispatch(
		t.Context(),
		tx,
		projectedStateVersion,
		accumulator,
		nil,
		0,
		1,
		nil,
		nil,
	))
	require.NotNil(t, capture.batch)
	require.Equal(t, projectedStateVersion, capture.batch.StateVersionId)
}

func TestDispatcherNotifiesAfterUnwindToGenesis(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	header := &types.Header{Number: *uint256.NewInt(1)}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	events := shards.NewEvents()
	headers, unsubscribe := events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, nil, 1, 1, &unwindPoint, nil))

	select {
	case got := <-headers:
		require.Len(t, got, 1)
	default:
		t.Fatal("header after genesis unwind was not announced")
	}
}

func TestDispatcherCapsHeadersAfterDeepUnwind(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	for blockNum := uint64(1); blockNum <= 1_500; blockNum++ {
		header := &types.Header{Number: *uint256.NewInt(blockNum)}
		require.NoError(t, rawdb.WriteHeader(tx, header))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), blockNum))
	}
	events := shards.NewEvents()
	headers, unsubscribe := events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, nil, 2_000, 1_500, &unwindPoint, nil))

	select {
	case got := <-headers:
		require.Len(t, got, 1_024)
		var first, last types.Header
		require.NoError(t, rlp.DecodeBytes(got[0], &first))
		require.NoError(t, rlp.DecodeBytes(got[len(got)-1], &last))
		require.Equal(t, uint64(477), first.Number.Uint64())
		require.Equal(t, uint64(1_500), last.Number.Uint64())
	default:
		t.Fatal("headers after deep unwind were not announced")
	}
}

func oneLogBlockReceipts(blockNum uint64) notifications.BlockReceipts {
	header := &types.Header{Number: *uint256.NewInt(blockNum)}
	l := &types.Log{BlockNumber: hexutil.Uint64(blockNum)}
	receipt := &types.Receipt{BlockNumber: uint256.NewInt(blockNum), Logs: types.Logs{l}}
	return notifications.BlockReceipts{Receipts: types.Receipts{receipt}, Txs: []types.Transaction{types.NewTransaction(blockNum, common.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), nil)}, Header: header}
}

func loadOneLogBlockReceipts(_ context.Context, _ kv.TemporalTx, blockNum uint64) (notifications.BlockReceipts, error) {
	return oneLogBlockReceipts(blockNum), nil
}

func writeHeadersAndFillCache(t *testing.T, tx kv.RwTx, recent *notifications.RecentReceipts, blocks uint64) {
	t.Helper()
	for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
		b := oneLogBlockReceipts(blockNum)
		require.NoError(t, rawdb.WriteHeader(tx, b.Header))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, b.Header.Hash(), blockNum))
		recent.Add(b.Receipts, b.Txs, b.Header)
	}
}

func TestDispatcherDeliversLogsOfBlocksMissingFromTheCache(t *testing.T) {
	const blocks = notifications.MaxNotifiedBlocks
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	recent.Clear()
	events := shards.NewEvents()
	logs, unsubscribe := events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyLogSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, loadOneLogBlockReceipts, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, recent, 0, blocks, nil, tx))

	select {
	case got := <-logs:
		require.Len(t, got, blocks, "every block's log is delivered")
		for i, l := range got {
			require.Equal(t, uint64(i+1), uint64(l.BlockNumber), "in block order")
		}
	default:
		t.Fatal("no logs delivered")
	}
}

func TestDispatcherDeliversReceiptsOfBlocksMissingFromTheCache(t *testing.T) {
	const blocks = notifications.MaxNotifiedBlocks
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	recent.Clear()
	events := shards.NewEvents()
	receipts, unsubscribe := events.AddReceiptsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyReceiptSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, loadOneLogBlockReceipts, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, recent, 0, blocks, nil, tx))

	select {
	case got := <-receipts:
		require.Len(t, got, blocks, "every block's receipt is delivered")
		for i, r := range got {
			blockNum := uint64(i + 1)
			require.Equal(t, blockNum, r.Receipt.BlockNumber.Uint64(), "in block order")
			require.Equal(t, blockNum, r.Header.Number.Uint64(), "with the block's header")
			require.Equal(t, blockNum, r.Tx.GetNonce(), "with the block's transaction")
		}
	default:
		t.Fatal("no receipts delivered")
	}
}

func TestDispatcherSendsRemovedLogsOfBlocksLoadedAfterACacheMiss(t *testing.T) {
	const blocks = 10
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	recent.Clear()
	events := shards.NewEvents()
	logs, unsubscribe := events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyLogSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, loadOneLogBlockReceipts, log.New())
	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, recent, 0, blocks, nil, tx))
	require.Len(t, <-logs, blocks)

	recent.Unwind(0)
	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, recent, blocks, blocks, nil, nil))

	select {
	case got := <-logs:
		require.Len(t, got, blocks, "every loaded block's log is removed")
		for _, l := range got {
			require.True(t, l.Removed)
		}
	default:
		t.Fatal("no removed logs delivered")
	}
}

// The Engine API path has no committed tx, so the cache alone must cover the longest notify span.
func TestDispatcherForkchoicePathDeliversLogsOfTheWholeNotifySpan(t *testing.T) {
	const blocks = 1_500
	_, tx := temporaltest.NewTestTx(t)
	recent := shards.NewNotifications(nil).RecentReceipts
	writeHeadersAndFillCache(t, tx, recent, blocks)
	events := shards.NewEvents()
	logs, unsubscribe := events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	events.EmptyLogSubscription(false)
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, nil, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, recent, 0, blocks, nil, nil))

	select {
	case got := <-logs:
		require.Len(t, got, 1_024, "the whole notify span is delivered")
		require.Equal(t, uint64(blocks-1_024+1), uint64(got[0].BlockNumber))
		require.Equal(t, uint64(blocks), uint64(got[len(got)-1].BlockNumber))
	default:
		t.Fatal("no logs delivered")
	}
}

func TestDispatcherKeepsBlocksAboveFinalizedWithoutSubscribers(t *testing.T) {
	const blocks = 3
	_, tx := temporaltest.NewTestTx(t)
	recent := notifications.NewRecentReceipts(notifications.MaxNotifiedBlocks)
	writeHeadersAndFillCache(t, tx, recent, blocks)
	rawdb.WriteForkchoiceFinalized(tx, oneLogBlockReceipts(2).Header.Hash())
	dispatcher := NewDispatcher(chain.AllProtocolChanges, shards.NewEvents(), nil, nil, log.New())

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, recent, 0, blocks, nil, nil))

	require.Equal(t, []uint64{1, 2}, recent.Missing(1, blocks+1))
}
