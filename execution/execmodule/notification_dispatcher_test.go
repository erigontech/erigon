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

func TestDispatcherDeliversLogsOfBlocksMissingFromTheCache(t *testing.T) {
	const blocks = 600 // more than the cache holds
	blockReceipts := func(blockNum uint64) notifications.BlockReceipts {
		header := &types.Header{Number: *uint256.NewInt(blockNum)}
		l := &types.Log{BlockNumber: hexutil.Uint64(blockNum)}
		receipt := &types.Receipt{BlockNumber: uint256.NewInt(blockNum), Logs: types.Logs{l}}
		return notifications.BlockReceipts{Receipts: types.Receipts{receipt}, Txs: []types.Transaction{types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 0, uint256.NewInt(0), nil)}, Header: header}
	}
	for _, tc := range []struct {
		name    string
		prepare func(*notifications.RecentReceipts)
	}{
		{"span over the cache", func(*notifications.RecentReceipts) {}},
		{"cleared cache", (*notifications.RecentReceipts).Clear},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, tx := temporaltest.NewTestTx(t)
			recent := notifications.NewRecentReceipts(512)
			for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
				b := blockReceipts(blockNum)
				require.NoError(t, rawdb.WriteHeader(tx, b.Header))
				require.NoError(t, rawdb.WriteCanonicalHash(tx, b.Header.Hash(), blockNum))
				recent.Add(b.Receipts, b.Txs, b.Header)
			}
			tc.prepare(recent)
			events := shards.NewEvents()
			logs, unsubscribe := events.AddLogsSubscription()
			t.Cleanup(unsubscribe)
			events.EmptyLogSubscription(false)
			load := func(_ context.Context, _ kv.TemporalTx, blockNum uint64) (notifications.BlockReceipts, error) {
				return blockReceipts(blockNum), nil
			}
			dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, load, log.New())

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
		})
	}
}
