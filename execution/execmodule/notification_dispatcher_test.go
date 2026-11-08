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

	"github.com/erigontech/erigon/common/log/v3"
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
	dispatcher := NewDispatcher(chain.AllProtocolChanges, nil, capture, log.New())

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
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, nil, 1, 1, &unwindPoint))

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
	dispatcher := NewDispatcher(chain.AllProtocolChanges, events, nil, log.New())
	unwindPoint := uint64(0)

	require.NoError(t, dispatcher.Dispatch(t.Context(), tx, 0, nil, nil, 2_000, 1_500, &unwindPoint))

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
