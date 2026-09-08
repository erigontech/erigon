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
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
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
