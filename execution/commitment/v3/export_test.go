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

package v3

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func PartitionWipeCase(t *testing.T, wipe bool, fill func(*commitment.Updates)) {
	t.Helper()
	updates := testUpdates(t, commitment.ModeUpdate, nil)
	fill(updates)
	p := &partitioner{}
	require.NoError(t, updates.HashSort(context.Background(), nil, func(hashedKey, _ []byte, update *commitment.Update) error {
		return p.add(bytes.Clone(hashedKey), update)
	}))
	storage, accounts := p.done()
	require.Len(t, accounts, 1)
	require.Equal(t, wipe, accounts[0].update.Deleted())
	if !wipe {
		require.Empty(t, storage)
		return
	}
	require.Len(t, storage, 1)
	require.True(t, storage[0].wipe)
	ctx := newMockContext()
	storageRound(t, ctx, storage[0].addrHash, []commitmenttest.Op{{Key: commitmenttest.Path([]byte{1}, 2), Storage: []byte{1}}})
	_, err := runStorageTask(ctx, storage[0])
	require.NoError(t, err)
	require.Contains(t, ctx.branches, string(StorageNodeKey(storage[0].addrHash, nil, nil)))
	require.Empty(t, ctx.branches[string(StorageNodeKey(storage[0].addrHash, nil, nil))])
}
