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

package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
)

func TestIsInitialCycleFromProgress(t *testing.T) {
	const ttl = uint64(1024)

	tests := []struct {
		name         string
		markerBlock  *uint64
		knownTip     uint64
		finish       uint64
		initialCycle bool
	}{
		{
			name:         "no marker",
			knownTip:     1000,
			finish:       1000,
			initialCycle: true,
		},
		{
			name:         "marker at tip",
			markerBlock:  ptr(uint64(1000)),
			knownTip:     1000,
			finish:       1000,
			initialCycle: false,
		},
		{
			name:         "marker recently behind tip",
			markerBlock:  ptr(uint64(1000)),
			knownTip:     1500,
			finish:       1500,
			initialCycle: false,
		},
		{
			name:         "marker beyond ttl",
			markerBlock:  ptr(uint64(1000)),
			knownTip:     3000,
			finish:       3000,
			initialCycle: true,
		},
		{
			name:         "finish reset",
			markerBlock:  ptr(uint64(1000)),
			knownTip:     1000,
			finish:       0,
			initialCycle: true,
		},
		{
			name:         "local progress behind marker beyond ttl",
			markerBlock:  ptr(uint64(3000)),
			knownTip:     1200,
			finish:       1200,
			initialCycle: true,
		},
		{
			name:         "local progress behind marker within ttl",
			markerBlock:  ptr(uint64(1500)),
			knownTip:     1499,
			finish:       1499,
			initialCycle: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, tx := memdb.NewTestTx(t)
			defer tx.Rollback()

			if tt.markerBlock != nil {
				require.NoError(t, rawdb.WriteTipReached(tx, *tt.markerBlock))
			}

			initialCycle, err := IsInitialCycleFromProgress(tx, tt.knownTip, tt.finish, ttl)
			require.NoError(t, err)
			require.Equal(t, tt.initialCycle, initialCycle)
		})
	}
}

func TestUpdateTipReachedFromProgress(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	require.NoError(t, UpdateTipReachedFromProgress(tx, 1000, 999))
	_, ok, err := rawdb.ReadLastTipReachedBlock(tx)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, UpdateTipReachedFromProgress(tx, 1000, 1000))
	block, ok, err := rawdb.ReadLastTipReachedBlock(tx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1000), block)

	require.NoError(t, UpdateTipReachedFromProgress(tx, 1500, 1500))
	block, ok, err = rawdb.ReadLastTipReachedBlock(tx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1500), block)

	require.NoError(t, UpdateTipReachedFromProgress(tx, 1200, 1200))
	block, ok, err = rawdb.ReadLastTipReachedBlock(tx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1500), block)
}

func TestIsInitialCycleFromProgressUsesKnownTip(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	require.NoError(t, rawdb.WriteTipReached(tx, 1000))

	initialCycle, err := IsInitialCycleFromProgress(tx, 1046, 1000, 1)
	require.NoError(t, err)
	require.True(t, initialCycle)
}

func TestIsInitialCycleFromProgressUsesMarkerAsKnownTipFloor(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	require.NoError(t, rawdb.WriteTipReached(tx, 20922434))

	initialCycle, err := IsInitialCycleFromProgress(tx, 20922400, 20922400, 1)
	require.NoError(t, err)
	require.True(t, initialCycle)
}

func TestDeleteTipReached(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	require.NoError(t, rawdb.WriteTipReached(tx, 1000))
	require.NoError(t, rawdb.DeleteTipReached(tx))

	_, ok, err := rawdb.ReadLastTipReachedBlock(tx)
	require.NoError(t, err)
	require.False(t, ok)
}

func ptr[T any](v T) *T {
	return &v
}
