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

package prune

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestEnsureCommitmentHistoryOlderCompatible(t *testing.T) {
	t.Run("first-set-unlimited", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 0)
		require.NoError(t, err)
		assert.EqualValues(t, 0, got)
	})

	t.Run("first-set-bounded", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 100_000)
		require.NoError(t, err)
		assert.EqualValues(t, 100_000, got)
		// Re-running with the same value should be a no-op.
		got2, err := EnsureCommitmentHistoryOlderCompatible(tx, 100_000)
		require.NoError(t, err)
		assert.EqualValues(t, 100_000, got2)
	})

	t.Run("shrink-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 200_000)
		require.NoError(t, err)

		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 50_000)
		require.NoError(t, err)
		assert.EqualValues(t, 50_000, got)
	})

	t.Run("expand-rejected", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 50_000)
		require.NoError(t, err)

		_, err = EnsureCommitmentHistoryOlderCompatible(tx, 200_000)
		require.Error(t, err)
	})

	t.Run("bounded-to-unlimited-rejected", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 50_000)
		require.NoError(t, err)

		_, err = EnsureCommitmentHistoryOlderCompatible(tx, 0)
		require.Error(t, err)
	})

	t.Run("unlimited-to-bounded-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 0)
		require.NoError(t, err)

		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 100_000)
		require.NoError(t, err)
		assert.EqualValues(t, 100_000, got)
	})
}
