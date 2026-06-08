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
		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 0, true)
		require.NoError(t, err)
		assert.EqualValues(t, 0, got)
	})

	t.Run("first-set-bounded", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 100_000, true)
		require.NoError(t, err)
		assert.EqualValues(t, 100_000, got)
		// Re-running with the same value should be a no-op.
		got2, err := EnsureCommitmentHistoryOlderCompatible(tx, 100_000, true)
		require.NoError(t, err)
		assert.EqualValues(t, 100_000, got2)
	})

	t.Run("shrink-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 200_000, true)
		require.NoError(t, err)

		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 50_000, true)
		require.NoError(t, err)
		assert.EqualValues(t, 50_000, got)
	})

	t.Run("expand-rejected", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 50_000, true)
		require.NoError(t, err)

		_, err = EnsureCommitmentHistoryOlderCompatible(tx, 200_000, true)
		require.Error(t, err)
	})

	t.Run("bounded-to-unlimited-rejected", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 50_000, true)
		require.NoError(t, err)

		_, err = EnsureCommitmentHistoryOlderCompatible(tx, 0, true)
		require.Error(t, err)
	})

	t.Run("unlimited-to-bounded-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 0, true)
		require.NoError(t, err)

		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 100_000, true)
		require.NoError(t, err)
		assert.EqualValues(t, 100_000, got)
	})

	// Operator restarts without passing the flag: configuredBlocks defaults to
	// 0 and configured=false. The persisted bound must stand instead of being
	// read as an expand-to-unlimited request that rejects startup.
	t.Run("unset-honors-stored", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryOlderCompatible(tx, 100_000, true)
		require.NoError(t, err)

		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 0, false)
		require.NoError(t, err)
		assert.EqualValues(t, 100_000, got)
	})

	// First run with the flag absent stays unlimited and persists nothing, so a
	// later explicit bound is a clean first-set rather than a 0 -> N transition.
	t.Run("unset-first-run-stays-unlimited", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		got, err := EnsureCommitmentHistoryOlderCompatible(tx, 0, false)
		require.NoError(t, err)
		assert.EqualValues(t, 0, got)

		_, ok, err := getCommitmentHistoryOlder(tx)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
