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

func TestResolveCommitmentHistorySteps(t *testing.T) {
	cases := []struct {
		name        string
		mode        string
		customSteps uint64
		want        uint64
		wantErr     bool
	}{
		{"default empty", "", 0, 0, false},
		{"archive", CommitmentHistoryArchiveMode, 0, 0, false},
		{"archive-with-custom-override", CommitmentHistoryArchiveMode, 13, 13, false},
		{"recent", CommitmentHistoryRecentMode, 0, CommitmentHistoryRecentSteps, false},
		{"medium", CommitmentHistoryMediumMode, 0, CommitmentHistoryMediumSteps, false},
		{"custom-with-value", CommitmentHistoryCustomMode, 42, 42, false},
		{"custom-without-value-rejected", CommitmentHistoryCustomMode, 0, 0, true},
		{"unknown-mode-rejected", "garbage", 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveCommitmentHistorySteps(tc.mode, tc.customSteps)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEnsureCommitmentHistoryDistanceCompatible(t *testing.T) {
	t.Run("first-set-archive", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		got, err := EnsureCommitmentHistoryDistanceCompatible(tx, 0)
		require.NoError(t, err)
		assert.EqualValues(t, 0, got)
	})

	t.Run("first-set-bounded", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		got, err := EnsureCommitmentHistoryDistanceCompatible(tx, 8)
		require.NoError(t, err)
		assert.EqualValues(t, 8, got)
		// Re-running with the same value should be a no-op.
		got2, err := EnsureCommitmentHistoryDistanceCompatible(tx, 8)
		require.NoError(t, err)
		assert.EqualValues(t, 8, got2)
	})

	t.Run("shrink-allowed", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryDistanceCompatible(tx, 16)
		require.NoError(t, err)

		got, err := EnsureCommitmentHistoryDistanceCompatible(tx, 4)
		require.NoError(t, err)
		assert.EqualValues(t, 4, got)
	})

	t.Run("expand-rejected", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryDistanceCompatible(tx, 4)
		require.NoError(t, err)

		_, err = EnsureCommitmentHistoryDistanceCompatible(tx, 16)
		require.Error(t, err)
	})

	t.Run("bounded-to-unlimited-rejected", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryDistanceCompatible(tx, 4)
		require.NoError(t, err)

		_, err = EnsureCommitmentHistoryDistanceCompatible(tx, 0)
		require.Error(t, err)
	})

	t.Run("unlimited-to-bounded-allowed", func(t *testing.T) {
		// We had everything; constraining future downloads is fine.
		_, tx := memdb.NewTestTx(t)
		_, err := EnsureCommitmentHistoryDistanceCompatible(tx, 0)
		require.NoError(t, err)

		got, err := EnsureCommitmentHistoryDistanceCompatible(tx, 8)
		require.NoError(t, err)
		assert.EqualValues(t, 8, got)
	})
}
