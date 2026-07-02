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

func TestValidateCommitmentHistory(t *testing.T) {
	finite := FullMode // History = Distance(262_144)
	cases := []struct {
		name    string
		mode    Mode
		wantErr bool
	}{
		{"no-commitment-no-constraint", finite, false},
		{"unbounded-history-any-commitment-ok", ArchiveMode.WithCommitmentHistory(500_000), false},
		{"within-history-allowed", finite.WithCommitmentHistory(100_000), false},
		{"equal-history-allowed", finite.WithCommitmentHistory(262_144), false},
		{"exceeds-history-rejected", finite.WithCommitmentHistory(500_000), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.mode)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCommitmentHistoryRoundTrip(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	want := ArchiveMode.WithCommitmentHistory(100_000)

	got, err := EnsureNotChanged(tx, want)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	// Re-read from the DB independently.
	stored, err := Get(tx)
	require.NoError(t, err)
	assert.EqualValues(t, 100_000, stored.CommitmentHistory().toValue())

	// Restart with the same value: no-op.
	got2, err := EnsureNotChanged(tx, want)
	require.NoError(t, err)
	assert.Equal(t, want, got2)
}

func TestCommitmentHistoryShrinkAndWidenAccepted(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	_, err := EnsureNotChanged(tx, ArchiveMode.WithCommitmentHistory(200_000))
	require.NoError(t, err)

	// Shrink — accepted, persisted.
	shrunk := ArchiveMode.WithCommitmentHistory(50_000)
	got, err := EnsureNotChanged(tx, shrunk)
	require.NoError(t, err)
	assert.Equal(t, shrunk, got)

	// Widen — accepted (re-downloadable), persisted.
	widened := ArchiveMode.WithCommitmentHistory(150_000)
	got, err = EnsureNotChanged(tx, widened)
	require.NoError(t, err)
	assert.Equal(t, widened, got)

	stored, err := Get(tx)
	require.NoError(t, err)
	assert.EqualValues(t, 150_000, stored.CommitmentHistory().toValue())
}

func TestCommitmentHistoryBoundedToUnboundedDropsKey(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	_, err := EnsureNotChanged(tx, ArchiveMode.WithCommitmentHistory(50_000))
	require.NoError(t, err)

	// Widen to unbounded (category removed): accepted, and the persisted bound
	// is dropped so a later read reports unbounded.
	got, err := EnsureNotChanged(tx, ArchiveMode)
	require.NoError(t, err)
	assert.Equal(t, ArchiveMode, got)
	assert.Nil(t, got.CommitmentHistory())

	stored, err := Get(tx)
	require.NoError(t, err)
	assert.Nil(t, stored.CommitmentHistory())
}
