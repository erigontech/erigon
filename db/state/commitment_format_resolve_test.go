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

package state

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
)

func dirsWithCommitmentFiles(t *testing.T, names ...string) datadir.Dirs {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0755))
	for _, name := range names {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), nil, 0644))
	}
	return dirs
}

// The record format belongs to the datadir. A v2 datadir opened by a v3-default build must stay
// v2, or readCommitmentRecords skips every one of its files and the trie reads as empty.
func TestCommitmentFormatFollowsExistingFiles(t *testing.T) {
	t.Parallel()

	legacy := dirsWithCommitmentFiles(t, "v2.2-commitment.0-16.kv", "v2.2-commitment.16-17.kv")
	got, err := ResolveCommitmentEdgeRecords(legacy, true, nil)
	require.NoError(t, err)
	require.False(t, got, "v2 files must not be read as edge records just because this build defaults to v3")

	edge := dirsWithCommitmentFiles(t, "v3.0-commitment.0-16.kv")
	got, err = ResolveCommitmentEdgeRecords(edge, false, nil)
	require.NoError(t, err)
	require.True(t, got, "v3 files must not be read as bundled rows just because this build defaults to v2")
}

func TestCommitmentFormatFreshDatadirTakesTheDefault(t *testing.T) {
	t.Parallel()

	fresh := dirsWithCommitmentFiles(t)
	for _, want := range []bool{true, false} {
		got, err := ResolveCommitmentEdgeRecords(fresh, want, nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestCommitmentFormatRejectsMixedDatadir(t *testing.T) {
	t.Parallel()

	mixed := dirsWithCommitmentFiles(t, "v2.2-commitment.0-16.kv", "v3.0-commitment.16-17.kv")
	_, err := ResolveCommitmentEdgeRecords(mixed, true, nil)
	require.Error(t, err, "a datadir carrying both encodings must fail loudly, not pick one")
	require.Contains(t, err.Error(), "both commitment record formats")
}
