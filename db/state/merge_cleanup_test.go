// Copyright 2024 The Erigon Authors
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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
)

// TestCleanAfterMerge_UnlinksSubsumedFiles guards the "last reader removes the file" invariant.
// deleteMergeFile no longer unlinks files eagerly, so cleanup must hold a rotx pinning the
// subsumed files; otherwise their FDs stay open and the files linger on disk. POSIX hides this
// (unlink-while-open succeeds) but Windows locks open files, so the check is on-disk presence.
func TestCleanAfterMerge_UnlinksSubsumedFiles(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)
	_, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()

	// 0-1 and 1-2 are proper subsets of 0-2; no external reader pins them.
	ranges := []testFileRange{{0, 1}, {1, 2}, {0, 2}}
	generateAccountsFile(t, dirs, ranges)
	generateStorageFile(t, dirs, ranges)
	generateCodeFile(t, dirs, ranges)
	generateCommitmentFile(t, dirs, ranges)
	require.NoError(t, agg.OpenFolder())

	require.NoError(t, agg.RemoveOverlapsAfterMerge(t.Context()))

	files, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	require.NoError(t, err)
	var leaked []string
	for _, f := range files {
		if strings.Contains(f, ".0-1.") || strings.Contains(f, ".1-2.") {
			leaked = append(leaked, f)
		}
	}
	require.Empty(t, leaked, "subsumed files must be unlinked from disk, got leaked: %v", leaked)
}
