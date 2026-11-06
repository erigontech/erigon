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

package scenarios

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Invalidation discards a primary together with the accessors built from it,
// which is only safe if the set comes back: the first attempt at discarding by
// set stranded 82 primaries because nothing rebuilt what it removed. This is
// that interlock — the whole set gone, the primary re-downloaded, and the
// accessor expected to return without anyone asking for it.
func TestAtomicSet_DiscardedAccessorComesBack(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	snapDir := t.TempDir()

	primary := "v1.1-025020-025030-headers.seg"
	accessor := "v2.0-025020-025030-headers.idx"
	builder := &countingBuilder{
		snapDir:   snapDir,
		accessors: map[string]string{primary: accessor},
	}
	_, inv := buildIndexingStack(t, ctx, snapDir, builder)

	primaryPath := filepath.Join(snapDir, primary)
	accessorPath := filepath.Join(snapDir, accessor)
	require.NoError(t, os.WriteFile(primaryPath, []byte("gen-a"), 0o644))

	requireIndexed := func() {
		t.Helper()
		require.Eventually(t, func() bool {
			state, ok := inv.LifecycleState(primary)
			return ok && state >= snapshot.LifecycleIndexed
		}, 5*time.Second, 25*time.Millisecond, "primary never reached Indexed")
	}
	requireIndexed()
	require.FileExists(t, accessorPath)

	// Invalidation: the primary and everything built from its bytes go together.
	require.NoError(t, dir.RemoveFile(primaryPath))
	require.NoError(t, dir.RemoveFile(accessorPath))
	require.Eventually(t, func() bool {
		_, ok := inv.LifecycleState(primary)
		return !ok
	}, 5*time.Second, 25*time.Millisecond, "inventory kept an entry for a file that is gone")

	// The replacement lands, and the accessor must return with no further prompt.
	require.NoError(t, os.WriteFile(primaryPath, []byte("gen-b"), 0o644))
	requireIndexed()
	require.FileExists(t, accessorPath,
		"discarding a set is only safe if the set comes back")
}
