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

package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestDiscoverNewFiles_AddsPrimariesAtDownloaded(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"v1.0-headers.0-500.seg",
		"v1.0-accounts.0-256.kv",
		"v1.0-history.0-256.v",
		"v1.0-headers.0-500.idx",  // accessor — must be skipped
		"v1.0-accounts.0-256.kvi", // accessor — must be skipped
		"random.txt",              // unknown — must be skipped
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte{}, 0644))
	}

	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv, SnapDir: dir}
	d.Sweep(context.Background(), nil)

	for _, primary := range []string{
		"v1.0-headers.0-500.seg",
		"v1.0-accounts.0-256.kv",
		"v1.0-history.0-256.v",
	} {
		state, ok := inv.LifecycleState(primary)
		require.True(t, ok, "primary %s must be discovered", primary)
		require.Equal(t, snapshot.LifecycleDownloaded, state,
			"primary %s must land at Downloaded; got %s", primary, state)
	}

	for _, skip := range []string{
		"v1.0-headers.0-500.idx",
		"v1.0-accounts.0-256.kvi",
		"random.txt",
	} {
		_, ok := inv.LifecycleState(skip)
		require.False(t, ok, "non-primary %s must be skipped", skip)
	}
}

func TestDiscoverNewFiles_PreservesExistingState(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.seg"), []byte{}, 0644))

	inv := snapshot.NewInventory()
	// Pre-populate at Advertisable. Discover must NOT downgrade it
	// (AddFile-replace would; the discover path checks LifecycleState
	// first and skips).
	inv.AddFile(&snapshot.FileEntry{
		Name: "a.seg", Local: true, Advertisable: true,
	})

	d := &Driver{Inv: inv, SnapDir: dir}
	d.Sweep(context.Background(), nil)

	state, _ := inv.LifecycleState("a.seg")
	require.Equal(t, snapshot.LifecycleAdvertisable, state,
		"discover must not regress an entry already past Downloaded")
}

func TestDiscoverNewFiles_EmptySnapDirNoOp(t *testing.T) {
	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv} // no SnapDir
	d.Sweep(context.Background(), nil)
	// No assertion beyond "didn't panic / didn't error". Empty
	// SnapDir is the production-tooling default.
}

func TestDiscoverNewFiles_MissingDirNoOp(t *testing.T) {
	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv, SnapDir: "/does/not/exist"}
	d.Sweep(context.Background(), nil)
	// Read error → silent skip; next sweep retries.
}
