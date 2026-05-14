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

// TestDiscoverNewFiles_PicksUpConfigAndSubdirs pins the bug-M contract.
// Before the fix, discoverNewFiles filtered top-level entries by
// snapshotPrimaryExts (.seg/.kv/.v/.ef only) AND only scanned the
// top-level dir. As a consequence:
//   - salt-*.txt and erigondb.toml at top-level were silently skipped
//     because their extensions don't match the primary-ext whitelist —
//     so the publisher's inventory had no salt or meta entries
//     → GenerateV2 emitted empty Salt/Meta sections in chain.toml
//     → consumers couldn't fetch them → ReloadSalt failed on exec start.
//   - State primaries in domain/, history/, idx/, accessor/ were never
//     enumerated.
//   - caplin/*.seg in the caplin/ subdir was never enumerated.
//
// This test exercises the full production-layout fan-out to lock in
// the fix.
func TestDiscoverNewFiles_PicksUpConfigAndSubdirs(t *testing.T) {
	dir := t.TempDir()

	// Mirror Erigon's canonical on-disk snapshot layout. Each file is
	// empty bytes — discover only checks names + presence. Accessor
	// files (.kvi/.kvei/.bt in domain/, .vi in accessor/) are NOT
	// distributed snapshots — they're built locally — so the discover
	// path intentionally skips them. They're omitted here.
	for _, rel := range []string{
		// Top-level non-extension-matching config files (bug M direct):
		"salt-blocks.txt",
		"salt-state.txt",
		"erigondb.toml",
		// Top-level block files (already worked pre-bug-M, included
		// to confirm the rewrite didn't regress them):
		"v1.0-000000-000500-headers.seg",
		"v1.0-000000-000500-bodies.seg",
		// State-domain subdirs (bug M direct: pre-fix, these were
		// never discovered because discover only scanned top-level):
		"domain/v1.0-accounts.0-256.kv",
		"history/v1.0-accountsHistory.0-256.v",
		"idx/v1.0-accountsIdx.0-256.ef",
		// Caplin subdir (bug M direct):
		"caplin/v1.1-000000-000010-beaconblocks.seg",
	} {
		path := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte{}, 0644))
	}

	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv, SnapDir: dir}
	d.Sweep(context.Background(), nil)

	// Inventory keys subdir entries with the subdir-prefixed path
	// (e.g. "domain/v1.0-accounts.0-256.kv"); top-level entries get
	// the bare basename. The disk-scan and the aggregator's
	// onFilesChange callback both use this convention so the keyspace
	// stays consistent.
	for _, key := range []string{
		"salt-blocks.txt",
		"salt-state.txt",
		"erigondb.toml",
		"v1.0-000000-000500-headers.seg",
		"v1.0-000000-000500-bodies.seg",
		"domain/v1.0-accounts.0-256.kv",
		"history/v1.0-accountsHistory.0-256.v",
		"idx/v1.0-accountsIdx.0-256.ef",
		"caplin/v1.1-000000-000010-beaconblocks.seg",
	} {
		state, ok := inv.LifecycleState(key)
		require.Truef(t, ok, "must be discovered: %s", key)
		require.Equalf(t, snapshot.LifecycleDownloaded, state,
			"must land at Downloaded: %s (got %s)", key, state)
	}
}
