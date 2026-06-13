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

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// stubRwTx satisfies kv.TemporalRwTx for the purpose of being non-nil;
// no method on it is exercised because Provider.Unwind in commit 2b
// returns the not-yet-implemented error before any tx work happens.
type stubRwTx struct{ kv.TemporalRwTx }

func TestProviderUnwind_RejectsNilTx(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: nil})
	require.Error(t, err)
	require.Contains(t, err.Error(), "opts.Tx is nil")
}

func TestProviderUnwind_RejectsNilProvider(t *testing.T) {
	t.Parallel()
	var p *Provider
	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: &stubRwTx{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil provider")
}

// TestProviderUnwind_ValidationOK_ReachesSubOps pins that an aligned
// Provider with a non-nil Tx passes the precondition guards and
// proceeds into the sub-op chain. The minimum-shape Provider here
// (no Inventory, no BlockReader) makes snapshot-trim a no-op and
// fails fast inside ensureCommitmentAtBlock with the BlockReader-nil
// check — that's the next step in the chain, exactly what we want
// to pin without standing up a real harness. The full happy path
// lands with the commit-3 scenario-3 E2E test against a real
// snapshot fixture.
func TestProviderUnwind_ValidationOK_ReachesSubOps(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	err := p.Unwind(context.Background(), 1000, UnwindOpts{Tx: &stubRwTx{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "commitment-anchor")
	require.Contains(t, err.Error(), "nil BlockReader",
		"reaching commitment-anchor proves the sub-op chain is wired in; the BlockReader-nil error is the expected next-step failure on this minimum-shape fixture")
}

// touchSeg writes an empty file at <dir>/<name>; used to fabricate
// on-disk presence for findInventoryOrphansPastBlock tests without
// standing up a real .seg.
func touchSeg(t *testing.T, dir, name string) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, name))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// TestFindInventoryOrphansPastBlock_NoSnapDirOrInventory pins the
// graceful no-op: tools and tests that construct a bare Provider
// without snapDir or Inventory must return (nil, nil) — not an error,
// not a false orphan.
func TestFindInventoryOrphansPastBlock_NoSnapDirOrInventory(t *testing.T) {
	t.Parallel()
	got, err := (&Provider{}).findInventoryOrphansPastBlock(2_900_000)
	require.NoError(t, err)
	require.Nil(t, got, "bare provider returns no orphans (graceful no-op)")
}

// TestFindInventoryOrphansPastBlock_AllInventory pins the happy path:
// every on-disk file is also in Inventory, so the orphan list is
// empty.
func TestFindInventoryOrphansPastBlock_AllInventory(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	for _, name := range []string{
		"v1.1-002900-002910-headers.seg",
		"v1.1-002900-002910-bodies.seg",
		"v1.1-002910-002920-headers.seg",
	} {
		touchSeg(t, snapDir, name)
		require.NoError(t, inv.AddFile(&snapshot.FileEntry{Name: name, Local: true}))
	}
	p := &Provider{snapDir: snapDir, Inventory: inv}
	got, err := p.findInventoryOrphansPastBlock(2_800_000)
	require.NoError(t, err)
	require.Empty(t, got, "every on-disk file is in inventory; no orphans")
}

// TestFindInventoryOrphansPastBlock_OnDiskNotInInventory pins the
// regression signal: a v1.1-*.seg whose range falls past toBlock and
// is missing from Inventory must surface as an orphan. This is the
// exact wedge shape live-caught 2026-06-12 — prior session retire
// crashed mid-build, left .seg files without .torrent (no
// announcement), Provider.Unwind silently missed them in trim.
func TestFindInventoryOrphansPastBlock_OnDiskNotInInventory(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	// Known files in inventory + on disk:
	known := []string{
		"v1.1-002900-002910-headers.seg",
		"v1.1-002900-002910-bodies.seg",
		"v1.1-002900-002910-transactions.seg",
	}
	for _, name := range known {
		touchSeg(t, snapDir, name)
		require.NoError(t, inv.AddFile(&snapshot.FileEntry{Name: name, Local: true}))
	}
	// Orphans on disk but missing from inventory — the wedge shape:
	orphans := []string{
		"v1.1-003000-003001-headers.seg",
		"v1.1-003000-003001-bodies.seg",
		"v1.1-003000-003001-transactions.seg",
	}
	for _, name := range orphans {
		touchSeg(t, snapDir, name)
	}
	p := &Provider{snapDir: snapDir, Inventory: inv}
	got, err := p.findInventoryOrphansPastBlock(2_900_000)
	require.NoError(t, err)
	// Sort-stable expectation per the function contract (sort.Strings).
	require.Equal(t, []string{
		"v1.1-003000-003001-bodies.seg",
		"v1.1-003000-003001-headers.seg",
		"v1.1-003000-003001-transactions.seg",
	}, got)
}

// TestFindInventoryOrphansPastBlock_OnlyPastToBlock pins the toBlock
// gate: only files whose FromBlock is strictly past toBlock are
// reported. A pre-toBlock file with no inventory entry is some other
// problem (stale entry from a prior advertisement, future deletion
// target — not Provider.Unwind's concern at this layer).
func TestFindInventoryOrphansPastBlock_OnlyPastToBlock(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	// On disk: one in-inventory file (covers the toBlock range) +
	// one orphan past toBlock + one orphan before toBlock (must be ignored).
	touchSeg(t, snapDir, "v1.1-002800-002900-headers.seg")
	require.NoError(t, inv.AddFile(&snapshot.FileEntry{Name: "v1.1-002800-002900-headers.seg", Local: true}))
	touchSeg(t, snapDir, "v1.1-002700-002800-headers.seg") // pre-toBlock orphan
	touchSeg(t, snapDir, "v1.1-002900-002910-headers.seg") // past-toBlock orphan
	p := &Provider{snapDir: snapDir, Inventory: inv}
	got, err := p.findInventoryOrphansPastBlock(2_899_999)
	require.NoError(t, err)
	require.Equal(t, []string{"v1.1-002900-002910-headers.seg"}, got,
		"only orphans strictly past toBlock count; pre-toBlock orphans are out of scope")
}

// TestFindInventoryOrphansPastBlock_IgnoresNonBlockFiles pins that the
// scan filters by v1.1-* prefix and .seg suffix — state-aggregator
// files (v2.0-*) live in subdirs and never reach the top-level scan,
// but defensively the prefix check excludes any v2.0-* that landed
// at the top level. Random non-snap files are also ignored.
func TestFindInventoryOrphansPastBlock_IgnoresNonBlockFiles(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	touchSeg(t, snapDir, "v2.0-stray-state-file.seg") // wrong prefix
	touchSeg(t, snapDir, "chain.toml")                // wrong extension
	touchSeg(t, snapDir, "v1.1-002900-002910-headers.seg.torrent") // wrong extension
	p := &Provider{snapDir: snapDir, Inventory: inv}
	got, err := p.findInventoryOrphansPastBlock(0)
	require.NoError(t, err)
	require.Empty(t, got, "non-block-snapshot files must not be reported as orphans")
}
