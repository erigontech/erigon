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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// These tests pin the self-heal behaviour added 2026-06-19 — when
// Provider.Unwind detects v1.1-*.seg files on disk past toBlock that
// aren't tracked by Inventory, it adds them rather than refusing.
//
// The fix replaces an earlier "refuse setHead" failure mode that
// blocked the mode-B soak iter 4 (depth 60k) on every run:
// SyncSnapshots' OtterSync re-download path (RequestSnapshotsDownload
// over gRPC) fetches preverified 1k-stub files after every mode-B
// unwind, and those subsumed-by-merged-sibling files never fire
// flow.DownloadComplete on the bus, so the Inventory never learns
// of them.
//
// Self-heal is bounded: only files matching the orphan filter
// (v1.1-*.seg on disk past toBlock, missing from Inventory) get
// added. Files already in Inventory, files below toBlock, and
// non-v1.1 files are untouched.

// TestHealInventoryOrphansPastBlock_AddsMissingFiles is the headline
// case: 3 orphans on disk past toBlock, none in Inventory → all 3
// land in Inventory with FromBlock/ToBlock parsed from the name so
// collectFilesPastBlock's range filter can classify them.
func TestHealInventoryOrphansPastBlock_AddsMissingFiles(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()

	// Pre-existing in-inventory file (covers blocks 2,800,000-2,900,000)
	// — must NOT be re-added or re-touched.
	known := "v1.1-002800-002900-headers.seg"
	touchSeg(t, snapDir, known)
	require.NoError(t, inv.AddFile(&snapshot.FileEntry{Name: known, Local: true, Advertisable: true}))

	// Orphans on disk past toBlock=2,950,000 — the wedge shape.
	// Modeled on the actual v15/v17/v18 reproducer: v1.1-002990-002991.
	orphans := []string{
		"v1.1-002990-002991-bodies.seg",
		"v1.1-002990-002991-headers.seg",
		"v1.1-002990-002991-transactions.seg",
	}
	for _, name := range orphans {
		touchSeg(t, snapDir, name)
	}

	p := &Provider{snapDir: snapDir, Inventory: inv}
	require.NoError(t, p.healInventoryOrphansPastBlock(2_950_000))

	// Every orphan landed in Inventory with parsed block range.
	gotNames := make([]string, 0)
	for _, e := range inv.BlockFiles() {
		gotNames = append(gotNames, e.Name)
	}
	sort.Strings(gotNames)
	require.Equal(t, []string{
		known,
		"v1.1-002990-002991-bodies.seg",
		"v1.1-002990-002991-headers.seg",
		"v1.1-002990-002991-transactions.seg",
	}, gotNames)

	for _, e := range inv.BlockFiles() {
		if e.Name == known {
			continue
		}
		require.True(t, e.Local, "orphan %s must land with Local=true", e.Name)
		require.True(t, e.Advertisable, "orphan %s must land with Advertisable=true", e.Name)
		require.Equal(t, uint64(2_990_000), e.FromBlock,
			"orphan %s must carry FromBlock parsed from name; collectFilesPastBlock relies on it", e.Name)
		require.Equal(t, uint64(2_991_000), e.ToBlock,
			"orphan %s must carry ToBlock parsed from name", e.Name)
	}
}

// TestHealInventoryOrphansPastBlock_NoOrphansIsNoop pins that when
// every on-disk file is already in Inventory, healing is a clean
// no-op (no spurious AddFile calls, no error).
func TestHealInventoryOrphansPastBlock_NoOrphansIsNoop(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	for _, name := range []string{
		"v1.1-002900-002910-headers.seg",
		"v1.1-002900-002910-bodies.seg",
	} {
		touchSeg(t, snapDir, name)
		require.NoError(t, inv.AddFile(&snapshot.FileEntry{Name: name, Local: true}))
	}
	before := len(inv.BlockFiles())

	p := &Provider{snapDir: snapDir, Inventory: inv}
	require.NoError(t, p.healInventoryOrphansPastBlock(2_800_000))

	require.Equal(t, before, len(inv.BlockFiles()),
		"no orphans → no new Inventory entries")
}

// TestHealInventoryOrphansPastBlock_OnlyPastToBlock pins the toBlock
// scope: only files whose FromBlock > toBlock get healed. Pre-toBlock
// files on disk but missing from Inventory are out of scope here
// (handled by other repair paths, or simply preserved for the
// straddle-rebuild logic). Matches findInventoryOrphansPastBlock's
// own contract.
func TestHealInventoryOrphansPastBlock_OnlyPastToBlock(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()

	// On disk: one in-inventory file at the toBlock range + one
	// orphan past + one orphan before. Only the past-orphan heals.
	touchSeg(t, snapDir, "v1.1-002800-002900-headers.seg")
	require.NoError(t, inv.AddFile(&snapshot.FileEntry{Name: "v1.1-002800-002900-headers.seg", Local: true}))
	touchSeg(t, snapDir, "v1.1-002700-002800-headers.seg") // pre-toBlock orphan — must stay out of Inventory here
	touchSeg(t, snapDir, "v1.1-002900-002910-headers.seg") // past-toBlock orphan — must land in Inventory

	p := &Provider{snapDir: snapDir, Inventory: inv}
	require.NoError(t, p.healInventoryOrphansPastBlock(2_899_999))

	gotNames := make([]string, 0)
	for _, e := range inv.BlockFiles() {
		gotNames = append(gotNames, e.Name)
	}
	sort.Strings(gotNames)
	require.Equal(t, []string{
		"v1.1-002800-002900-headers.seg",
		"v1.1-002900-002910-headers.seg",
	}, gotNames,
		"only the past-toBlock orphan must heal; pre-toBlock orphan is out of scope for Provider.Unwind preflight")
}

// TestHealInventoryOrphansPastBlock_NilInventoryIsNoop pins the
// graceful no-op for tools/tests that construct a bare Provider
// without an Inventory.
func TestHealInventoryOrphansPastBlock_NilInventoryIsNoop(t *testing.T) {
	t.Parallel()
	snapDir := t.TempDir()
	touchSeg(t, snapDir, "v1.1-002990-002991-headers.seg")

	p := &Provider{snapDir: snapDir, Inventory: nil}
	require.NoError(t, p.healInventoryOrphansPastBlock(2_900_000),
		"nil Inventory must be a clean no-op; findInventoryOrphansPastBlock returns (nil,nil) on this shape")
}
