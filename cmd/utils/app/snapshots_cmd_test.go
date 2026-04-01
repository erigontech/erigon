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

package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/stretchr/testify/require"
)

type bundle struct {
	domain, history, ii state.SnapNameSchema
}

type RootNum = kv.RootNum

func Test_DeleteLatestStateSnaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	for _, dc := range []statecfg.DomainCfg{statecfg.Schema.AccountsDomain, statecfg.Schema.StorageDomain, statecfg.Schema.CodeDomain, statecfg.Schema.ReceiptDomain} {
		b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 10)
		for i := 0; i < 10; i++ {
			createFiles(t, dirs, i*10, (i+1)*10, &b)
		}
	}

	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(statecfg.Schema.ReceiptDomain, dirs, 10)

	file, _ := b.domain.DataFile(version.V1_0, 90, 100)
	confirmExist(t, file)

	// delete 9-10
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)
	file, _ = b.domain.DataFile(version.V1_0, 90, 100)
	confirmDoesntExist(t, file)

	// should delete 8-9
	err = DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)
	file, _ = b.domain.DataFile(version.V1_0, 80, 90)
	confirmDoesntExist(t, file)
}

func Test_DeleteLatestStateSnaps_DomainWithLargeRange(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, _ = state.SnapSchemaFromDomainCfg(dc, dirs, 10)

	for i := 0; i < 9; i++ {
		createSchemaFiles(t, b.history, i*10, (i+1)*10)
	}
	createSchemaFiles(t, b.domain, 0, 100)

	domainFile, _ := b.domain.DataFile(version.V1_0, 0, 100)
	confirmExist(t, domainFile)

	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)
	confirmDoesntExist(t, domainFile)
}

func Test_DeleteLatestStateSnaps_DomainAndHistorySameEnd(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, _ = state.SnapSchemaFromDomainCfg(dc, dirs, 10)

	for i := 0; i < 4; i++ {
		createSchemaFiles(t, b.history, i*10, (i+1)*10)
	}
	createSchemaFiles(t, b.domain, 0, 40)

	historyFile, _ := b.history.DataFile(version.V1_0, 30, 40)
	domainFile, _ := b.domain.DataFile(version.V1_0, 0, 40)
	confirmExist(t, historyFile)
	confirmExist(t, domainFile)

	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)
	confirmDoesntExist(t, historyFile)
	confirmDoesntExist(t, domainFile)
}

func createSchemaFiles(t *testing.T, schema state.SnapNameSchema, from, to int) {
	t.Helper()
	rootFrom, rootTo := RootNum(from), RootNum(to)
	touchFile := func(filepath string) {
		file, err := os.OpenFile(filepath, os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		file.Close()
	}
	file, _ := schema.DataFile(version.V1_0, rootFrom, rootTo)
	touchFile(file)
	acc := schema.AccessorList()
	if acc.Has(statecfg.AccessorBTree) {
		file, _ := schema.BtIdxFile(version.V1_0, rootFrom, rootTo)
		touchFile(file)
	}
	if acc.Has(statecfg.AccessorExistence) {
		file, _ := schema.ExistenceFile(version.V1_0, rootFrom, rootTo)
		touchFile(file)
	}
	if acc.Has(statecfg.AccessorHashMap) {
		file, _ := schema.AccessorIdxFile(version.V1_0, rootFrom, rootTo, 0)
		touchFile(file)
	}
}

func confirmExist(t *testing.T, filename string) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Errorf("file %s does not exist", filename)
	}
}

func confirmDoesntExist(t *testing.T, filename string) {
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Errorf("file %s exists", filename)
	}
}

func createFiles(t *testing.T, dirs datadir.Dirs, from, to int, b *bundle) {
	t.Helper()
	createSchemaFiles(t, b.domain, from, to)
	if b.history != nil {
		createSchemaFiles(t, b.history, from, to)
	}
	if b.ii != nil {
		createSchemaFiles(t, b.ii, from, to)
	}
}

func Test_DeleteStateSnaps_RemovesTmpFiles(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	// Create some normal state snapshot files so DeleteStateSnapshots has something to process
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 10)
	for i := 0; i < 3; i++ {
		createFiles(t, dirs, i*10, (i+1)*10, &b)
	}

	touchFile := func(path string) {
		f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
		require.NoError(t, err)
		f.Close()
	}

	// SnapForkable is not auto-created by datadir.New, so create it manually
	require.NoError(t, os.MkdirAll(dirs.SnapForkable, 0755))

	// Create .tmp files in all snapshot directories (matching patterns from issue #18789)
	tmpFiles := []string{
		filepath.Join(dirs.Snap, "v1.1-headers.0-500.seg.123456.tmp"),
		filepath.Join(dirs.SnapDomain, "v1.1-commitment.8272-8280.kv.252752124.tmp"),
		filepath.Join(dirs.SnapHistory, "v2.0-commitment.8256-8272.kvi.857462302.tmp"),
		filepath.Join(dirs.SnapIdx, "v1.1-storage.8256-8288.bt.209594880.tmp"),
		filepath.Join(dirs.SnapAccessors, "v2.0-commitment.8256-8272.kvi.3646922560.existence.tmp"),
		filepath.Join(dirs.SnapCaplin, "v1.0-beaconblocks.0-100.seg.999999.tmp"),
		filepath.Join(dirs.SnapForkable, "v1.0-forkable.0-100.kv.111111.tmp"),
	}
	for _, tf := range tmpFiles {
		touchFile(tf)
		confirmExist(t, tf)
	}

	// Run DeleteStateSnapshots (non-dry-run)
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true})
	require.NoError(t, err)

	// All .tmp files should be removed
	for _, tf := range tmpFiles {
		confirmDoesntExist(t, tf)
	}
}

func Test_DeleteStateSnaps_DryRunKeepsTmpFiles(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	// Create some normal state snapshot files
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 10)
	for i := 0; i < 3; i++ {
		createFiles(t, dirs, i*10, (i+1)*10, &b)
	}

	touchFile := func(path string) {
		f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
		require.NoError(t, err)
		f.Close()
	}

	// Create .tmp files
	tmpFiles := []string{
		filepath.Join(dirs.SnapDomain, "v1.1-commitment.8272-8280.kv.252752124.tmp"),
		filepath.Join(dirs.SnapHistory, "v2.0-commitment.8256-8272.kvi.857462302.tmp"),
	}
	for _, tf := range tmpFiles {
		touchFile(tf)
	}

	// Run DeleteStateSnapshots with dry-run=true
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DryRun: true})
	require.NoError(t, err)

	// .tmp files should still exist (dry-run does not delete)
	for _, tf := range tmpFiles {
		confirmExist(t, tf)
	}
}

// Test_DeleteLatestStateSnaps_SubsetRemoval verifies that when --latest removes a merged
// file, all its sub-range files are also removed (the subset removal fix).
func Test_DeleteLatestStateSnaps_SubsetRemoval(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	// Use stepSize=1 so RootNum values map 1:1 to step numbers in filenames
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 1)

	// Create files simulating a real merge scenario:
	// Base merged files: 0-128, 128-192
	// Latest merged file: 192-224
	// Sub-ranges of 192-224: 192-208, 208-216, 216-220, 220-222, 222-223, 223-224
	// Tip file: 224-225
	ranges := [][2]int{
		{0, 128}, {128, 192},
		{192, 224},
		{192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}, {223, 224},
		{224, 225},
	}
	for _, r := range ranges {
		createFiles(t, dirs, r[0], r[1], &b)
	}

	// First call: should remove only the tip file 224-225
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	// 224-225 should be gone
	file, _ := b.domain.DataFile(version.V1_0, RootNum(224), RootNum(225))
	confirmDoesntExist(t, file)

	// 192-224 and its sub-ranges should still exist
	for _, r := range [][2]int{{192, 224}, {192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}} {
		file, _ = b.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmExist(t, file)
	}

	// Second call: should remove 223-224 (From >= _maxFrom=223),
	// 192-224 (To == _maxTo=224), AND all sub-ranges of 192-224
	err = DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	// All of these should be gone (the merged file and all its sub-ranges)
	for _, r := range [][2]int{{192, 224}, {192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}, {223, 224}} {
		file, _ = b.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmDoesntExist(t, file)
	}

	// Base files should still exist
	file, _ = b.domain.DataFile(version.V1_0, RootNum(0), RootNum(128))
	confirmExist(t, file)
	file, _ = b.domain.DataFile(version.V1_0, RootNum(128), RootNum(192))
	confirmExist(t, file)
}

// Test_DeleteStateSnaps_StepRange_SubsetRemoval verifies that --step range mode
// also removes subset files within the specified range.
func Test_DeleteStateSnaps_StepRange_SubsetRemoval(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 1)

	// Create files:
	// 0-128, 128-192, 192-224
	// Sub-ranges of 192-224: 192-208, 208-216, 216-220, 220-222, 222-223, 223-224
	ranges := [][2]int{
		{0, 128}, {128, 192},
		{192, 224},
		{192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}, {223, 224},
	}
	for _, r := range ranges {
		createFiles(t, dirs, r[0], r[1], &b)
	}

	// Use --step 192-224 to remove the merged file and its subsets
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, StepRange: "192-224", DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	// 192-224 and all sub-ranges should be gone
	for _, r := range [][2]int{{192, 224}, {192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}, {223, 224}} {
		file, _ := b.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmDoesntExist(t, file)
	}

	// Base files should still exist
	file, _ := b.domain.DataFile(version.V1_0, RootNum(0), RootNum(128))
	confirmExist(t, file)
	file, _ = b.domain.DataFile(version.V1_0, RootNum(128), RootNum(192))
	confirmExist(t, file)
}

// Test_DeleteLatestStateSnaps_NoFalseSubsetRemoval verifies that sub-range files
// of a non-removed merged file are NOT incorrectly removed by the subset pass.
func Test_DeleteLatestStateSnaps_NoFalseSubsetRemoval(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 1)

	// Create non-overlapping files: 0-64, 64-128, 128-192, 192-193
	// 0-64 has sub-ranges: 0-32, 32-64
	ranges := [][2]int{
		{0, 64}, {0, 32}, {32, 64},
		{64, 128}, {128, 192}, {192, 193},
	}
	for _, r := range ranges {
		createFiles(t, dirs, r[0], r[1], &b)
	}

	// Remove latest: should only remove 192-193
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	file, _ := b.domain.DataFile(version.V1_0, RootNum(192), RootNum(193))
	confirmDoesntExist(t, file)

	// Sub-ranges of 0-64 should NOT be removed (0-64 itself is not being removed)
	file, _ = b.domain.DataFile(version.V1_0, RootNum(0), RootNum(32))
	confirmExist(t, file)
	file, _ = b.domain.DataFile(version.V1_0, RootNum(32), RootNum(64))
	confirmExist(t, file)
	file, _ = b.domain.DataFile(version.V1_0, RootNum(0), RootNum(64))
	confirmExist(t, file)
}

// Test_DeleteLatestStateSnaps_NoCrossDomainSubsetRemoval verifies that the subset
// pass does not cascade across domain types. If accounts has a merged 192-224 file
// but storage does NOT (only has sub-range files), removing accounts.192-224
// must NOT delete storage's sub-range files — they are the only copy of that data.
// This simulates a crash during merge that leaves domains at different merge levels.
func Test_DeleteLatestStateSnaps_NoCrossDomainSubsetRemoval(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	// Set up accounts: has merged 192-224 + sub-ranges + tip 224-225
	acctBundle := bundle{}
	acctBundle.domain, acctBundle.history, acctBundle.ii = state.SnapSchemaFromDomainCfg(statecfg.Schema.AccountsDomain, dirs, 1)
	for _, r := range [][2]int{{0, 128}, {128, 192}, {192, 224}, {192, 208}, {208, 216}, {224, 225}} {
		createFiles(t, dirs, r[0], r[1], &acctBundle)
	}

	// Set up storage: NO merged 192-224, only has sub-range files + tip 224-225
	// This simulates a crash where accounts merged but storage didn't
	stoBundle := bundle{}
	stoBundle.domain, stoBundle.history, stoBundle.ii = state.SnapSchemaFromDomainCfg(statecfg.Schema.StorageDomain, dirs, 1)
	for _, r := range [][2]int{{0, 128}, {128, 192}, {192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}, {223, 224}, {224, 225}} {
		createFiles(t, dirs, r[0], r[1], &stoBundle)
	}

	// First --latest: remove tip 224-225 from both domains
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true})
	require.NoError(t, err)

	// Second --latest: should remove accounts.192-224 + its sub-ranges,
	// but storage sub-ranges must SURVIVE (no merged storage.192-224 to cascade from)
	err = DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true})
	require.NoError(t, err)

	// accounts.192-224 and its sub-ranges should be gone
	for _, r := range [][2]int{{192, 224}, {192, 208}, {208, 216}} {
		file, _ := acctBundle.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmDoesntExist(t, file)
	}

	// storage sub-ranges should STILL EXIST — they are NOT subsets of any
	// storage file marked for removal (storage has no 192-224 merged file)
	for _, r := range [][2]int{{192, 208}, {208, 216}, {216, 220}, {220, 222}} {
		file, _ := stoBundle.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmExist(t, file)
	}

	// storage.223-224 SHOULD be gone (From >= _maxFrom=223, direct match)
	file, _ := stoBundle.domain.DataFile(version.V1_0, RootNum(223), RootNum(224))
	confirmDoesntExist(t, file)
}

// Test_DeleteStateSnaps_StepRange_OverlappingNonSubsetPreserved verifies that files whose
// step range overlaps a removed file but is NOT a strict subset are preserved.
// Uses --step mode to precisely control which range is targeted, avoiding the
// _maxFrom/_maxTo heuristics of --latest mode.
func Test_DeleteStateSnaps_StepRange_OverlappingNonSubsetPreserved(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 1)

	// Create files:
	// 0-64: base file (outside target range)
	// 64-192: merged file targeted for removal via --step 64-192
	// 32-128: overlaps 64-192 but starts before it — NOT a subset
	// 128-256: overlaps 64-192 but extends beyond it — NOT a subset
	// 96-160: strict subset of 64-192 — SHOULD be removed
	// 64-128: strict subset of 64-192 — SHOULD be removed
	ranges := [][2]int{
		{0, 64},
		{64, 192},
		{32, 128},
		{128, 256},
		{96, 160},
		{64, 128},
	}
	for _, r := range ranges {
		createFiles(t, dirs, r[0], r[1], &b)
	}

	// Use --step 64-192 to precisely target the merged file
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, StepRange: "64-192", DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	// 64-192 and its strict subsets 96-160, 64-128 should be gone
	for _, r := range [][2]int{{64, 192}, {96, 160}, {64, 128}} {
		file, _ := b.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmDoesntExist(t, file)
	}

	// Overlapping-but-non-subset files should still exist
	file, _ := b.domain.DataFile(version.V1_0, RootNum(32), RootNum(128))
	confirmExist(t, file)
	file, _ = b.domain.DataFile(version.V1_0, RootNum(128), RootNum(256))
	confirmExist(t, file)

	// Base file should still exist
	file, _ = b.domain.DataFile(version.V1_0, RootNum(0), RootNum(64))
	confirmExist(t, file)
}

// Test_DeleteLatestStateSnaps_DryRunPreservesSubsetFiles verifies that dry-run mode
// does not actually delete subset files — they should appear in output but remain on disk.
func Test_DeleteLatestStateSnaps_DryRunPreservesSubsetFiles(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 1)

	// Create files:
	// 0-128: base merged file
	// 128-256: merged file (latest, will be targeted for removal)
	// 128-192, 192-224: sub-ranges of 128-256
	// 256-257: tip file
	ranges := [][2]int{
		{0, 128},
		{128, 256},
		{128, 192}, {192, 224},
		{256, 257},
	}
	for _, r := range ranges {
		createFiles(t, dirs, r[0], r[1], &b)
	}

	// First dry-run call: targets tip 256-257 — should NOT delete anything
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DryRun: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	// ALL files should still exist after dry-run
	for _, r := range [][2]int{{0, 128}, {128, 256}, {128, 192}, {192, 224}, {256, 257}} {
		file, _ := b.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmExist(t, file)
	}

	// Now actually remove the tip so the next call targets 128-256
	err = DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	file, _ := b.domain.DataFile(version.V1_0, RootNum(256), RootNum(257))
	confirmDoesntExist(t, file)

	// Dry-run targeting 128-256 and its subsets — should NOT delete anything
	err = DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, RemoveLatest: true, DryRun: true, DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	// 128-256 and all sub-ranges should STILL exist (dry-run)
	for _, r := range [][2]int{{128, 256}, {128, 192}, {192, 224}} {
		file, _ := b.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmExist(t, file)
	}

	// Base file should still exist
	file, _ = b.domain.DataFile(version.V1_0, RootNum(0), RootNum(128))
	confirmExist(t, file)
}
