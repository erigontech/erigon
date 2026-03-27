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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

// ── du tests ────────────────────────────────────────────────────────────

func TestDUClassifyFile(t *testing.T) {
	tests := []struct {
		dir, name string
		want      string
	}{
		// domains (data files)
		{"domain", "v1.0-accounts.0-16.kv", duCatDomains},
		{"domain", "v1.0-storage.16-32.kv", duCatDomains},

		// domain accessors (.kvi, .kvei, .bt) live in domain/ and are never pruned
		{"domain", "v1.0-accounts.0-16.kvi", duCatDomains},
		{"domain", "v1.0-accounts.0-16.kvei", duCatDomains},
		{"domain", "v1.0-accounts.0-16.bt", duCatDomains},

		// rcache overrides domain
		{"domain", "v1.0-rcache.0-16.kv", duCatRcache},
		{"history", "v1.0-rcache.0-16.v", duCatRcache},
		{"idx", "v1.0-rcache.0-16.ef", duCatRcache},

		// commitment hist overrides history/idx
		{"history", "v1.0-commitment.0-16.v", duCatCommitHist},
		{"idx", "v1.0-commitment.0-16.ef", duCatCommitHist},

		// commitment in domain is still domains (not commitment hist)
		{"domain", "v1.0-commitment.0-16.kv", duCatDomains},

		// plain categories
		{"history", "v1.0-accounts.0-16.v", duCatHistory},
		{"idx", "v1.0-accounts.0-16.ef", duCatInvIdx},
		{"accessor", "v1.0-accounts.0-16.bt", duCatAccessors},
		{"caplin", "v1.0-beaconblocks.0-100.seg", duCatCaplin},

		// block segments (top-level snapshots dir)
		{"snapshots", "v1.0-0-500-headers.seg", duCatBlocks},
		{"snapshots", "v1.0-0-500-bodies.seg", duCatBlocks},
		{"snapshots", "v1.0-0-500-transactions.idx", duCatBlocks},

		// case insensitivity
		{"Domain", "v1.0-accounts.0-16.kv", duCatDomains},
		{"HISTORY", "v1.0-RCACHE.0-16.v", duCatRcache},

		// unknown dir with segment extension → block segments
		{"something", "random.dat", duCatBlocks},

		// non-segment files → other
		{"snapshots", "salt.txt", duCatOther},
		{"snapshots", "v1.0-0-500-headers.torrent", duCatOther},
	}

	for _, tt := range tests {
		t.Run(tt.dir+"/"+tt.name, func(t *testing.T) {
			got := duClassifyFile(tt.dir, tt.name)
			if got != tt.want {
				t.Errorf("duClassifyFile(%q, %q) = %q, want %q", tt.dir, tt.name, got, tt.want)
			}
		})
	}
}

func TestDUWalkSnapshots(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	// Create mock files in various snapshot subdirectories.
	writeFile := func(dir, name string, size int) {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0644))
	}

	// domain files (state file format: type.from-to)
	writeFile(dirs.SnapDomain, "v1.0-accounts.0-16.kv", 1000)
	writeFile(dirs.SnapDomain, "v1.0-rcache.0-16.kv", 200)

	// history files
	writeFile(dirs.SnapHistory, "v1.0-accounts.0-16.v", 500)
	writeFile(dirs.SnapHistory, "v1.0-commitment.0-16.v", 300)

	// idx files
	writeFile(dirs.SnapIdx, "v1.0-accounts.0-16.ef", 400)

	// accessor files
	writeFile(dirs.SnapAccessors, "v1.0-accounts.0-16.bt", 250)

	// caplin files
	writeFile(dirs.SnapCaplin, "v1.0-beaconblocks.0-100.seg", 600)

	// block segment files (top-level snapshots dir)
	writeFile(dirs.Snap, "v1.0-0-500-headers.seg", 800)

	files, err := duWalkSnapshots(dirs)
	require.NoError(t, err)

	// Build category->totalSize map.
	catSizes := make(map[string]int64)
	catCounts := make(map[string]int)
	for _, f := range files {
		catSizes[f.Category] += f.Size
		catCounts[f.Category]++
	}

	require.Equal(t, int64(1000), catSizes[duCatDomains])
	require.Equal(t, int64(200), catSizes[duCatRcache])
	require.Equal(t, int64(500), catSizes[duCatHistory])
	require.Equal(t, int64(300), catSizes[duCatCommitHist])
	require.Equal(t, int64(400), catSizes[duCatInvIdx])
	require.Equal(t, int64(250), catSizes[duCatAccessors])
	require.Equal(t, int64(600), catSizes[duCatCaplin])
	require.Equal(t, int64(800), catSizes[duCatBlocks])

	// Check range parsing works for a state file.
	var found bool
	for _, f := range files {
		if f.Name == "v1.0-accounts.0-16.kv" {
			require.Equal(t, uint64(0), f.From)
			require.Equal(t, uint64(16), f.To)
			require.True(t, f.IsState)
			found = true
			break
		}
	}
	require.True(t, found, "accounts domain file should be in results")

	// Check block segment is not a state file.
	for _, f := range files {
		if f.Name == "v1.0-0-500-headers.seg" {
			require.False(t, f.IsState)
			// Block file ranges are multiplied by 1000 by ParseFileName
			require.Equal(t, uint64(0), f.From)
			require.Equal(t, uint64(500000), f.To)
			break
		}
	}
}

func TestDUWalkSnapshots_EmptyDir(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	files, err := duWalkSnapshots(dirs)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestDUWalkSnapshots_MissingDir(t *testing.T) {
	// Use Open instead of New to avoid creating directories.
	dirs := datadir.Open(filepath.Join(t.TempDir(), "nonexistent"))
	files, err := duWalkSnapshots(dirs)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestDUComputeEstimates(t *testing.T) {
	// Synthetic file list simulating an archive node with realistic mainnet-like values:
	// - domain files (always kept)
	// - old and new history/idx files
	// - commitment hist (archive-only)
	// - rcache domain and history files (kept in non-archive modes with receipt pruning)
	// - logaddrs inv idx (receipt-related, kept in blocks mode)
	// - old and new block segments
	// - caplin (always kept)
	//
	// maxStep=2000, maxBlock=20_000_000, mergeBlock=15_537_394
	// stepPruneDistance = DefaultPruneDistance(100_000) * 2000 / 20_000_000 = 10
	// mergeStep = 15_537_394 * 2000 / 20_000_000 = 1553
	// State prune cutoff: step To <= 2000-10 = 1990 → old history/idx pruned
	// Block prune cutoff: block To <= 20_000_000-100_000 = 19_900_000 → old

	files := []duFileInfo{
		// Domains — always kept in all modes
		{Name: "accounts.0-500.kv", Size: 1000, Category: duCatDomains, IsState: true, From: 0, To: 500},
		{Name: "storage.1500-2000.kv", Size: 2000, Category: duCatDomains, IsState: true, From: 1500, To: 2000},

		// Old accessor (To=500 <= 1990 cutoff) — pruned in blocks/full/minimal
		{Name: "accounts.0-500.bt", Size: 500, Category: duCatAccessors, IsState: true, From: 0, To: 500},

		// Old history (To=500 <= 1990 cutoff) — pruned in blocks/full/minimal
		{Name: "accounts.0-500.v", Size: 3000, Category: duCatHistory, IsState: true, From: 0, To: 500},

		// New history (To=2000 > 1990) — kept in all
		{Name: "accounts.1500-2000.v", Size: 4000, Category: duCatHistory, IsState: true, From: 1500, To: 2000},

		// Old inverted index (To=500 <= 1990) — pruned in blocks/full/minimal
		{Name: "accounts.0-500.ef", Size: 1500, Category: duCatInvIdx, IsState: true, From: 0, To: 500},

		// New inverted index — kept
		{Name: "accounts.1500-2000.ef", Size: 2500, Category: duCatInvIdx, IsState: true, From: 1500, To: 2000},

		// Commitment hist — archive only (excluded from all non-archive modes)
		{Name: "commitment.0-500.v", Size: 800, Category: duCatCommitHist, IsState: true, From: 0, To: 500},

		// Rcache domain file (in domain/) — never pruned, kept in all modes
		{Path: "/data/snapshots/domain/rcache.0-500.kv", Name: "rcache.0-500.kv", Size: 400, Category: duCatRcache, IsState: true, From: 0, To: 500},

		// Rcache history file (in history/) — receipt-pruned:
		//   blocks: kept (KeepAllBlocksPruneMode)
		//   full: pruned (From=0 < mergeStep=1553)
		//   minimal: pruned (same, already pruned in full)
		{Path: "/data/snapshots/history/rcache.0-500.v", Name: "rcache.0-500.v", Size: 200, Category: duCatRcache, IsState: true, From: 0, To: 500},

		// Old logaddrs inv idx (receipt-related, To=500 <= 1990) —
		//   normally pruned as inv idx, but receipt-related so kept in blocks mode.
		//   full: pruned (From=0 < mergeStep=1553)
		//   minimal: pruned (same)
		{Name: "logaddrs.0-500.ef", Size: 350, Category: duCatInvIdx, IsState: true, From: 0, To: 500},

		// Old transaction block segment (To=15_000_000 <= 19_900_000) — pruned in full/minimal
		{Name: "0-15000-transactions.seg", Size: 5000, Category: duCatBlocks, IsState: false, From: 0, To: 15_000_000},

		// Old headers block segment — NOT prunable (headers/bodies always kept)
		{Name: "0-15000-headers.seg", Size: 3000, Category: duCatBlocks, IsState: false, From: 0, To: 15_000_000},

		// New block segment (To=20_000_000 > 19_900_000) — kept in all
		{Name: "15000-20000-headers.seg", Size: 6000, Category: duCatBlocks, IsState: false, From: 15_000_000, To: 20_000_000},

		// Caplin — always kept
		{Name: "beaconblocks.0-100.seg", Size: 700, Category: duCatCaplin, IsState: false, From: 0, To: 100},
	}

	maxBlock := uint64(20_000_000)
	maxStep := uint64(2000)

	mergeBlock := uint64(15_537_394)
	estimates := duComputeEstimates(files, maxBlock, maxStep, mergeBlock)
	require.Len(t, estimates, 3)

	// Archive: sum everything
	archiveTotal := int64(1000 + 2000 + 500 + 3000 + 4000 + 1500 + 2500 + 800 + 400 + 200 + 350 + 5000 + 3000 + 6000 + 700)
	require.Equal(t, "archive", estimates[0].Mode)
	require.Equal(t, archiveTotal, estimates[0].TotalBytes)
	require.Equal(t, int64(0), estimates[0].Delta)

	// Full: archive minus old history(3000) minus old idx(1500) minus old accessor(500)
	// minus commitHist(800) minus pre-merge tx(5000)
	// minus rcache hist(200, From=0 < mergeStep=1553)
	// minus logaddrs(350, From=0 < mergeStep=1553) — rcache domain(400) kept
	fullTotal := archiveTotal - 3000 - 1500 - 500 - 800 - 5000 - 200 - 350
	require.Equal(t, "full", estimates[1].Mode)
	require.Equal(t, fullTotal, estimates[1].TotalBytes)
	require.Equal(t, fullTotal-archiveTotal, estimates[1].Delta)
	require.Equal(t, "post-merge blocks", estimates[1].BlocksDesc)

	// Minimal: full minus nothing extra (old tx already pruned by full, remaining blocks are recent,
	// rcache hist and logaddrs already pruned by full's merge-based receipt pruning)
	minimalTotal := fullTotal
	require.Equal(t, "minimal", estimates[2].Mode)
	require.Equal(t, minimalTotal, estimates[2].TotalBytes)
	require.Equal(t, minimalTotal-archiveTotal, estimates[2].Delta)

	// Invariant: archive >= full >= minimal
	require.GreaterOrEqual(t, estimates[0].TotalBytes, estimates[1].TotalBytes)
	require.GreaterOrEqual(t, estimates[1].TotalBytes, estimates[2].TotalBytes)
}

func TestDUComputeEstimates_NoPruning(t *testing.T) {
	// When maxStep and maxBlock are small (below pruneDistance),
	// nothing gets pruned, so all modes should be equal (except commitment hist).
	files := []duFileInfo{
		{Name: "d.kv", Size: 100, Category: duCatDomains, IsState: true, From: 0, To: 10},
		{Name: "h.v", Size: 200, Category: duCatHistory, IsState: true, From: 0, To: 10},
		{Name: "b.seg", Size: 300, Category: duCatBlocks, IsState: false, From: 0, To: 50000},
	}

	estimates := duComputeEstimates(files, 50000, 10, 0)
	// All modes include everything (no old files to prune, no commitment/rcache, no merge block)
	require.Equal(t, int64(600), estimates[0].TotalBytes) // archive
	require.Equal(t, int64(600), estimates[1].TotalBytes) // full
	require.Equal(t, int64(600), estimates[2].TotalBytes) // minimal
}

func TestDUComputeEstimates_EmptyFiles(t *testing.T) {
	estimates := duComputeEstimates(nil, 0, 0, 0)
	require.Len(t, estimates, 3)
	for _, e := range estimates {
		require.Equal(t, int64(0), e.TotalBytes)
	}
}

func TestDUDetectNodeType(t *testing.T) {
	t.Run("archive - has old state history from step 0", func(t *testing.T) {
		// Archive mode keeps all history — old history files from step 0 are present.
		files := []duFileInfo{
			{Category: duCatDomains, Size: 100, IsState: true, To: 2000},
			{Category: duCatHistory, Size: 500, IsState: true, From: 0, To: 500},
			{Category: duCatHistory, Size: 500, IsState: true, From: 500, To: 2000},
			{Category: duCatBlocks, IsState: false, From: 0, To: 500000, Size: 200},
		}
		require.Equal(t, "archive", duDetectNodeType(files))
	})

	t.Run("archive - with commitment hist and rcache (still archive by history)", func(t *testing.T) {
		files := []duFileInfo{
			{Category: duCatDomains, Size: 100, IsState: true, To: 2000},
			{Category: duCatHistory, Size: 500, IsState: true, From: 0, To: 500},
			{Category: duCatCommitHist, Size: 50, IsState: true, From: 0, To: 500},
			{Category: duCatRcache, Size: 50, IsState: true, From: 0, To: 500},
			{Category: duCatBlocks, IsState: false, To: 500000, Size: 200},
		}
		require.Equal(t, "archive", duDetectNodeType(files))
	})

	t.Run("full - has old tx blocks from 0, no old state history", func(t *testing.T) {
		// Non-archive modes persist receipts (rcache present) but prune old history.
		// maxBlock=500000, pruneDistance=100000, cutoff=400000
		files := []duFileInfo{
			{Category: duCatDomains, Size: 100, IsState: true, To: 50},
			{Category: duCatHistory, Size: 500, IsState: true, From: 40, To: 50},
			{Category: duCatRcache, Size: 50, IsState: true, From: 0, To: 50},
			{Name: "0-300-transactions.seg", Category: duCatBlocks, IsState: false, From: 0, To: 300000, Size: 200},
			{Name: "300-500-headers.seg", Category: duCatBlocks, IsState: false, From: 300000, To: 500000, Size: 200},
		}
		require.Equal(t, "full", duDetectNodeType(files))
	})

	t.Run("full - has old transaction blocks not from 0, no old state history", func(t *testing.T) {
		// Transactions start at 200000 (not 0) → pre-merge segments were pruned → full mode
		files := []duFileInfo{
			{Category: duCatDomains, Size: 100, IsState: true, To: 50},
			{Category: duCatHistory, Size: 500, IsState: true, From: 40, To: 50},
			{Name: "200-300-transactions.seg", Category: duCatBlocks, IsState: false, From: 200000, To: 300000, Size: 200},
			{Name: "300-500-transactions.seg", Category: duCatBlocks, IsState: false, From: 300000, To: 500000, Size: 200},
		}
		require.Equal(t, "full", duDetectNodeType(files))
	})

	t.Run("minimal - only recent blocks", func(t *testing.T) {
		files := []duFileInfo{
			{Category: duCatDomains, Size: 100, IsState: true, To: 50},
			{Category: duCatBlocks, IsState: false, From: 450000, To: 500000, Size: 200},
		}
		require.Equal(t, "minimal", duDetectNodeType(files))
	})

	t.Run("minimal - empty files", func(t *testing.T) {
		require.Equal(t, "minimal", duDetectNodeType(nil))
	})

	t.Run("minimal - blocks below prune distance threshold", func(t *testing.T) {
		// maxBlock=50000 < pruneDistance=100000 → can't determine old blocks
		files := []duFileInfo{
			{Category: duCatBlocks, IsState: false, From: 0, To: 50000, Size: 200},
		}
		require.Equal(t, "minimal", duDetectNodeType(files))
	})

	t.Run("young chain with history from 0 not detected as archive", func(t *testing.T) {
		// Chain too young for pruning to matter (maxStep < stepPruneDistance).
		// Even non-archive modes keep all history when young.
		// stepPruneDistance = 100000 * 5 / 50000 = 10, maxStep=5 < 10
		files := []duFileInfo{
			{Category: duCatDomains, Size: 100, IsState: true, To: 5},
			{Category: duCatHistory, Size: 500, IsState: true, From: 0, To: 5},
			{Category: duCatBlocks, IsState: false, From: 0, To: 50000, Size: 200},
		}
		// maxStep=5, maxBlock=50000, stepPruneDistance=10, maxStep(5) <= stepPruneDistance(10)
		// → too young to distinguish, falls through to block-based detection → minimal
		// (no old tx segments since maxBlock < pruneDistance)
		require.Equal(t, "minimal", duDetectNodeType(files))
	})
}

func TestDUAggregateCategories(t *testing.T) {
	files := []duFileInfo{
		{Category: duCatDomains, Size: 100},
		{Category: duCatDomains, Size: 200},
		{Category: duCatHistory, Size: 300},
		{Category: duCatBlocks, Size: 50},
	}

	cats := duAggregateCategories(files)
	require.Equal(t, duCategoryStat{Bytes: 300, Files: 2}, cats[duCatDomains])
	require.Equal(t, duCategoryStat{Bytes: 300, Files: 1}, cats[duCatHistory])
	require.Equal(t, duCategoryStat{Bytes: 50, Files: 1}, cats[duCatBlocks])
	require.Len(t, cats, 3)
}

func TestDUAggregateCategories_Empty(t *testing.T) {
	cats := duAggregateCategories(nil)
	require.Empty(t, cats)
}

func TestDUFormatHuman(t *testing.T) {
	result := duResult{
		Chain:        "mainnet",
		DetectedMode: "archive",
		BlockRange:   [2]uint64{0, 21500000},
		StepRange:    [2]uint64{0, 2048},
		TotalBytes:   1024 * 1024 * 1024 * 100, // 100 GB
		TotalFiles:   500,
		Categories: map[string]duCategoryStat{
			duCatDomains: {Bytes: 50 * 1024 * 1024 * 1024, Files: 200},
			duCatHistory: {Bytes: 30 * 1024 * 1024 * 1024, Files: 150},
			duCatBlocks:  {Bytes: 20 * 1024 * 1024 * 1024, Files: 150},
		},
		Estimates: []duEstimate{
			{Mode: "archive", TotalBytes: 100 * 1024 * 1024 * 1024, Delta: 0, BlocksDesc: "all blocks", HistoryDesc: "all history"},
			{Mode: "full", TotalBytes: 80 * 1024 * 1024 * 1024, Delta: -20 * 1024 * 1024 * 1024, BlocksDesc: "all blocks", HistoryDesc: "last 100.000"},
		},
	}

	var buf bytes.Buffer
	duFormatHuman(&buf, result, false)
	out := buf.String()

	// Check header line — no ConfiguredMode set, so shows "archive (detected)".
	require.True(t, strings.Contains(out, "mainnet"), "should contain chain name")
	require.True(t, strings.Contains(out, "archive (detected)"), "should show detected mode with qualifier when DB unavailable")
	require.True(t, strings.Contains(out, "21.500.000"), "should contain formatted block range")
	require.True(t, strings.Contains(out, "2.048"), "should contain formatted step range")

	// Check breakdown section.
	require.True(t, strings.Contains(out, "Breakdown"), "should have breakdown header")
	require.True(t, strings.Contains(out, "domains"), "should list domains category")
	require.True(t, strings.Contains(out, "history"), "should list history category")
	require.True(t, strings.Contains(out, "block segments"), "should list block segments category")
	require.True(t, strings.Contains(out, "total"), "should have total line")

	// Check estimates section.
	require.True(t, strings.Contains(out, "Estimated Size by Node Type"), "should have estimates header")
	require.True(t, strings.Contains(out, "all blocks"), "should show blocks description")
	require.True(t, strings.Contains(out, "last 100.000"), "should show history description")
}

func TestDUFormatHuman_ConfiguredMode(t *testing.T) {
	t.Run("configured matches detected", func(t *testing.T) {
		result := duResult{
			Chain:          "mainnet",
			ConfiguredMode: "full",
			DetectedMode:   "full",
			Categories:     map[string]duCategoryStat{},
		}
		var buf bytes.Buffer
		duFormatHuman(&buf, result, false)
		out := buf.String()
		require.Contains(t, out, "full")
		require.NotContains(t, out, "(detected)")
		require.NotContains(t, out, "files look like")
	})

	t.Run("configured differs from detected", func(t *testing.T) {
		result := duResult{
			Chain:          "mainnet",
			ConfiguredMode: "full",
			DetectedMode:   "archive",
			Categories:     map[string]duCategoryStat{},
		}
		var buf bytes.Buffer
		duFormatHuman(&buf, result, false)
		out := buf.String()
		require.Contains(t, out, "full")
		require.Contains(t, out, "files look like archive")
	})

	t.Run("no DB - shows detected with qualifier", func(t *testing.T) {
		result := duResult{
			Chain:        "unknown",
			DetectedMode: "minimal",
			Categories:   map[string]duCategoryStat{},
		}
		var buf bytes.Buffer
		duFormatHuman(&buf, result, false)
		out := buf.String()
		require.Contains(t, out, "minimal (detected)")
	})
}

func TestDUFormatHuman_EmptyResult(t *testing.T) {
	result := duResult{
		Chain:        "unknown",
		DetectedMode: "minimal",
		Categories:   map[string]duCategoryStat{},
	}

	var buf bytes.Buffer
	duFormatHuman(&buf, result, false)
	out := buf.String()

	require.True(t, strings.Contains(out, "unknown"), "should contain chain name")
	require.True(t, strings.Contains(out, "total"), "should have total line")
}

func TestDUFormatJSON(t *testing.T) {
	result := duResult{
		Chain:        "mainnet",
		DetectedMode: "archive",
		BlockRange:   [2]uint64{0, 21500000},
		StepRange:    [2]uint64{0, 2048},
		TotalBytes:   107374182400,
		TotalFiles:   500,
		Categories: map[string]duCategoryStat{
			duCatDomains: {Bytes: 53687091200, Files: 200},
			duCatHistory: {Bytes: 32212254720, Files: 150},
		},
		Estimates: []duEstimate{
			{Mode: "archive", TotalBytes: 107374182400, Delta: 0, BlocksDesc: "all blocks", HistoryDesc: "all history"},
		},
	}

	var buf bytes.Buffer
	err := duFormatJSON(&buf, result)
	require.NoError(t, err)

	// Verify valid JSON by unmarshaling.
	var decoded duResult
	err = json.Unmarshal(buf.Bytes(), &decoded)
	require.NoError(t, err)

	require.Equal(t, "mainnet", decoded.Chain)
	require.Equal(t, "archive", decoded.DetectedMode)
	require.Equal(t, [2]uint64{0, 21500000}, decoded.BlockRange)
	require.Equal(t, [2]uint64{0, 2048}, decoded.StepRange)
	require.Equal(t, int64(107374182400), decoded.TotalBytes)
	require.Equal(t, 500, decoded.TotalFiles)
	require.Len(t, decoded.Categories, 2)
	require.Equal(t, int64(53687091200), decoded.Categories[duCatDomains].Bytes)
	require.Equal(t, 200, decoded.Categories[duCatDomains].Files)
	require.Len(t, decoded.Estimates, 1)
	require.Equal(t, "archive", decoded.Estimates[0].Mode)
}

func TestDUFormatJSON_EmptyResult(t *testing.T) {
	result := duResult{
		Chain:      "unknown",
		Categories: map[string]duCategoryStat{},
	}

	var buf bytes.Buffer
	err := duFormatJSON(&buf, result)
	require.NoError(t, err)

	var decoded map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &decoded)
	require.NoError(t, err)
	require.Equal(t, "unknown", decoded["chain"])
}

func TestDUFormatSize(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0B"},
		{512, "512B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{1048576, "1.0MB"},
		{1073741824, "1.0GB"},
		{1099511627776, "1.0TB"},
		{-1073741824, "-1.0GB"},
	}

	for _, tt := range tests {
		require.Equal(t, tt.expected, duFormatSize(tt.input), "for input %d", tt.input)
	}
}

func TestDUFormatNumber(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1.000"},
		{21500000, "21.500.000"},
		{1000000000, "1.000.000.000"},
	}

	for _, tt := range tests {
		require.Equal(t, tt.expected, duFormatNumber(tt.input), "for input %d", tt.input)
	}
}

// TestDUAcceptanceCriteria is an end-to-end acceptance test that verifies:
// 1. All three output sections render correctly (header, breakdown, estimates)
// 2. --json flag produces valid parseable JSON
// 3. Estimates are consistent (archive >= full >= minimal)
// 4. Category sizes sum to total
func TestDUAcceptanceCriteria(t *testing.T) {
	// Build a realistic file set that exercises all categories and pruning paths.
	files := []duFileInfo{
		{Name: "accounts.0-50000.kv", Size: 10000, Category: duCatDomains, IsState: true, From: 0, To: 50000},
		{Name: "storage.150000-200000.kv", Size: 20000, Category: duCatDomains, IsState: true, From: 150000, To: 200000},
		{Name: "accounts.0-50000.v", Size: 8000, Category: duCatHistory, IsState: true, From: 0, To: 50000},
		{Name: "accounts.150000-200000.v", Size: 12000, Category: duCatHistory, IsState: true, From: 150000, To: 200000},
		{Name: "accounts.0-50000.ef", Size: 5000, Category: duCatInvIdx, IsState: true, From: 0, To: 50000},
		{Name: "accounts.150000-200000.ef", Size: 7000, Category: duCatInvIdx, IsState: true, From: 150000, To: 200000},
		{Name: "accounts.0-50000.bt", Size: 3000, Category: duCatAccessors, IsState: true, From: 0, To: 50000},
		{Name: "commitment.0-50000.v", Size: 2000, Category: duCatCommitHist, IsState: true, From: 0, To: 50000},
		{Name: "rcache.0-50000.kv", Size: 1500, Category: duCatRcache, IsState: true, From: 0, To: 50000},
		{Name: "0-300-headers.seg", Size: 15000, Category: duCatBlocks, IsState: false, From: 0, To: 300000},
		{Name: "0-300-transactions.seg", Size: 10000, Category: duCatBlocks, IsState: false, From: 0, To: 300000},
		{Name: "300-500-bodies.seg", Size: 18000, Category: duCatBlocks, IsState: false, From: 300000, To: 500000},
		{Name: "beaconblocks.0-100.seg", Size: 4000, Category: duCatCaplin, IsState: false, From: 0, To: 100},
	}

	maxBlock := uint64(500000)
	maxStep := uint64(200000)

	// Aggregate categories.
	cats := duAggregateCategories(files)

	// Verify category sizes sum to total.
	var catSum int64
	var catFiles int
	for _, c := range cats {
		catSum += c.Bytes
		catFiles += c.Files
	}
	var expectedTotal int64
	for _, f := range files {
		expectedTotal += f.Size
	}
	require.Equal(t, expectedTotal, catSum, "category bytes must sum to total")
	require.Equal(t, len(files), catFiles, "category file count must sum to total files")

	// Compute estimates.
	estimates := duComputeEstimates(files, maxBlock, maxStep, 0)
	require.Len(t, estimates, 3)

	// Verify archive >= full >= minimal (acceptance criterion 3).
	require.GreaterOrEqual(t, estimates[0].TotalBytes, estimates[1].TotalBytes, "archive >= full")
	require.GreaterOrEqual(t, estimates[1].TotalBytes, estimates[2].TotalBytes, "full >= minimal")

	// Archive delta must be 0.
	require.Equal(t, int64(0), estimates[0].Delta)
	// Non-archive deltas must be negative or zero.
	require.LessOrEqual(t, estimates[1].Delta, int64(0))
	require.LessOrEqual(t, estimates[2].Delta, int64(0))

	// Build result struct.
	result := duResult{
		Chain:        "mainnet",
		DetectedMode: "archive",
		BlockRange:   [2]uint64{0, maxBlock},
		StepRange:    [2]uint64{0, maxStep},
		TotalBytes:   expectedTotal,
		TotalFiles:   len(files),
		Categories:   cats,
		Estimates:    estimates,
	}

	// Acceptance criterion 1: human output has all three sections.
	var humanBuf bytes.Buffer
	duFormatHuman(&humanBuf, result, false)
	human := humanBuf.String()

	// Header section.
	require.Contains(t, human, "mainnet")
	require.Contains(t, human, "archive")
	require.Contains(t, human, "500.000")
	require.Contains(t, human, "200.000")

	// Breakdown section.
	require.Contains(t, human, "Breakdown")
	require.Contains(t, human, "domains")
	require.Contains(t, human, "history")
	require.Contains(t, human, "block segments")
	require.Contains(t, human, "caplin")
	require.Contains(t, human, "total")

	// Estimates section.
	require.Contains(t, human, "Estimated Size by Node Type")
	require.Contains(t, human, "archive")
	require.Contains(t, human, "full")
	require.Contains(t, human, "minimal")

	// Acceptance criterion 2: JSON output is valid and parseable.
	var jsonBuf bytes.Buffer
	err := duFormatJSON(&jsonBuf, result)
	require.NoError(t, err)

	var decoded duResult
	err = json.Unmarshal(jsonBuf.Bytes(), &decoded)
	require.NoError(t, err, "JSON must be valid and unmarshalable")

	// Verify round-trip fidelity.
	require.Equal(t, result.Chain, decoded.Chain)
	require.Equal(t, result.DetectedMode, decoded.DetectedMode)
	require.Equal(t, result.BlockRange, decoded.BlockRange)
	require.Equal(t, result.StepRange, decoded.StepRange)
	require.Equal(t, result.TotalBytes, decoded.TotalBytes)
	require.Equal(t, result.TotalFiles, decoded.TotalFiles)
	require.Len(t, decoded.Categories, len(result.Categories))
	require.Len(t, decoded.Estimates, 3)

	// Verify JSON estimates also maintain archive >= full >= minimal.
	require.GreaterOrEqual(t, decoded.Estimates[0].TotalBytes, decoded.Estimates[1].TotalBytes)
	require.GreaterOrEqual(t, decoded.Estimates[1].TotalBytes, decoded.Estimates[2].TotalBytes)
}
