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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

type duFakeSnapshots struct {
	dbservices.BlockSnapshots
	max uint64
}

func (f duFakeSnapshots) SegmentsMax() uint64 { return f.max }

type duFakeBlockReader struct {
	frozenMax uint64
	body      func(blockNum uint64) (baseTxNum, txCount uint64)
}

func (f duFakeBlockReader) Snapshots() dbservices.BlockSnapshots {
	return duFakeSnapshots{max: f.frozenMax}
}

func (f duFakeBlockReader) IterateFrozenBodies(_ kv.Getter, fn func(blockNum, baseTxNum, txCount uint64) error) error {
	for b := uint64(0); b < f.frozenMax; b++ {
		base, n := f.body(b)
		if err := fn(b, base, n); err != nil {
			return err
		}
	}
	return nil
}

func (duFakeBlockReader) FreezingCfg() ethconfig.BlocksFreezing { return ethconfig.BlocksFreezing{} }
func (duFakeBlockReader) AllTypes() []snaptype.Type             { return nil }
func (f duFakeBlockReader) TxnumReader() rawdbv3.TxNumsReader {
	return rawdbv3.TxNums.WithCustomReadTxNumFunc(duFakeTxIndex(f))
}

type duFakeTxIndex duFakeBlockReader

func (f duFakeTxIndex) MaxTxNum(_ context.Context, _ kv.Tx, _ kv.Cursor, blockNum uint64) (uint64, bool, error) {
	if blockNum >= f.frozenMax {
		return 0, false, nil
	}
	base, n := f.body(blockNum)
	return base + n - 1, true, nil
}

func (duFakeTxIndex) BlockNumber(context.Context, kv.Tx, uint64) (uint64, bool, error) {
	return 0, false, nil
}

type bundle struct {
	domain, history, ii state.SnapNameSchema
}

type RootNum = kv.RootNum

func TestSegLS_NoChaindata(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, lsDatadir(context.Background(), dirs, log.New()))
}

func Test_DeleteLatestStateSnaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	for _, dc := range []statecfg.DomainCfg{statecfg.Schema.AccountsDomain, statecfg.Schema.StorageDomain, statecfg.Schema.CodeDomain, statecfg.Schema.ReceiptDomain} {
		b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 10)
		for i := range 10 {
			createFiles(t, i*10, (i+1)*10, &b)
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

	for i := range 9 {
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

	for i := range 4 {
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
		file, err := os.OpenFile(filepath, os.O_RDONLY|os.O_CREATE, 0o644)
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

func createFiles(t *testing.T, from, to int, b *bundle) {
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
	for i := range 3 {
		createFiles(t, i*10, (i+1)*10, &b)
	}

	touchFile := func(path string) {
		f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
		require.NoError(t, err)
		f.Close()
	}

	// Create .tmp files in all snapshot directories (matching patterns from issue #18789)
	tmpFiles := []string{
		filepath.Join(dirs.Snap, "v1.1-headers.0-500.seg.123456.tmp"),
		filepath.Join(dirs.SnapDomain, "v1.1-commitment.8272-8280.kv.252752124.tmp"),
		filepath.Join(dirs.SnapHistory, "v2.0-commitment.8256-8272.kvi.857462302.tmp"),
		filepath.Join(dirs.SnapIdx, "v1.1-storage.8256-8288.bt.209594880.tmp"),
		filepath.Join(dirs.SnapAccessors, "v2.0-commitment.8256-8272.kvi.3646922560.existence.tmp"),
		filepath.Join(dirs.SnapCaplin, "v1.0-beaconblocks.0-100.seg.999999.tmp"),
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
	for i := range 3 {
		createFiles(t, i*10, (i+1)*10, &b)
	}

	touchFile := func(path string) {
		f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
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
		{0, 128},
		{128, 192},
		{192, 224},
		{192, 208},
		{208, 216},
		{216, 220},
		{220, 222},
		{222, 223},
		{223, 224},
		{224, 225},
	}
	for _, r := range ranges {
		createFiles(t, r[0], r[1], &b)
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
		{0, 128},
		{128, 192},
		{192, 224},
		{192, 208},
		{208, 216},
		{216, 220},
		{220, 222},
		{222, 223},
		{223, 224},
	}
	for _, r := range ranges {
		createFiles(t, r[0], r[1], &b)
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

func Test_parseStepRange(t *testing.T) {
	const maxStep = 224
	for _, tc := range []struct {
		name     string
		in       string
		from, to uint64
		wantErr  bool
	}{
		{name: "range", in: "5-10", from: 5, to: 10},
		{name: "range from zero", in: "0-128", from: 0, to: 128},
		{name: "from plus", in: "5+", from: 5, to: maxStep},
		{name: "from plus zero", in: "0+", from: 0, to: maxStep},
		{name: "from plus equals max", in: "224+", from: 224, to: maxStep},
		{name: "empty", in: "", wantErr: true},
		{name: "plus only", in: "+", wantErr: true},
		{name: "single number", in: "5", wantErr: true},
		{name: "bad plus prefix", in: "5x+", wantErr: true},
		{name: "range trailing chars", in: "5-10x", wantErr: true},
		{name: "range extra dash", in: "5-10-20", wantErr: true},
		{name: "not a number", in: "abc", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, to, err := parseStepRange(tc.in, maxStep)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.from, from)
			require.Equal(t, tc.to, to)
		})
	}
}

// Test_DeleteStateSnaps_StepRange_FromPlus verifies that "N+" resolves "to" to the
// highest available step, removing everything from step N to the latest.
func Test_DeleteStateSnaps_StepRange_FromPlus(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	dc := statecfg.Schema.ReceiptDomain
	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 1)

	ranges := [][2]int{
		{0, 128},
		{128, 192},
		{192, 224},
		{192, 208},
		{208, 216},
		{216, 220},
		{220, 222},
		{222, 223},
		{223, 224},
	}
	for _, r := range ranges {
		createFiles(t, r[0], r[1], &b)
	}

	// "192+" must behave like "192-224" because 224 is the highest available step.
	err := DeleteStateSnapshots(DeleteStateSnapshotsArgs{Dirs: dirs, StepRange: "192+", DomainNames: []string{"receipt"}})
	require.NoError(t, err)

	for _, r := range [][2]int{{192, 224}, {192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}, {223, 224}} {
		file, _ := b.domain.DataFile(version.V1_0, RootNum(r[0]), RootNum(r[1]))
		confirmDoesntExist(t, file)
	}

	file, _ := b.domain.DataFile(version.V1_0, RootNum(0), RootNum(128))
	confirmExist(t, file)
	file, _ = b.domain.DataFile(version.V1_0, RootNum(128), RootNum(192))
	confirmExist(t, file)
}

func Test_DeleteStateSnaps_EmptyStepRange(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stepRange    string
		removeLatest bool
	}{
		{name: "from plus at latest", stepRange: "224+"},
		{name: "from plus past latest", stepRange: "225+"},
		{name: "equal bounds", stepRange: "224-224"},
		{name: "zero bounds", stepRange: "0-0"},
		{name: "reversed bounds", stepRange: "225-224"},
		{name: "step overrides latest", stepRange: "224+", removeLatest: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			b := bundle{}
			b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(statecfg.Schema.ReceiptDomain, dirs, 1)
			createFiles(t, 0, 224, &b)
			file, _ := b.domain.DataFile(version.V1_0, 0, 224)
			require.NoError(t, os.WriteFile(file+".torrent", []byte("torrent"), 0o644))
			require.NoError(t, os.WriteFile(file+".123.tmp", []byte("tmp"), 0o644))
			var files []string
			require.NoError(t, filepath.WalkDir(dirs.Snap, func(path string, entry os.DirEntry, err error) error {
				if err == nil && !entry.IsDir() {
					files = append(files, path)
				}
				return err
			}))

			require.NoError(t, DeleteStateSnapshots(DeleteStateSnapshotsArgs{
				Dirs:         dirs,
				StepRange:    tc.stepRange,
				RemoveLatest: tc.removeLatest,
				DomainNames:  []string{"receipt"},
			}))
			for _, path := range files {
				require.FileExists(t, path)
			}
		})
	}
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
		{0, 64},
		{0, 32},
		{32, 64},
		{64, 128},
		{128, 192},
		{192, 193},
	}
	for _, r := range ranges {
		createFiles(t, r[0], r[1], &b)
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
		createFiles(t, r[0], r[1], &acctBundle)
	}

	// Set up storage: NO merged 192-224, only has sub-range files + tip 224-225
	// This simulates a crash where accounts merged but storage didn't
	stoBundle := bundle{}
	stoBundle.domain, stoBundle.history, stoBundle.ii = state.SnapSchemaFromDomainCfg(statecfg.Schema.StorageDomain, dirs, 1)
	for _, r := range [][2]int{{0, 128}, {128, 192}, {192, 208}, {208, 216}, {216, 220}, {220, 222}, {222, 223}, {223, 224}, {224, 225}} {
		createFiles(t, r[0], r[1], &stoBundle)
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
		createFiles(t, r[0], r[1], &b)
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
		{128, 192},
		{192, 224},
		{256, 257},
	}
	for _, r := range ranges {
		createFiles(t, r[0], r[1], &b)
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

		// commitment hist overrides history/idx/accessor
		{"history", "v1.0-commitment.0-16.v", duCatCommitHist},
		{"idx", "v1.0-commitment.0-16.ef", duCatCommitHist},
		{"accessor", "v1.1-commitment.0-16.vi", duCatCommitHist},
		{"accessor", "v2.0-commitment.0-16.efi", duCatCommitHist},

		// commitment in domain is still domains (not commitment hist)
		{"domain", "v1.0-commitment.0-16.kv", duCatDomains},

		// plain categories
		{"history", "v1.0-accounts.0-16.v", duCatHistory},
		{"idx", "v1.0-accounts.0-16.ef", duCatInvIdx},
		{"accessor", "v1.0-accounts.0-16.bt", duCatAccessors},
		{"caplin", "v1.0-beaconblocks.0-100.seg", duCatCaplin},

		// block segments (top-level snapshots dir)
		{"snapshots", "v1.0-000000-000500-headers.seg", duCatBlocks},
		{"snapshots", "v1.0-000000-000500-bodies.seg", duCatBlocks},
		{"snapshots", "v1.0-000000-000500-transactions.idx", duCatBlocks},

		// consensus-layer segments also live in the top-level snapshots dir,
		// but their retention follows the caplin flags, not --prune.mode
		{"snapshots", "v1.1-000000-000100-beaconblocks.seg", duCatCaplin},
		{"snapshots", "v1.1-000000-000100-beaconblocks.idx", duCatCaplin},
		{"snapshots", "v1.1-000000-000100-blobsidecars.seg", duCatCaplin},
		{"snapshots", "v1.1-000000-000100-blocksidecars.seg", duCatCaplin},

		// case insensitivity
		{"Domain", "v1.0-accounts.0-16.kv", duCatDomains},
		{"HISTORY", "v1.0-RCACHE.0-16.v", duCatRcache},

		// unknown dir with segment extension → block segments
		{"something", "random.dat", duCatBlocks},

		// non-segment files → other
		{"snapshots", "salt.txt", duCatOther},
		{"snapshots", "v1.0-000000-000500-headers.torrent", duCatOther},
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
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
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
	writeFile(dirs.Snap, "v1.0-000000-000500-headers.seg", 800)

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
		if f.Name == "v1.0-000000-000500-headers.seg" {
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

func TestDUComputeEstimates_FullKeepsHistoryTheDenseTipNeeds(t *testing.T) {
	snapDir := t.TempDir()
	br := duFakeBlockReader{frozenMax: 600_000, body: func(b uint64) (uint64, uint64) {
		if b < 300_000 {
			return b, 1
		}
		return 300_000 + (b-300_000)*10, 10
	}}
	_, tx := mdbxtest.NewTestTx(t)
	file := func(dir, name string, size int64, cat string, from, to uint64) duFileInfo {
		return duFileInfo{Path: filepath.Join(snapDir, dir, name), Name: name, Size: size, Category: cat, IsState: true, From: from, To: to}
	}
	files := []duFileInfo{
		file("domain", "v1.1-accounts.0-3300.kv", 100, duCatDomains, 0, 3300),
		file("history", "v1.1-accounts.0-512.v", 1000, duCatHistory, 0, 512),
		file("history", "v1.1-accounts.1024-1536.v", 10, duCatHistory, 1024, 1536),
		{Path: filepath.Join(snapDir, "v1.1-000000-006000-headers.seg"), Name: "v1.1-000000-006000-headers.seg", Size: 1, Category: duCatBlocks, From: 0, To: 600_000},
	}

	estimates, err := duComputeEstimates(context.Background(), tx, br, nil, 1000, files, snapDir)
	require.NoError(t, err)
	require.Equal(t, "full", estimates[1].Mode)
	require.Equal(t, int64(111), estimates[1].TotalBytes)
}

func duTestEstimates(t *testing.T, files []duFileInfo, snapDir string, frozenMax uint64) ([]duEstimate, error) {
	_, tx := mdbxtest.NewTestTx(t)
	br := duFakeBlockReader{frozenMax: frozenMax, body: func(b uint64) (uint64, uint64) { return b, 1 }}
	return duComputeEstimates(context.Background(), tx, br, nil, 1000, files, snapDir)
}

func TestDUComputeEstimates_NoPruning(t *testing.T) {
	snapDir := t.TempDir()
	files := []duFileInfo{
		{Path: filepath.Join(snapDir, "domain", "v1.1-accounts.0-10.kv"), Name: "v1.1-accounts.0-10.kv", Size: 100, Category: duCatDomains, IsState: true, From: 0, To: 10},
		{Path: filepath.Join(snapDir, "history", "v1.1-accounts.0-10.v"), Name: "v1.1-accounts.0-10.v", Size: 200, Category: duCatHistory, IsState: true, From: 0, To: 10},
		{Path: filepath.Join(snapDir, "v1.1-000000-000500-headers.seg"), Name: "v1.1-000000-000500-headers.seg", Size: 300, Category: duCatBlocks, From: 0, To: 50000},
	}

	estimates, err := duTestEstimates(t, files, snapDir, 50_000)
	require.NoError(t, err)
	require.Len(t, estimates, 4)
	for _, e := range estimates {
		require.Equal(t, int64(600), e.TotalBytes, "mode %q", e.Mode)
	}
}

func TestDUComputeEstimates_EmptyFiles(t *testing.T) {
	estimates, err := duTestEstimates(t, nil, t.TempDir(), 0)
	require.NoError(t, err)
	require.Len(t, estimates, 4)
	for _, e := range estimates {
		require.Equal(t, int64(0), e.TotalBytes)
	}
}

func TestDUComputeEstimates_ExcludesCaplin(t *testing.T) {
	snapDir := t.TempDir()
	files := []duFileInfo{
		{Path: filepath.Join(snapDir, "domain", "v1.0-accounts.0-10.kv"), Name: "v1.0-accounts.0-10.kv", Size: 100, Category: duCatDomains, IsState: true, From: 0, To: 10},
		{Path: filepath.Join(snapDir, "v1.1-000000-000100-blobsidecars.seg"), Name: "v1.1-000000-000100-blobsidecars.seg", Size: 9000, Category: duCatCaplin, From: 0, To: 100},
		{Path: filepath.Join(snapDir, "caplin", "v1.1-000000-000100-BalancesDump.seg"), Name: "v1.1-000000-000100-BalancesDump.seg", Size: 500, Category: duCatCaplin, From: 0, To: 100},
	}

	estimates, err := duTestEstimates(t, files, snapDir, 50_000)
	require.NoError(t, err)
	require.Len(t, estimates, 4)
	for _, e := range estimates {
		require.Equal(t, int64(100), e.TotalBytes, "mode %q must exclude caplin data", e.Mode)
	}
	require.Equal(t, int64(9500), duCaplinBytes(files))
}

func TestDUComputeEstimates_CountsUnparseableStateFilesInEveryMode(t *testing.T) {
	snapDir := t.TempDir()
	files := []duFileInfo{
		{Path: filepath.Join(snapDir, "domain", "v1.1-accounts.0-10.kv"), Name: "v1.1-accounts.0-10.kv", Size: 100, Category: duCatDomains, IsState: true, From: 0, To: 10},
		{Path: filepath.Join(snapDir, "domain", ".DS_Store"), Name: ".DS_Store", Size: 7, Category: duCatDomains, IsState: true},
	}

	estimates, err := duTestEstimates(t, files, snapDir, 300_000)
	require.NoError(t, err)
	for _, e := range estimates {
		require.Equal(t, int64(107), e.TotalBytes, "mode %q", e.Mode)
	}
}

func TestDUReadChain_UninitialisedChaindataIsAnError(t *testing.T) {
	_, tx := mdbxtest.NewTestTx(t)
	var err error
	require.NotPanics(t, func() { _, _, err = duReadChain(tx) })
	require.Error(t, err)
}

func TestDUStepSize_ZeroStepSizeIsAnError(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte("steps_in_frozen_file = 64\n"), 0o644))

	_, err := duStepSize(dirs, log.New())
	require.Error(t, err)
}

func duWriteSegment(t *testing.T, dir string, from, to uint64, typ snaptype.Enum) {
	word := []byte{1}
	if typ == snaptype2.Enums.Bodies {
		var err error
		word, err = rlp.EncodeToBytes(&types.BodyForStorage{BaseTxnID: types.BaseTxnID(from)})
		require.NoError(t, err)
	}
	duWriteSegmentWord(t, dir, from, to, typ, word)
}

func duWriteSegmentWord(t *testing.T, dir string, from, to uint64, typ snaptype.Enum, word []byte) {
	c, err := seg.NewCompressor(t.Context(), "test", filepath.Join(dir, snaptype.SegmentFileName(version.V1_0, from, to, typ)), dir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	require.NoError(t, c.AddWord(word))
	require.NoError(t, c.Compress())
	idxNames := []string{typ.String()}
	if typ == snaptype2.Transactions.Enum() {
		idxNames = append(idxNames, snaptype2.Indexes.TxnHash2BlockNum.Name)
	}
	for _, name := range idxNames {
		idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:   1,
			BucketSize: 10,
			TmpDir:     dir,
			IndexFile:  filepath.Join(dir, snaptype.IdxFileName(version.V1_0, from, to, name)),
			LeafSize:   8,
		}, log.New())
		require.NoError(t, err)
		idx.DisableFsync()
		require.NoError(t, idx.AddKey([]byte{1}, 0))
		require.NoError(t, idx.Build(t.Context()))
		idx.Close()
	}
}

func TestDUOpenBlockSnapshots_UsesTheDownloaderRange(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	for _, r := range [][2]uint64{{0, 500_000}, {500_000, 1_000_000}} {
		duWriteSegment(t, dirs.Snap, r[0], r[1], snaptype2.Enums.Headers)
		duWriteSegment(t, dirs.Snap, r[0], r[1], snaptype2.Enums.Bodies)
	}
	duWriteSegment(t, dirs.Snap, 0, 500_000, snaptype2.Enums.Transactions)

	snaps, err := duOpenBlockSnapshots(dirs, networkname.Mainnet, log.New())
	require.NoError(t, err)
	defer snaps.Close()
	require.Equal(t, uint64(999_999), snaps.SegmentsMax())
}

func TestDUOpenBlockSnapshots_IgnoresBrokenTransactions(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	duWriteSegment(t, dirs.Snap, 0, 500_000, snaptype2.Enums.Headers)
	duWriteSegment(t, dirs.Snap, 0, 500_000, snaptype2.Enums.Bodies)
	duWriteSegment(t, dirs.Snap, 0, 500_000, snaptype2.Enums.Transactions)
	txs := filepath.Join(dirs.Snap, snaptype.SegmentFileName(version.V1_0, 0, 500_000, snaptype2.Enums.Transactions))
	require.NoError(t, os.Truncate(txs, 3))

	snaps, err := duOpenBlockSnapshots(dirs, networkname.Mainnet, log.New())
	require.NoError(t, err)
	defer snaps.Close()
	require.Equal(t, uint64(499_999), snaps.SegmentsMax())
}

func TestDUCollect_WorksWithoutChaindata(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte("step_size = 1000\nsteps_in_frozen_file = 64\n"), 0o644))

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.Equal(t, "unknown", result.Chain)
	require.Len(t, result.Estimates, 4)
	require.NoFileExists(t, filepath.Join(dirs.Chaindata, "mdbx.dat"))
}

func duTestDatadirWithBlocks(t *testing.T, ranges ...[2]uint64) datadir.Dirs {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte("step_size = 1000\nsteps_in_frozen_file = 64\n"), 0o644))
	for _, r := range ranges {
		duWriteSegment(t, dirs.Snap, r[0], r[1], snaptype2.Enums.Headers)
		duWriteSegment(t, dirs.Snap, r[0], r[1], snaptype2.Enums.Bodies)
	}
	return dirs
}

func TestDUCollect_BrokenBlockSegmentOmitsEstimates(t *testing.T) {
	dirs := duTestDatadirWithBlocks(t, [2]uint64{0, 500_000})
	require.NoError(t, os.Truncate(filepath.Join(dirs.Snap, snaptype.SegmentFileName(version.V1_0, 0, 500_000, snaptype2.Enums.Headers)), 3))

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.Empty(t, result.Estimates)
	require.Equal(t, uint64(500_000), result.BlockRange[1])
}

func TestDUCollect_BodiesIndexGapOmitsEstimates(t *testing.T) {
	dirs := duTestDatadirWithBlocks(t, [2]uint64{0, 500_000}, [2]uint64{500_000, 1_000_000}, [2]uint64{1_000_000, 1_500_000})
	require.NoError(t, dir2.RemoveFile(filepath.Join(dirs.Snap, snaptype.IdxFileName(version.V1_0, 500_000, 1_000_000, snaptype2.Enums.Bodies.String()))))

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.Empty(t, result.Estimates)
}

func TestDUCollect_UnindexedLeadingBodiesOmitEstimates(t *testing.T) {
	dirs := duTestDatadirWithBlocks(t, [2]uint64{0, 500_000}, [2]uint64{500_000, 1_000_000})
	require.NoError(t, dir2.RemoveFile(filepath.Join(dirs.Snap, snaptype.IdxFileName(version.V1_0, 0, 500_000, snaptype2.Enums.Bodies.String()))))

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.Empty(t, result.Estimates)
}

func TestDUCollect_HeadersBeyondBodiesOmitEstimates(t *testing.T) {
	dirs := duTestDatadirWithBlocks(t, [2]uint64{0, 500_000})
	duWriteSegment(t, dirs.Snap, 500_000, 1_000_000, snaptype2.Enums.Headers)

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.Empty(t, result.Estimates)
}

func TestDUCollect_UndecodableBodiesOmitEstimates(t *testing.T) {
	dirs := duTestDatadirWithBlocks(t)
	duWriteSegment(t, dirs.Snap, 0, 500_000, snaptype2.Enums.Headers)
	duWriteSegmentWord(t, dirs.Snap, 0, 500_000, snaptype2.Enums.Bodies, []byte{1})

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.Empty(t, result.Estimates)
	require.Equal(t, uint64(500_000), result.BlockRange[1])
}

func TestDUCollect_IndexedBlocksKeepEstimates(t *testing.T) {
	dirs := duTestDatadirWithBlocks(t, [2]uint64{0, 500_000}, [2]uint64{500_000, 1_000_000})

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.Len(t, result.Estimates, 4)
}

func TestDUStepSize_LegacyDatadirIsNotWritten(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, datadir.PreverifiedFileName), nil, 0o644))

	stepSize, err := duStepSize(dirs, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(config3.LegacyStepSize), stepSize)
	require.NoFileExists(t, filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE))
}

func TestDUStepSize_FreshDatadirUsesTheDefault(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	stepSize, err := duStepSize(dirs, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(config3.DefaultStepSize), stepSize)
	require.NoFileExists(t, filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE))
}

func TestDUCollect_UnnamedChainIsUnknown(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte("step_size = 1000\nsteps_in_frozen_file = 64\n"), 0o644))
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	genesis := common.HexToHash("0x01")
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, genesis, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, genesis, &chain.Config{ChainID: uint256.NewInt(1337)})
	}))
	db.Close()

	result, err := duCollect(context.Background(), dirs, log.New())
	require.NoError(t, err)
	require.NotEmpty(t, result.ConfiguredMode)
	require.Equal(t, "unknown", result.Chain)
}

func TestDUComputeEstimates_LogIndexFollowsHistoryWindow(t *testing.T) {
	snapDir := t.TempDir()
	_, tx := mdbxtest.NewTestTx(t)
	br := duFakeBlockReader{frozenMax: 600_000, body: func(b uint64) (uint64, uint64) { return b, 1 }}
	logIdx := func(from, to uint64, size int64) duFileInfo {
		name := fmt.Sprintf("v1.1-logaddrs.%d-%d.ef", from, to)
		return duFileInfo{Path: filepath.Join(snapDir, "idx", name), Name: name, Size: size, Category: duCatInvIdx, IsState: true, From: from, To: to}
	}
	files := []duFileInfo{
		{Path: filepath.Join(snapDir, "domain", "v1.1-accounts.0-600.kv"), Name: "v1.1-accounts.0-600.kv", Category: duCatDomains, IsState: true, From: 0, To: 600},
		logIdx(0, 256, 1000),
		logIdx(256, 337, 100),
		logIdx(337, 338, 50),
		logIdx(338, 448, 10),
		logIdx(448, 600, 1),
	}

	estimates, err := duComputeEstimates(context.Background(), tx, br, nil, 1000, files, snapDir)
	require.NoError(t, err)
	want := map[string]int64{"archive": 1161, "full": 61, "blocks": 61, "minimal": 1}
	for _, e := range estimates {
		require.Equal(t, want[e.Mode], e.TotalBytes, "mode %q", e.Mode)
	}
}

func TestDUComputeEstimates_ArchiveSkipsCommitmentHistory(t *testing.T) {
	snapDir := t.TempDir()
	files := []duFileInfo{
		{Path: filepath.Join(snapDir, "domain", "v1.1-accounts.0-10.kv"), Name: "v1.1-accounts.0-10.kv", Size: 100, Category: duCatDomains, IsState: true, From: 0, To: 10},
		{Path: filepath.Join(snapDir, "history", "v1.1-commitment.0-10.v"), Name: "v1.1-commitment.0-10.v", Size: 7000, Category: duCatCommitHist, IsState: true, From: 0, To: 10},
		{Path: filepath.Join(snapDir, "accessor", "v1.1-commitment.0-10.vi"), Name: "v1.1-commitment.0-10.vi", Size: 700, Category: duCatCommitHist, IsState: true, From: 0, To: 10},
	}

	estimates, err := duTestEstimates(t, files, snapDir, 50_000)
	require.NoError(t, err)
	require.Equal(t, "archive", estimates[0].Mode)
	require.Equal(t, int64(100), estimates[0].TotalBytes)
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
		Chain:          "mainnet",
		ConfiguredMode: "archive",
		BlockRange:     [2]uint64{0, 21500000},
		StepRange:      [2]uint64{0, 2048},
		TotalBytes:     1024 * 1024 * 1024 * 100, // 100 GB
		TotalFiles:     500,
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

	require.True(t, strings.Contains(out, "mainnet"), "should contain chain name")
	require.True(t, strings.Contains(out, "mainnet | archive |"), "should show the configured mode")
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
	t.Run("from chaindata", func(t *testing.T) {
		var buf bytes.Buffer
		duFormatHuman(&buf, duResult{Chain: "mainnet", ConfiguredMode: "full", Categories: map[string]duCategoryStat{}}, false)
		require.Contains(t, buf.String(), "mainnet | full |")
	})

	t.Run("no chaindata", func(t *testing.T) {
		var buf bytes.Buffer
		duFormatHuman(&buf, duResult{Chain: "unknown", Categories: map[string]duCategoryStat{}}, false)
		require.Contains(t, buf.String(), "unknown | mode unknown |")
	})
}

func TestDUFormatHuman_EmptyResult(t *testing.T) {
	result := duResult{
		Chain:      "unknown",
		Categories: map[string]duCategoryStat{},
	}

	var buf bytes.Buffer
	duFormatHuman(&buf, result, false)
	out := buf.String()

	require.True(t, strings.Contains(out, "unknown"), "should contain chain name")
	require.True(t, strings.Contains(out, "total"), "should have total line")
}

func TestDUFormatJSON(t *testing.T) {
	result := duResult{
		Chain:      "mainnet",
		BlockRange: [2]uint64{0, 21500000},
		StepRange:  [2]uint64{0, 2048},
		TotalBytes: 107374182400,
		TotalFiles: 500,
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

	var decoded map[string]any
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
	snapDir := t.TempDir()
	at := func(dir, name string, size int64, cat string, state bool, from, to uint64) duFileInfo {
		return duFileInfo{Path: filepath.Join(snapDir, dir, name), Name: name, Size: size, Category: cat, IsState: state, From: from, To: to}
	}
	files := []duFileInfo{
		at("domain", "v1.1-accounts.0-500.kv", 10000, duCatDomains, true, 0, 500),
		at("domain", "v1.1-storage.400-500.kv", 20000, duCatDomains, true, 400, 500),
		at("history", "v1.1-accounts.0-100.v", 8000, duCatHistory, true, 0, 100),
		at("history", "v1.1-accounts.400-500.v", 12000, duCatHistory, true, 400, 500),
		at("idx", "v1.1-accounts.0-100.ef", 5000, duCatInvIdx, true, 0, 100),
		at("idx", "v1.1-accounts.400-500.ef", 7000, duCatInvIdx, true, 400, 500),
		at("accessor", "v1.1-accounts.0-100.vi", 3000, duCatAccessors, true, 0, 100),
		at("history", "v1.1-commitment.0-100.v", 2000, duCatCommitHist, true, 0, 100),
		at("domain", "v1.1-rcache.0-500.kv", 1500, duCatRcache, true, 0, 500),
		at("", "v1.1-000000-003000-headers.seg", 15000, duCatBlocks, false, 0, 300000),
		at("", "v1.1-000000-003000-transactions.seg", 10000, duCatBlocks, false, 0, 300000),
		at("", "v1.1-003000-005000-bodies.seg", 18000, duCatBlocks, false, 300000, 500000),
		at("", "v1.1-000000-000100-beaconblocks.seg", 4000, duCatCaplin, false, 0, 100),
	}

	maxBlock := uint64(500000)
	maxStep := uint64(500)

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
	estimates, err := duTestEstimates(t, files, snapDir, maxBlock)
	require.NoError(t, err)
	require.Len(t, estimates, 4)

	// Verify archive >= blocks >= full >= minimal (acceptance criterion 3).
	require.GreaterOrEqual(t, estimates[0].TotalBytes, estimates[2].TotalBytes, "archive >= blocks")
	require.GreaterOrEqual(t, estimates[2].TotalBytes, estimates[1].TotalBytes, "blocks >= full")
	require.GreaterOrEqual(t, estimates[1].TotalBytes, estimates[3].TotalBytes, "full >= minimal")

	// Archive delta must be 0.
	require.Equal(t, int64(0), estimates[0].Delta)
	// Non-archive deltas must be negative or zero.
	require.LessOrEqual(t, estimates[1].Delta, int64(0))
	require.LessOrEqual(t, estimates[2].Delta, int64(0))
	require.LessOrEqual(t, estimates[3].Delta, int64(0))

	// Build result struct.
	result := duResult{
		Chain:      "mainnet",
		BlockRange: [2]uint64{0, maxBlock},
		StepRange:  [2]uint64{0, maxStep},
		TotalBytes: expectedTotal,
		TotalFiles: len(files),
		Categories: cats,
		Estimates:  estimates,
	}

	// Acceptance criterion 1: human output has all three sections.
	var humanBuf bytes.Buffer
	duFormatHuman(&humanBuf, result, false)
	human := humanBuf.String()

	// Header section.
	require.Contains(t, human, "mainnet")
	require.Contains(t, human, "archive")
	require.Contains(t, human, "500.000")
	require.Contains(t, human, "steps 0–500")

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
	err = duFormatJSON(&jsonBuf, result)
	require.NoError(t, err)

	var decoded duResult
	err = json.Unmarshal(jsonBuf.Bytes(), &decoded)
	require.NoError(t, err, "JSON must be valid and unmarshalable")

	// Verify round-trip fidelity.
	require.Equal(t, result.Chain, decoded.Chain)
	require.Equal(t, result.BlockRange, decoded.BlockRange)
	require.Equal(t, result.StepRange, decoded.StepRange)
	require.Equal(t, result.TotalBytes, decoded.TotalBytes)
	require.Equal(t, result.TotalFiles, decoded.TotalFiles)
	require.Len(t, decoded.Categories, len(result.Categories))
	require.Len(t, decoded.Estimates, 4)

	// Verify JSON estimates also maintain archive >= blocks >= full >= minimal.
	require.GreaterOrEqual(t, decoded.Estimates[0].TotalBytes, decoded.Estimates[2].TotalBytes, "archive >= blocks")
	require.GreaterOrEqual(t, decoded.Estimates[2].TotalBytes, decoded.Estimates[1].TotalBytes, "blocks >= full")
	require.GreaterOrEqual(t, decoded.Estimates[1].TotalBytes, decoded.Estimates[3].TotalBytes, "full >= minimal")
}

// touchBlockSnap creates an empty file at <dirs.Snap>/<name>, ensuring the snap
// directory exists. Used by the rm-blocks tests below.
func touchBlockSnap(t *testing.T, dirs datadir.Dirs, name string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dirs.Snap, 0o755))
	full := filepath.Join(dirs.Snap, name)
	f, err := os.OpenFile(full, os.O_RDONLY|os.O_CREATE, 0o644)
	require.NoError(t, err)
	f.Close()
	return full
}

// Test_DeleteBlockSnaps covers the happy path end-to-end: only the latest
// range is removed (incl. transactions-to-block.idx and .torrent / partial
// .torrent<suffix> companions), older ranges and their torrents survive,
// and .tmp artifacts in dirs.Snap are swept regardless.
func Test_DeleteBlockSnaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	keep := []string{
		"v1.0-000000-000500-headers.seg",
		"v1.0-000000-000500-headers.seg.torrent", // companion of an older range — must survive
		"v1.0-000000-000500-bodies.seg",
		"v1.0-000000-000500-transactions.seg",
		"v1.0-000000-000500-transactions-to-block.idx",
	}
	rm := []string{
		"v1.0-000500-001000-headers.seg",
		"v1.0-000500-001000-headers.seg.torrent",      // exact .torrent companion
		"v1.0-000500-001000-headers.seg.torrent.bolt", // partial .torrent<suffix>
		"v1.0-000500-001000-bodies.seg",
		"v1.0-000500-001000-transactions.seg",
		"v1.0-000500-001000-transactions.idx",
		"v1.0-000500-001000-transactions-to-block.idx",
		"v1.1-headers.0-500.seg.123456.tmp",     // unconditional sweep
		"v1.0-000500-001000-bodies.seg.999.tmp", // unconditional sweep
	}
	var keepPaths, rmPaths []string
	for _, n := range keep {
		keepPaths = append(keepPaths, touchBlockSnap(t, dirs, n))
	}
	for _, n := range rm {
		rmPaths = append(rmPaths, touchBlockSnap(t, dirs, n))
	}

	err := DeleteBlockSnapshots(DeleteBlockSnapshotsArgs{Dirs: dirs})
	require.NoError(t, err)

	for _, p := range keepPaths {
		confirmExist(t, p)
	}
	for _, p := range rmPaths {
		confirmDoesntExist(t, p)
	}
}

// Test_DeleteBlockSnaps_DryRun verifies that DryRun does not modify the
// filesystem (block files OR .tmp artifacts).
func Test_DeleteBlockSnaps_DryRun(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	files := []string{
		"v1.0-000000-000500-headers.seg",
		"v1.0-000500-001000-headers.seg",
		"v1.0-000500-001000-transactions-to-block.idx",
		"v1.0-000500-001000-headers.seg.torrent.bolt",
		"v1.1-headers.0-500.seg.123456.tmp",
	}
	var paths []string
	for _, n := range files {
		paths = append(paths, touchBlockSnap(t, dirs, n))
	}

	err := DeleteBlockSnapshots(DeleteBlockSnapshotsArgs{Dirs: dirs, DryRun: true})
	require.NoError(t, err)

	for _, p := range paths {
		confirmExist(t, p)
	}
}

// Test_DeleteBlockSnaps_NoBlockFilesSweepsTmp verifies the empty-snap-dir
// branch still cleans .tmp leftovers.
func Test_DeleteBlockSnaps_NoBlockFilesSweepsTmp(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	tmp := touchBlockSnap(t, dirs, "v1.1-headers.0-500.seg.123456.tmp")

	err := DeleteBlockSnapshots(DeleteBlockSnapshotsArgs{Dirs: dirs})
	require.NoError(t, err)

	confirmDoesntExist(t, tmp)
}

func Test_removeAccessorsForRebuild(t *testing.T) {
	dirs := datadir.New(t.TempDir())

	touch := func(path string) {
		f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	accessors := []string{
		filepath.Join(dirs.Snap, "v1.0-headers.0-500.idx"),
		filepath.Join(dirs.SnapCaplin, "v1.0-beaconstate.0-100.idx"),
		filepath.Join(dirs.SnapAccessors, "v1.0-accounts.0-64.vi"),
		filepath.Join(dirs.SnapAccessors, "v1.0-accounts.0-64.efi"),
		filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-64.kvi"),
		filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-64.bt"),
		filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-64.kvei"),
	}
	dataFiles := []string{
		filepath.Join(dirs.Snap, "v1.0-headers.0-500.seg"),
		filepath.Join(dirs.SnapCaplin, "v1.0-beaconstate.0-100.seg"),
		filepath.Join(dirs.SnapIdx, "v1.0-accounts.0-64.ef"),
		filepath.Join(dirs.SnapHistory, "v1.0-accounts.0-64.v"),
		filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-64.kv"),
	}
	for _, f := range accessors {
		touch(f)
		touch(f + ".torrent")
	}
	for _, f := range dataFiles {
		touch(f)
	}

	orphanTorrent := filepath.Join(dirs.SnapDomain, "v1.0-storage.0-64.kvi.torrent")
	touch(orphanTorrent)
	dataTorrent := filepath.Join(dirs.Snap, "v1.0-headers.0-500.seg.torrent")
	touch(dataTorrent)

	require.NoError(t, removeAccessorsForRebuild(dirs, log.New()))

	for _, f := range accessors {
		confirmDoesntExist(t, f)
		confirmDoesntExist(t, f+".torrent")
	}
	confirmDoesntExist(t, orphanTorrent)
	for _, f := range dataFiles {
		confirmExist(t, f)
	}
	confirmExist(t, dataTorrent)
}

// Every commitment branch key sorts before "state", so a seek for it in a file
// without the state key runs past the last key and finds no cursor.
func TestCheckCommitmentFileHasRootWithoutStateKey(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "v1.0-commitment.0-1.kv")
	compression := statecfg.Schema.CommitmentDomain.Compression
	comp, err := seg.NewCompressor(t.Context(), t.Name(), dataPath, dir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer comp.Close()
	w := seg.NewWriter(comp, compression)
	for _, word := range [][]byte{{0x01}, {0xaa}} {
		_, err = w.Write(word)
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())

	d, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	defer d.Close()
	btPath := filepath.Join(dir, "v1.0-commitment.0-1.bt")
	require.NoError(t, btindex.BuildBtreeIndexWithDecompressor(btPath, "", seg.NewReader(d.MakeGetter(), compression),
		background.NewProgressSet(), dir, 1, log.New(), true, statecfg.AccessorBTree))

	hasState, broken, _, err := checkCommitmentFileHasRoot(dataPath)
	require.NoError(t, err)
	require.False(t, hasState)
	require.False(t, broken)
}
