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

	rootFrom, rootTo := RootNum(from), RootNum(to)

	touchFile := func(filepath string) {
		file, err := os.OpenFile(filepath, os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		file.Close()
	}

	genFile := func(schema state.SnapNameSchema) {
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

	genFile(b.domain)
	genFile(b.history)
	genFile(b.ii)
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
