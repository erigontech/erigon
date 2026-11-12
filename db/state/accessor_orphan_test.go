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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/mvcc"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// oneBuiltStepWithMissedAccessors leaves a domain holding one step whose .kv is
// on disk and in dirtyFiles, with its btree accessors removed so they count as
// missed. The returned paths are where a build would put them back.
func oneBuiltStepWithMissedAccessors(t *testing.T) (*Domain, *FilesItem, string, string) {
	t.Helper()
	logger := log.New()
	db, d := testDbAndDomain(t, logger)
	require.True(t, d.Accessors.Has(statecfg.AccessorBTree))

	ctx := t.Context()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domainRoTx := d.beginForTests()
	defer domainRoTx.Close()
	writer := domainRoTx.NewWriter()
	defer writer.Close()
	require.NoError(t, writer.PutWithPrev([]byte("k1"), []byte("v1"), 2, nil))
	require.NoError(t, writer.PutWithPrev([]byte("k2"), []byte("v2"), 3, nil))
	require.NoError(t, writer.Flush(ctx, tx))
	require.NoError(t, d.collateBuildIntegrate(ctx, 0, tx, background.NewProgressSet()))

	var item *FilesItem
	d.dirtyFiles.Walk(func(items []*FilesItem) bool {
		for _, it := range items {
			item = it
		}
		return true
	})
	require.NotNil(t, item, "collate must have produced a dirty file")

	btPath, kveiPath := d.kvBtAccessorPathForItem(item), d.kvExistenceIdxPathForItem(item)
	require.NoError(t, dir.RemoveFile(btPath))
	_ = dir.RemoveFile(kveiPath)
	require.NoFileExists(t, btPath)

	return d, item, btPath, kveiPath
}

func missedBtreeOnly(item *FilesItem) *MissedAccessorDomainFiles {
	return &MissedAccessorDomainFiles{
		history: &MissedAccessorHistoryFiles{ii: &MissedAccessorIIFiles{}},
		files:   MissedFilesMap{statecfg.AccessorBTree: []*FilesItem{item}},
	}
}

func runMissedAccessorBuild(t *testing.T, d *Domain, files *MissedAccessorDomainFiles) {
	t.Helper()
	g, gCtx := errgroup.WithContext(t.Context())
	d.BuildMissedAccessors(gCtx, g, background.NewProgressSet(), files, (&Aggregator{}).publishAccessors)
	require.NoError(t, g.Wait())
}

// A merge retires a primary while a missed-accessor build, working from a
// dirtyFiles snapshot taken before the merge, is still writing accessors for
// it. Publishing them anyway leaves files indexing bytes that are being
// deleted, and nothing ever removes them.
func TestBuildMissedAccessors_RetiredPrimaryGetsNoAccessors(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	d, item, btPath, kveiPath := oneBuiltStepWithMissedAccessors(t)

	retire(mvcc.RetireReasonMerged, d.dirtyFiles, []*FilesItem{item}, d.FilenameBase, log.New())
	runMissedAccessorBuild(t, d, missedBtreeOnly(item))

	require.NoFileExists(t, btPath, "the .bt indexes a primary that is being deleted")
	require.NoFileExists(t, kveiPath, "the .kvei indexes a primary that is being deleted")
}

// The converse, so this cannot become a refusal to build: a live primary gets
// its whole accessor set.
func TestBuildMissedAccessors_LivePrimaryGetsItsAccessors(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	d, item, btPath, kveiPath := oneBuiltStepWithMissedAccessors(t)

	runMissedAccessorBuild(t, d, missedBtreeOnly(item))

	require.FileExists(t, btPath)
	require.FileExists(t, kveiPath)
}

// Retirement for a reason that leaves the file on disk is still retirement: the
// item is gone from dirtyFiles either way, and canDelete alone would not say so.
func TestBuildMissedAccessors_RetiredAfterExternalDeleteGetsNoAccessors(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	d, item, btPath, kveiPath := oneBuiltStepWithMissedAccessors(t)

	retire(mvcc.RetireReasonWasDeletedFromDisk, d.dirtyFiles, []*FilesItem{item}, d.FilenameBase, log.New())
	require.False(t, item.canDelete.Load(), "this reason leaves canDelete false")
	runMissedAccessorBuild(t, d, missedBtreeOnly(item))

	require.NoFileExists(t, btPath)
	require.NoFileExists(t, kveiPath)
}
