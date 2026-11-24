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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestAggregatorBuildRetiresReplacedFiles(t *testing.T) {
	const stepSize = uint64(10)
	db, agg := testDbAndAggregatorv3(t, stepSize)
	ctx := t.Context()
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		for _, domain := range []kv.Domain{kv.AccountsDomain, kv.ReceiptDomain} {
			writer := at.d[domain].NewWriter(db)
			t.Cleanup(writer.Close)
			for _, txNum := range []uint64{1, stepSize + 1} {
				require.NoError(t, writer.PutWithPrev([]byte{byte(txNum)}, []byte{7}, txNum, nil))
			}
			require.NoError(t, writer.Flush(ctx, tx))
			writer.Close()
		}
		writer := at.searchII(kv.LogAddrIdx).newWriter(db, agg.dirs.Tmp, false)
		defer writer.close()
		require.NoError(t, writer.Add([]byte{1}, 1))
		return writer.Flush(ctx, tx)
	}))

	receipts := agg.d[kv.ReceiptDomain]
	files := []struct {
		kind     string
		dirty    *DirtyFiles
		original *FilesItem
	}{
		{kind: "receipt values", dirty: receipts.dirtyFiles},
		{kind: "receipt history", dirty: receipts.History.dirtyFiles},
		{kind: "receipt history index", dirty: receipts.InvertedIndex.dirtyFiles},
		{kind: "log address index", dirty: agg.searchII(kv.LogAddrIdx).dirtyFiles},
	}
	var reader *AggregatorRoTx
	agg.SetMaintenanceObserver(func(_ context.Context, event MaintenanceEvent) {
		if event.Point != MaintenanceFilesBuilt {
			return
		}
		// Pin the files from disk before the builder publishes its own mappings of the same paths.
		require.NoError(t, agg.OpenFolder(db))
		reader = agg.BeginFilesRo()
		t.Cleanup(reader.Close)
		for i := range files {
			file := &files[i]
			items := file.dirty.Items()
			require.Len(t, items, 1, file.kind)
			file.original = items[0]
			t.Cleanup(file.original.closeFiles)
			require.NotNil(t, file.original.decompressor, file.kind)
		}
	})
	require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), 0, unboundedFinalityCtx, nil))
	require.NotNil(t, reader)
	for _, file := range files {
		old := file.original
		replacement, ok := file.dirty.Get(old)
		require.True(t, ok, "missing %s replacement", file.kind)
		require.NotSame(t, old, replacement, file.kind)
		require.Equal(t, old.FilePaths(agg.dirs.Snap), replacement.FilePaths(agg.dirs.Snap), file.kind)
		require.NotNil(t, old.decompressor, "a pinned reader must keep the %s mapping", file.kind)
		require.True(t, old.decompressor.IsOpen(), file.kind)
	}
	reader.Close()
	for _, file := range files {
		old := file.original
		require.Nil(t, old.decompressor, "the last reader must release the old %s mapping", file.kind)
		require.Nil(t, old.index, file.kind)
		require.Nil(t, old.vAccessor, file.kind)
		replacement, ok := file.dirty.Get(old)
		require.True(t, ok, "missing %s replacement", file.kind)
		require.True(t, replacement.decompressor.IsOpen(), file.kind)
		require.FileExists(t, replacement.decompressor.FilePath(), file.kind)
	}
}
