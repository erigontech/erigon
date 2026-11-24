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

	domain := agg.d[kv.ReceiptDomain]
	files := []*DirtyFiles{domain.dirtyFiles, domain.History.dirtyFiles, domain.InvertedIndex.dirtyFiles, agg.searchII(kv.LogAddrIdx).dirtyFiles}
	var reader *AggregatorRoTx
	var replaced []*FilesItem
	agg.SetMaintenanceObserver(func(_ context.Context, event MaintenanceEvent) {
		if event.Point != MaintenanceFilesBuilt {
			return
		}
		// Pin the files from disk before the builder publishes its own mappings of the same paths.
		require.NoError(t, agg.OpenFolder(db))
		reader = agg.BeginFilesRo()
		t.Cleanup(reader.Close)
		for _, dirty := range files {
			require.Equal(t, 1, dirty.Len())
			old := dirty.Items()[0]
			t.Cleanup(old.closeFiles)
			require.NotNil(t, old.decompressor)
			replaced = append(replaced, old)
		}
	})
	require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), 0, unboundedFinalityCtx, nil))
	require.NotNil(t, reader)
	for i, old := range replaced {
		replacement := files[i].Items()[0]
		require.NotSame(t, old, replacement)
		require.Equal(t, old.FilePaths(agg.dirs.Snap), replacement.FilePaths(agg.dirs.Snap))
		require.NotNil(t, old.decompressor, "a pinned reader must keep its mappings")
		require.True(t, old.decompressor.IsOpen())
	}
	reader.Close()
	for i, old := range replaced {
		require.Nil(t, old.decompressor, "replaced mappings must close with their last reader")
		require.Nil(t, old.index)
		require.Nil(t, old.vAccessor)
		replacement := files[i].Items()[0]
		require.True(t, replacement.decompressor.IsOpen())
		require.FileExists(t, replacement.decompressor.FilePath())
	}
}
