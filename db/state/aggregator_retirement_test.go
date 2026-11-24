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

	"github.com/erigontech/erigon/db/datadir"
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
	require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), 0, unboundedFinalityCtx, nil))
	reader := agg.BeginFilesRo()
	defer reader.Close()
	domain := agg.d[kv.ReceiptDomain]
	files := []*DirtyFiles{domain.dirtyFiles, domain.History.dirtyFiles, domain.InvertedIndex.dirtyFiles, agg.searchII(kv.LogAddrIdx).dirtyFiles}
	var replaced []*FilesItem
	for _, dirty := range files {
		require.Equal(t, 1, dirty.Len())
		old := dirty.Items()[0]
		t.Cleanup(old.closeFiles)
		require.NotNil(t, old.decompressor)
		replaced = append(replaced, old)
	}

	// Build elsewhere so this checks publication without overwriting mapped files.
	builder := NewTest(datadir.New(t.TempDir())).StepSize(stepSize).MustOpen(ctx)
	defer builder.Close()
	static := &AggV3StaticFiles{}
	defer static.CleanupOnError()
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		domain := builder.d[kv.ReceiptDomain]
		collation, err := domain.collate(ctx, 0, 0, stepSize, tx)
		require.NoError(t, err)
		defer collation.Close()
		static.d[kv.ReceiptDomain], err = domain.buildFiles(ctx, 0, collation, builder.backgroundProgress)
		require.NoError(t, err)
		for id, ii := range builder.standaloneIIs() {
			if ii.Name != kv.LogAddrIdx {
				continue
			}
			indexCollation, err := ii.collate(ctx, 0, tx)
			require.NoError(t, err)
			static.ivfs[id], err = ii.buildFiles(ctx, 0, indexCollation, builder.backgroundProgress)
			indexCollation.Close()
			require.NoError(t, err)
		}
		return nil
	}))
	agg.IntegrateDirtyFiles(static, 0, stepSize)
	for i, old := range replaced {
		require.NotSame(t, old, files[i].Items()[0])
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
