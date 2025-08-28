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

package state

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

func TestGCReadAfterRemoveFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)
		collateAndMergeHistory(t, db, h, txs, true)

		t.Run("read after: remove when have reader", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - create immutable view
			// - del cold file
			// - read from canDelete file
			// - close view
			// - open new view
			// - make sure there is no canDelete file
			hc := h.BeginFilesRo()

			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)
			require.NotNil(lastOnFs.decompressor)
			h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

			lastInView := hc.files[len(hc.files)-1]

			g := seg.NewPagedReader(hc.statelessGetter(len(hc.files)-1), hc.h.HistoryValuesOnCompressedPage, true)
			require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
			require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
			if g.HasNext() {
				k, _ := g.Next(nil)
				require.Len(k, 8)
				v, _ := g.Next(nil)
				require.Len(v, 8)
			}

			require.NotNil(lastOnFs.decompressor)
			//replace of locality index must not affect current HistoryRoTx, but expect to be closed after last reader
			hc.Close()
			require.Nil(lastOnFs.decompressor)

			nonDeletedOnFs, _ := h.dirtyFiles.Max()
			require.False(nonDeletedOnFs.frozen)
			require.NotNil(nonDeletedOnFs.decompressor) // non-canDelete files are not closed

			hc = h.BeginFilesRo()
			newLastInView := hc.files[len(hc.files)-1]
			require.False(lastOnFs.frozen)
			require.False(lastInView.startTxNum == newLastInView.startTxNum && lastInView.endTxNum == newLastInView.endTxNum)

			hc.Close()
		})

		t.Run("read after: remove when no btReaders", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - del cold file
			// - new reader must not see canDelete file
			hc := h.BeginFilesRo()
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
		})
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestDomainGCReadAfterRemoveFile(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *Domain, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)
		collateAndMerge(t, db, nil, h, txs)

		t.Run("read after: remove when have reader", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - create immutable view
			// - del cold file
			// - read from canDelete file
			// - close view
			// - open new view
			// - make sure there is no canDelete file
			hc := h.BeginFilesRo()
			_ = hc
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.

			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)

			require.NotNil(lastOnFs.decompressor)
			h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

			lastInView := hc.files[len(hc.files)-1]
			g := lastInView.src.decompressor.MakeGetter()
			require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
			require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
			if g.HasNext() {
				k, _ := g.Next(nil)
				require.Len(k, 8)
				v, _ := g.Next(nil)
				require.Len(v, 8)
			}

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)

			nonDeletedOnFs, _ := h.dirtyFiles.Max()
			require.False(nonDeletedOnFs.frozen)
			require.NotNil(nonDeletedOnFs.decompressor) // non-canDelete files are not closed

			hc = h.BeginFilesRo()
			newLastInView := hc.files[len(hc.files)-1]
			require.False(lastOnFs.frozen)
			require.False(lastInView.startTxNum == newLastInView.startTxNum && lastInView.endTxNum == newLastInView.endTxNum)

			hc.Close()
		})

		t.Run("read after: remove when no btReaders", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - del cold file
			// - new reader must not see canDelete file
			hc := h.BeginFilesRo()
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			deleteMergeFile(h.dirtyFiles, []*FilesItem{lastOnFs}, "", h.logger)
			h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
		})
	}
	logger := log.New()
	db, d, txs := filledDomain(t, logger)
	test(t, d, db, txs)
}
