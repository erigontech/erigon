package state

import (
	"context"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestGCReadAfterRemoveFile(t *testing.T) {
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
			h.integrateMergedFiles(nil, []*filesItem{lastOnFs}, nil, nil)
			require.NotNil(lastOnFs.decompressor)
			h.reCalcVisibleFiles()

			lastInView := hc.files[len(hc.files)-1]
			g := lastInView.src.decompressor.MakeGetter()
			require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
			require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
			if g.HasNext() {
				k, _ := g.Next(nil)
				require.Equal(8, len(k))
				v, _ := g.Next(nil)
				require.Equal(8, len(v))
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

		t.Run("read after: remove when no readers", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - del cold file
			// - new reader must not see canDelete file
			hc := h.BeginFilesRo()
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			h.integrateMergedFiles(nil, []*filesItem{lastOnFs}, nil, nil)

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
			h.integrateMergedDirtyFiles([]*filesItem{lastOnFs}, nil, nil, nil, nil, nil)
			require.NotNil(lastOnFs.decompressor)
			h.reCalcVisibleFiles()

			lastInView := hc.files[len(hc.files)-1]
			g := lastInView.src.decompressor.MakeGetter()
			require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
			require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
			if g.HasNext() {
				k, _ := g.Next(nil)
				require.Equal(8, len(k))
				v, _ := g.Next(nil)
				require.Equal(8, len(v))
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

		t.Run("read after: remove when no readers", func(t *testing.T) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			// - del cold file
			// - new reader must not see canDelete file
			hc := h.BeginFilesRo()
			lastOnFs, _ := h.dirtyFiles.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			h.integrateMergedDirtyFiles([]*filesItem{lastOnFs}, nil, nil, nil, nil, nil)
			h.reCalcVisibleFiles()

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
		})
	}
	logger := log.New()
	db, d, txs := filledDomain(t, logger)
	test(t, d, db, txs)
}
