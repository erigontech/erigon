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
		collateAndMergeHistory(t, db, h, txs)

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
			hc := h.MakeContext()
			_ = hc
			lastOnFs, _ := h.files.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			h.integrateMergedFiles(nil, []*filesItem{lastOnFs}, nil, nil)
			require.NotNil(lastOnFs.decompressor)

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
			loc := hc.ic.loc // replace of locality index must not affect current HistoryContext, but expect to be closed after last reader
			h.localityIndex.integrateFiles(LocalityIndexFiles{}, 0, 0)
			require.NotNil(loc.file)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
			require.NotNil(loc.file)

			nonDeletedOnFs, _ := h.files.Max()
			require.False(nonDeletedOnFs.frozen)
			require.NotNil(nonDeletedOnFs.decompressor) // non-canDelete files are not closed

			hc = h.MakeContext()
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
			hc := h.MakeContext()
			lastOnFs, _ := h.files.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			h.integrateMergedFiles(nil, []*filesItem{lastOnFs}, nil, nil)

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
		})
	}
	t.Run("large_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, false, logger)
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
			hc := h.MakeContext()
			_ = hc
			lastOnFs, _ := h.files.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			h.integrateMergedFiles([]*filesItem{lastOnFs}, nil, nil, nil, nil, nil)
			require.NotNil(lastOnFs.decompressor)

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

			nonDeletedOnFs, _ := h.files.Max()
			require.False(nonDeletedOnFs.frozen)
			require.NotNil(nonDeletedOnFs.decompressor) // non-canDelete files are not closed

			hc = h.MakeContext()
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
			hc := h.MakeContext()
			lastOnFs, _ := h.files.Max()
			require.False(lastOnFs.frozen) // prepared dataset must have some non-frozen files. or it's bad dataset.
			h.integrateMergedFiles([]*filesItem{lastOnFs}, nil, nil, nil, nil, nil)

			require.NotNil(lastOnFs.decompressor)
			hc.Close()
			require.Nil(lastOnFs.decompressor)
		})
	}
	logger := log.New()
	_, db, d, txs := filledDomain(t, logger)
	test(t, d, db, txs)
}
