package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGCReadAfterRemoveFile(t *testing.T) {
	require := require.New(t)
	_, db, h, txs := filledHistory(t)
	collateAndMergeHistory(t, db, h, txs)
	ctx := context.Background()

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

		lastInView, _ := hc.historyFiles.Max()
		require.Equal(lastInView.startTxNum, lastOnFs.startTxNum)
		require.Equal(lastInView.endTxNum, lastOnFs.endTxNum)
		if lastInView.getter.HasNext() {
			k, _ := lastInView.getter.Next(nil)
			require.Equal(8, len(k))
			v, _ := lastInView.getter.Next(nil)
			require.Equal(8, len(v))
		}

		require.NotNil(lastOnFs.decompressor)
		hc.Close()
		require.Nil(lastOnFs.decompressor)

		nonDeletedOnFs, _ := h.files.Max()
		require.False(nonDeletedOnFs.frozen)
		require.NotNil(nonDeletedOnFs.decompressor) // non-canDelete files are not closed

		hc = h.MakeContext()
		newLastInView, _ := hc.historyFiles.Max()
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
