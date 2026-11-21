package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/seg"
)

// visibleItem builds a FilesItem that passes checkForVisibility for accessors==0
// (only a non-nil decompressor is required).
func visibleItem(startTxNum, endTxNum uint64) *FilesItem {
	it := newFilesItem(startTxNum, endTxNum)
	it.decompressor = &seg.Decompressor{}
	return it
}

func TestCoveredByVisibleFiles(t *testing.T) {
	t.Parallel()
	merged := visibleItem(0, 4)
	sub02 := visibleItem(0, 2)
	sub24 := visibleItem(2, 4)

	t.Run("covered by contiguous subs", func(t *testing.T) {
		require.True(t, coveredByVisibleFiles(merged, []*FilesItem{sub02, sub24, merged}, 0))
	})
	t.Run("uncovered when no subs", func(t *testing.T) {
		require.False(t, coveredByVisibleFiles(merged, []*FilesItem{merged}, 0))
	})
	t.Run("uncovered with a gap in subs", func(t *testing.T) {
		require.False(t, coveredByVisibleFiles(merged, []*FilesItem{sub02, merged}, 0))
	})
	t.Run("uncovered when a sub is not visible", func(t *testing.T) {
		invisible := newFilesItem(2, 4) // no decompressor => not visible
		require.False(t, coveredByVisibleFiles(merged, []*FilesItem{sub02, invisible, merged}, 0))
	})
}

func TestDropCoveredAccessors(t *testing.T) {
	t.Parallel()
	merged := visibleItem(0, 4)
	sub02 := visibleItem(0, 2)
	sub24 := visibleItem(2, 4)

	t.Run("drops merged file covered by its subs", func(t *testing.T) {
		got := dropCoveredAccessors([]*FilesItem{merged}, []*FilesItem{sub02, sub24, merged}, 0)
		require.Empty(t, got)
	})
	t.Run("keeps merged file whose subs are gone", func(t *testing.T) {
		got := dropCoveredAccessors([]*FilesItem{merged}, []*FilesItem{merged}, 0)
		require.Equal(t, []*FilesItem{merged}, got)
	})
	t.Run("keeps a frontier file with no subsets", func(t *testing.T) {
		frontier := visibleItem(4, 6)
		got := dropCoveredAccessors([]*FilesItem{frontier}, []*FilesItem{sub02, sub24, frontier}, 0)
		require.Equal(t, []*FilesItem{frontier}, got)
	})
}
