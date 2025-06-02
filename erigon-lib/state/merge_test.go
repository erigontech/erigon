package state

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/kv"
)

func TestDomainRoTx_findMergeRange(t *testing.T) {
	t.Parallel()

	newDomainRoTx := func(aggStep uint64, files []visibleFile) *DomainRoTx {
		return &DomainRoTx{
			name:    kv.AccountsDomain,
			files:   files,
			aggStep: aggStep,
			ht:      &HistoryRoTx{iit: &InvertedIndexRoTx{}},
		}
	}

	createFile := func(startTxNum, endTxNum uint64) visibleFile {
		return visibleFile{
			startTxNum: startTxNum,
			endTxNum:   endTxNum,
			src:        &filesItem{startTxNum: startTxNum, endTxNum: endTxNum},
		}
	}

	t.Run("empty_and_single_file", func(t *testing.T) {
		dt := newDomainRoTx(1, []visibleFile{})
		result := dt.findMergeRange(100, 32)
		assert.False(t, result.values.needMerge)
		assert.Equal(t, uint64(1), result.aggStep)

		dt = newDomainRoTx(1, []visibleFile{createFile(0, 2)})
		result = dt.findMergeRange(4, 32)
		assert.False(t, result.values.needMerge)
	})

	t.Run("multiple_files_merge", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1),
			createFile(1, 2),
			createFile(2, 3),
		}
		dt := newDomainRoTx(1, files)
		result := dt.findMergeRange(4, 32)

		assert.True(t, result.values.needMerge)
		assert.Equal(t, uint64(0), result.values.from)
		assert.Equal(t, uint64(2), result.values.to)
	})

	t.Run("maxEndTxNum_filter", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1),
			createFile(1, 2),
			createFile(2, 4),
			createFile(4, 8),
		}
		dt := newDomainRoTx(1, files)
		result := dt.findMergeRange(3, 32)

		assert.True(t, result.values.needMerge)
		assert.Equal(t, uint64(0), result.values.from)
		assert.Equal(t, uint64(2), result.values.to)
	})

	t.Run("power_of_two_and_span_logic", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1),
			createFile(1, 2),
			createFile(2, 4),
			createFile(4, 8),
		}
		dt := newDomainRoTx(1, files)
		result := dt.findMergeRange(16, 32)

		if result.values.needMerge {
			assert.Equal(t, uint64(0), result.values.from)
		}

		dt = newDomainRoTx(2, []visibleFile{createFile(0, 2), createFile(2, 4)})
		result = dt.findMergeRange(8, 32)
		if result.values.needMerge {
			assert.Equal(t, uint64(0), result.values.from)
			assert.Equal(t, uint64(4), result.values.to)
		}
	})

	t.Run("aggregation_steps", func(t *testing.T) {
		for _, aggStep := range []uint64{1, 2, 4, 8} {
			endTx := aggStep * 4
			files := []visibleFile{
				createFile(0, aggStep),
				createFile(aggStep, aggStep*2),
			}
			dt := newDomainRoTx(aggStep, files)
			result := dt.findMergeRange(endTx, 32)
			assert.Equal(t, aggStep, result.aggStep)
		}
	})

	t.Run("edge_cases", func(t *testing.T) {
		files := []visibleFile{createFile(0, 1)}
		dt := newDomainRoTx(1, files)

		result := dt.findMergeRange(1, 32)
		assert.False(t, result.values.needMerge)

		files = []visibleFile{createFile(0, 1), createFile(1, 2)}
		dt = newDomainRoTx(1, files)
		result = dt.findMergeRange(4, 0)
		assert.False(t, result.values.needMerge)

		result = dt.findMergeRange(4, 1000000)
		if result.values.needMerge {
			assert.Equal(t, uint64(0), result.values.from)
		}
	})

	t.Run("file_ordering", func(t *testing.T) {
		files := []visibleFile{
			createFile(2, 4),
			createFile(0, 1),
			createFile(1, 2),
		}
		dt := newDomainRoTx(1, files)
		result := dt.findMergeRange(8, 32)

		if result.values.needMerge {
			assert.Equal(t, uint64(0), result.values.from)
		}
	})
}
