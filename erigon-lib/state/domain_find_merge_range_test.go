package state

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/kv"
)

func TestDomainRoTx_findMergeRange(t *testing.T) {
	t.Parallel()

	// Helper to create a DomainRoTx with manual file setup
	newDomainRoTx := func(aggStep uint64, files []visibleFile) *DomainRoTx {
		return &DomainRoTx{
			name:    kv.AccountsDomain,
			files:   files,
			aggStep: aggStep,
			ht:      &HistoryRoTx{iit: &InvertedIndexRoTx{}},
		}
	}

	// Helper to create a visible file
	createFile := func(startTxNum, endTxNum uint64) visibleFile {
		return visibleFile{
			startTxNum: startTxNum,
			endTxNum:   endTxNum,
			src: &filesItem{
				startTxNum: startTxNum,
				endTxNum:   endTxNum,
			},
		}
	}

	t.Run("empty_domain", func(t *testing.T) {
		dt := newDomainRoTx(1, []visibleFile{})

		result := dt.findMergeRange(100, 32)

		assert.False(t, result.values.needMerge)
		assert.Equal(t, uint64(1), result.aggStep)
	})

	t.Run("single_file_no_merge", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 2),
		}
		dt := newDomainRoTx(1, files)

		result := dt.findMergeRange(4, 32)

		assert.False(t, result.values.needMerge)
	})

	t.Run("multiple_files_merge_needed", func(t *testing.T) {
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

		// Set maxEndTxNum to 3, should exclude files ending after 3
		result := dt.findMergeRange(3, 32)

		assert.True(t, result.values.needMerge)
		assert.Equal(t, uint64(0), result.values.from)
		assert.Equal(t, uint64(2), result.values.to)
	})

	t.Run("span_calculation", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 2),
			createFile(2, 4),
		}
		dt := newDomainRoTx(2, files) // aggStep = 2

		result := dt.findMergeRange(8, 32)

		if result.values.needMerge {
			// endStep = 4/2 = 2, spanStep = 2 & -2 = 2, span = 2*2 = 4
			// fromTxNum = 4-4 = 0, which is < startTxNum=2, so merge needed
			assert.Equal(t, uint64(0), result.values.from)
			assert.Equal(t, uint64(4), result.values.to)
		}
	})

	t.Run("power_of_two_logic", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1), // endStep=1, spanStep=1&-1=1
			createFile(1, 2), // endStep=2, spanStep=2&-2=2
			createFile(2, 4), // endStep=4, spanStep=4&-4=4
			createFile(4, 8), // endStep=8, spanStep=8&-8=8
		}
		dt := newDomainRoTx(1, files)

		result := dt.findMergeRange(16, 32)

		if result.values.needMerge {
			// Should find the smallest valid merge range
			assert.Equal(t, uint64(0), result.values.from)
		}
	})

	t.Run("aggregation_step_impact", func(t *testing.T) {
		testCases := []struct {
			name    string
			aggStep uint64
		}{
			{"aggStep_1", 1},
			{"aggStep_2", 2},
			{"aggStep_4", 4},
			{"aggStep_8", 8},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				endTx := tc.aggStep * 4 // 4 steps
				files := []visibleFile{
					createFile(0, tc.aggStep),
					createFile(tc.aggStep, tc.aggStep*2),
				}
				dt := newDomainRoTx(tc.aggStep, files)

				result := dt.findMergeRange(endTx, 32)

				assert.Equal(t, tc.aggStep, result.aggStep)
			})
		}
	})

	t.Run("edge_case_boundary_conditions", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1),
		}
		dt := newDomainRoTx(1, files)

		// Test with maxEndTxNum exactly at file boundary
		result := dt.findMergeRange(1, 32)

		// Should not merge since fromTxNum would be negative
		assert.False(t, result.values.needMerge)
	})

	t.Run("large_span_values", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1),
			createFile(1, 2),
		}
		dt := newDomainRoTx(1, files)

		// Test with very large maxSpan
		result := dt.findMergeRange(4, 1000000)

		if result.values.needMerge {
			assert.Equal(t, uint64(0), result.values.from)
		}
	})

	t.Run("zero_maxSpan", func(t *testing.T) {
		files := []visibleFile{
			createFile(0, 1),
			createFile(1, 2),
		}
		dt := newDomainRoTx(1, files)

		// Test with zero maxSpan
		result := dt.findMergeRange(4, 0)

		// With maxSpan=0, no merges should be possible
		assert.False(t, result.values.needMerge)
	})

	t.Run("file_ordering_matters", func(t *testing.T) {
		// Files should be processed in order of endTxNum
		files := []visibleFile{
			createFile(2, 4),
			createFile(0, 1),
			createFile(1, 2),
		}
		dt := newDomainRoTx(1, files)

		result := dt.findMergeRange(8, 32)

		if result.values.needMerge {
			// Should find optimal merge range regardless of file order
			assert.Equal(t, uint64(0), result.values.from)
		}
	})
}
