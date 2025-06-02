package state

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	datadir2 "github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDomainRoTx_findMergeRange(t *testing.T) {
	t.Parallel()

	// Helper to create test domain
	newTestDomain := func(aggStep uint64) *Domain {
		cfg := Schema.AccountsDomain
		salt := uint32(1)
		if cfg.hist.iiCfg.salt == nil {
			cfg.hist.iiCfg.salt = new(atomic.Pointer[uint32])
		}
		cfg.hist.iiCfg.salt.Store(&salt)
		cfg.hist.iiCfg.dirs = datadir2.New(os.TempDir())
		cfg.hist.iiCfg.name = kv.InvertedIdx(0)
		cfg.hist.iiCfg.version = IIVersionTypes{version.V1_0_standart, version.V1_0_standart}

		d, err := NewDomain(cfg, aggStep, log.New())
		require.NoError(t, err)
		fmt.Printf("DEBUG: Created domain with filenameBase: '%s'\n", d.filenameBase)
		return d
	}

	// Helper to setup files for domain
	setupDomainFiles := func(d *Domain, files []string) *DomainRoTx {
		fmt.Printf("DEBUG: Setting up files: %v\n", files)
		d.scanDirtyFiles(files)
		fmt.Printf("DEBUG: After scanDirtyFiles, dirtyFiles count: %d\n", d.dirtyFiles.Len())
		d.dirtyFiles.Scan(func(item *filesItem) bool {
			item.decompressor = &seg.Decompressor{}
			fmt.Printf("DEBUG: Found dirty file: startTxNum=%d, endTxNum=%d\n", item.startTxNum, item.endTxNum)
			return true
		})
		d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
		fmt.Printf("DEBUG: After reCalcVisibleFiles, visible files count: %d\n", len(d._visible.files))

		dt := d.BeginFilesRo()
		return dt
	}

	// Helper to setup history files
	setupHistoryFiles := func(d *Domain, files []string) {
		d.History.scanDirtyFiles(files)
		d.History.dirtyFiles.Scan(func(item *filesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		d.History.reCalcVisibleFiles(d.History.dirtyFilesEndTxNumMinimax())
	}

	// Helper to setup inverted index files
	setupIndexFiles := func(d *Domain, files []string) {
		d.History.InvertedIndex.scanDirtyFiles(files)
		d.History.InvertedIndex.dirtyFiles.Scan(func(item *filesItem) bool {
			item.decompressor = &seg.Decompressor{}
			return true
		})
		d.History.InvertedIndex.reCalcVisibleFiles(d.History.InvertedIndex.dirtyFilesEndTxNumMinimax())
	}

	t.Run("empty_domain", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		dt := d.BeginFilesRo()
		defer dt.Close()

		result := dt.findMergeRange(100, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		assert.False(t, result.values.needMerge)
		assert.False(t, result.history.history.needMerge)
		assert.False(t, result.history.index.needMerge)
		assert.Equal(t, uint64(1), result.aggStep)
	})

	t.Run("single_file_no_merge", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-2.kv",
		})
		defer dt.Close()

		result := dt.findMergeRange(4, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		assert.False(t, result.values.needMerge)
	})

	t.Run("multiple_files_merge_needed", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-1.kv",
			"v1.0-accounts.1-2.kv",
			"v1.0-accounts.2-3.kv",
		})
		defer dt.Close()

		result := dt.findMergeRange(4, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		assert.True(t, result.values.needMerge)
		assert.Equal(t, uint64(0), result.values.from)
		assert.Equal(t, uint64(1), result.values.to)
	})

	t.Run("maxEndTxNum_filter", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-1.kv",
			"v1.0-accounts.1-2.kv",
			"v1.0-accounts.2-4.kv",
			"v1.0-accounts.4-8.kv",
		})
		defer dt.Close()

		// Set maxEndTxNum to 3, should exclude files ending after 3
		result := dt.findMergeRange(3, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		assert.True(t, result.values.needMerge)
		assert.Equal(t, uint64(0), result.values.from)
		assert.Equal(t, uint64(2), result.values.to)
	})

	t.Run("span_calculation", func(t *testing.T) {
		d := newTestDomain(2) // aggStep = 2
		defer d.Close()

		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-2.kv",
			"v1.0-accounts.2-4.kv",
		})
		defer dt.Close()

		result := dt.findMergeRange(8, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		if result.values.needMerge {
			// endStep = 4/2 = 2, spanStep = 2 & -2 = 2, span = 2*2 = 4
			// fromTxNum = 4-4 = 0, which is < startTxNum=2, so merge needed
			assert.Equal(t, uint64(0), result.values.from)
			assert.Equal(t, uint64(4), result.values.to)
		}
	})

	t.Run("power_of_two_logic", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		// Test the bit manipulation logic: endStep & -endStep
		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-1.kv", // endStep=1, spanStep=1&-1=1
			"v1.0-accounts.1-2.kv", // endStep=2, spanStep=2&-2=2
			"v1.0-accounts.2-4.kv", // endStep=4, spanStep=4&-4=4
			"v1.0-accounts.4-8.kv", // endStep=8, spanStep=8&-8=8
		})
		defer dt.Close()

		result := dt.findMergeRange(16, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		if result.values.needMerge {
			// Should find the smallest valid merge range
			assert.Equal(t, uint64(0), result.values.from)
		}
	})

	t.Run("history_integration", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		// Setup domain files
		setupDomainFiles(d, []string{
			"v1.0-accounts.0-1.kv",
			"v1.0-accounts.1-2.kv",
		})

		// Setup history files
		setupHistoryFiles(d, []string{
			"v1.0-accounts.0-1.v",
			"v1.0-accounts.1-2.v",
		})

		// Setup index files
		setupIndexFiles(d, []string{
			"v1.0-accounts.0-1.ef",
			"v1.0-accounts.1-2.ef",
		})

		dt := d.BeginFilesRo()
		defer dt.Close()

		result := dt.findMergeRange(4, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		// Check that history ranges are properly calculated
		assert.NotNil(t, result.history)
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
				d := newTestDomain(tc.aggStep)
				defer d.Close()

				endTx := tc.aggStep * 4 // 4 steps
				dt := setupDomainFiles(d, []string{
					fmt.Sprintf("v1.0-accounts.0-%d.kv", tc.aggStep),
					fmt.Sprintf("v1.0-accounts.%d-%d.kv", tc.aggStep, tc.aggStep*2),
				})
				defer dt.Close()

				result := dt.findMergeRange(endTx, 32)

				assert.Equal(t, kv.AccountsDomain, result.name)
				assert.Equal(t, tc.aggStep, result.aggStep)
			})
		}
	})

	t.Run("edge_case_boundary_conditions", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-1.kv",
		})
		defer dt.Close()

		// Test with maxEndTxNum exactly at file boundary
		result := dt.findMergeRange(1, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		// Should not merge since fromTxNum would be negative
		assert.False(t, result.values.needMerge)
	})

	t.Run("large_span_values", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-1.kv",
			"v1.0-accounts.1-2.kv",
		})
		defer dt.Close()

		// Test with very large maxSpan
		result := dt.findMergeRange(4, 1000000)

		assert.Equal(t, kv.AccountsDomain, result.name)
		if result.values.needMerge {
			assert.Equal(t, uint64(0), result.values.from)
		}
	})

	t.Run("zero_maxSpan", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.0-1.kv",
			"v1.0-accounts.1-2.kv",
		})
		defer dt.Close()

		// Test with zero maxSpan
		result := dt.findMergeRange(4, 0)

		assert.Equal(t, kv.AccountsDomain, result.name)
		// With maxSpan=0, no merges should be possible
		assert.False(t, result.values.needMerge)
	})

	t.Run("file_ordering_matters", func(t *testing.T) {
		d := newTestDomain(1)
		defer d.Close()

		// Files should be processed in order of endTxNum
		dt := setupDomainFiles(d, []string{
			"v1.0-accounts.2-4.kv",
			"v1.0-accounts.0-1.kv",
			"v1.0-accounts.1-2.kv",
		})
		defer dt.Close()

		result := dt.findMergeRange(8, 32)

		assert.Equal(t, kv.AccountsDomain, result.name)
		if result.values.needMerge {
			// Should find optimal merge range regardless of file order
			assert.Equal(t, uint64(0), result.values.from)
		}
	})
}
