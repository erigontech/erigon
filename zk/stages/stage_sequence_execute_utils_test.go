package stages

import (
	"testing"

	zktx "github.com/ledgerwatch/erigon/zk/tx"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
)

type mockChecker struct {
	data map[uint64]zktypes.L1InfoTreeUpdate
}

func (m *mockChecker) GetL1InfoTreeUpdate(idx uint64) (*zktypes.L1InfoTreeUpdate, error) {
	if val, ok := m.data[idx]; ok {
		return &val, nil
	}
	return nil, nil
}

func Test_CheckForBadBatch(t *testing.T) {
	tests := map[string]struct {
		data                map[uint64]zktypes.L1InfoTreeUpdate
		blocks              []zktx.DecodedBatchL2Data
		latestTimestamp     uint64
		timestampLimit      uint64
		highestAllowedIndex uint64
		expectedBadBatch    bool
	}{
		"no_issues": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 1000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      9999,
			expectedBadBatch:    false,
		},
		"missing_l1_info_tree_update": {
			latestTimestamp: 500,
			data:            map[uint64]zktypes.L1InfoTreeUpdate{},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      9999,
			expectedBadBatch:    true,
		},
		"l1_info_update_in_the_future": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 3000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      9999,
			expectedBadBatch:    true,
		},
		"timestamp_exceeds_timestamp_limit": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 3000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  1000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 100,
			timestampLimit:      100,
			expectedBadBatch:    true,
		},
		"index_in_the_future": {
			latestTimestamp: 500,
			data: map[uint64]zktypes.L1InfoTreeUpdate{
				1: {
					Index:     1,
					Timestamp: 3000,
				},
			},
			blocks: []zktx.DecodedBatchL2Data{
				{
					DeltaTimestamp:  9000,
					L1InfoTreeIndex: 1,
				},
			},
			highestAllowedIndex: 0,
			timestampLimit:      9999,
			expectedBadBatch:    true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &mockChecker{
				data: tt.data,
			}
			bad, err := checkForBadBatch(1, m, tt.latestTimestamp, tt.highestAllowedIndex, tt.timestampLimit, tt.blocks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if bad != tt.expectedBadBatch {
				t.Fatalf("expected bad batch: %v, got: %v", tt.expectedBadBatch, bad)
			}
		})
	}
}

type mockForkDb struct {
	allForks   []uint64
	allBatches []uint64
	batchForks map[uint64]uint64
}

func (m mockForkDb) GetAllForkHistory() ([]uint64, []uint64, error) {
	return m.allForks, m.allBatches, nil
}

func (m mockForkDb) GetLatestForkHistory() (uint64, uint64, error) {
	return m.allForks[len(m.allForks)-1], m.allBatches[len(m.allBatches)-1], nil

}

func (m mockForkDb) GetForkId(batch uint64) (uint64, error) {
	return m.batchForks[batch], nil
}

func (m mockForkDb) WriteForkIdBlockOnce(forkId, block uint64) error {
	return nil
}

func (m mockForkDb) WriteForkId(batch, forkId uint64) error {
	return nil
}

func Test_PrepareForkId_DuringRecovery(t *testing.T) {
	tests := map[string]struct {
		lastBatch  uint64
		allForks   []uint64
		allBatches []uint64
		batchForks map[uint64]uint64
		expected   uint64
	}{
		"simple case": {
			lastBatch:  2,
			allForks:   []uint64{1, 2, 3},
			allBatches: []uint64{1, 2, 3},
			batchForks: map[uint64]uint64{1: 1},
			expected:   2,
		},
		"with gaps": {
			lastBatch:  22,
			allForks:   []uint64{1, 2, 3},
			allBatches: []uint64{10, 20, 30},
			batchForks: map[uint64]uint64{1: 1},
			expected:   2,
		},
		"early in restore": {
			lastBatch:  2,
			allForks:   []uint64{1, 2, 3},
			allBatches: []uint64{1, 20, 30},
			batchForks: map[uint64]uint64{1: 1},
			expected:   1,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mock := mockForkDb{
				allForks:   test.allForks,
				allBatches: test.allBatches,
				batchForks: test.batchForks,
			}

			forkId, err := prepareForkId(test.lastBatch, 1, mock)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if forkId != test.expected {
				t.Errorf("expected: %v, got: %v", test.expected, forkId)
			}
		})
	}
}
