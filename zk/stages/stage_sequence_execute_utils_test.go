package stages

import (
	"testing"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
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
		data             map[uint64]zktypes.L1InfoTreeUpdate
		blocks           []zktx.DecodedBatchL2Data
		latestTimestamp  uint64
		expectedBadBatch bool
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
			expectedBadBatch: false,
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
			expectedBadBatch: true,
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
			expectedBadBatch: true,
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
			expectedBadBatch: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			m := &mockChecker{
				data: tt.data,
			}
			bad, err := checkForBadBatch(m, tt.latestTimestamp, tt.blocks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if bad != tt.expectedBadBatch {
				t.Fatalf("expected bad batch: %v, got: %v", tt.expectedBadBatch, bad)
			}
		})
	}
}
