package jsonrpc

import (
	"reflect"
	"testing"

	"github.com/erigontech/erigon/core/types"
)

// makeLog simplifies creating log pointers for tests.
func makeLog(blockNum uint64, index uint) *types.ErigonLog {
	return &types.ErigonLog{
		BlockNumber: blockNum,
		Index:       index,
	}
}

// logKey represents the sortable keys of a log for comparison.
type logKey struct {
	Block uint64
	Index uint
}

// getLogKeys extracts the relevant keys from a slice of logs for easier comparison.
func getLogKeys(logs []*types.ErigonLog) []logKey {
	if logs == nil {
		return nil // Handle nil slice explicitly if it can happen
	}
	keys := make([]logKey, len(logs))
	for i, log := range logs {
		if log == nil {
			return nil
		} else {
			keys[i] = logKey{
				Block: log.BlockNumber,
				Index: log.Index,
			}
		}
	}
	return keys
}

func TestMergeSortedLogs(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name     string
		orig     []*types.ErigonLog
		new      []*types.ErigonLog
		expected []*types.ErigonLog
	}{
		{
			name:     "Both empty",
			orig:     []*types.ErigonLog{},
			new:      []*types.ErigonLog{},
			expected: []*types.ErigonLog{},
		},
		{
			name: "Original empty",
			orig: []*types.ErigonLog{},
			new: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 0),
			},
			expected: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 0),
			},
		},
		{
			name: "New empty",
			orig: []*types.ErigonLog{
				makeLog(15, 0),
				makeLog(25, 1),
			},
			new: []*types.ErigonLog{},
			expected: []*types.ErigonLog{
				makeLog(15, 0),
				makeLog(25, 1),
			},
		},
		{
			name: "No overlap - Orig first",
			orig: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 1),
			},
			new: []*types.ErigonLog{
				makeLog(30, 0),
				makeLog(40, 1),
			},
			expected: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 1),
				makeLog(30, 0),
				makeLog(40, 1),
			},
		},
		{
			name: "No overlap - New first",
			orig: []*types.ErigonLog{
				makeLog(30, 0),
				makeLog(40, 1),
			},
			new: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 1),
			},
			expected: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 1),
				makeLog(30, 0),
				makeLog(40, 1),
			},
		},
		{
			name: "Duplicate block number - stability check",
			orig: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 1), // Orig Block 20, Index 1
				makeLog(30, 0),
			},
			new: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(20, 5), // New Block 20, Index 5 (should come AFTER orig's due to stability)
				makeLog(40, 0),
			},
			expected: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(10, 0),
				makeLog(20, 1), // Orig Block 20 comes first
				makeLog(20, 5), // New Block 20 comes second
				makeLog(30, 0),
				makeLog(40, 0),
			},
		},
		{
			name: "Original exhausted first",
			orig: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(30, 0),
			},
			new: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(15, 0),
				makeLog(20, 0), // Orig pointer stops at 30
				makeLog(40, 0), // This and subsequent are added by new cleanup
				makeLog(50, 0),
			},
			expected: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(10, 0),
				makeLog(15, 0),
				makeLog(20, 0),
				makeLog(30, 0),
				makeLog(40, 0),
				makeLog(50, 0),
			},
		},
		{
			name: "New exhausted first",
			orig: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(15, 0),
				makeLog(20, 0), // New pointer stops at 10
				makeLog(40, 0), // This and subsequent are added by orig cleanup
				makeLog(50, 0),
			},
			new: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(30, 0),
			},
			expected: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(10, 0),
				makeLog(15, 0),
				makeLog(20, 0),
				makeLog(30, 0),
				makeLog(40, 0),
				makeLog(50, 0),
			},
		},
		{
			name: "Multiple duplicates interleaved",
			orig: []*types.ErigonLog{
				makeLog(10, 0),
				makeLog(20, 1), // Orig B20 I1
				makeLog(20, 3), // Orig B20 I3
				makeLog(40, 0), // Orig B40 I0
			},
			new: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(20, 5), // New B20 I5 (higher index)
				makeLog(20, 8), // New B20 I8 (higher index)
				makeLog(30, 0),
				makeLog(40, 2), // New B40 I2 (higher index)
			},
			expected: []*types.ErigonLog{
				makeLog(5, 0),
				makeLog(10, 0),
				makeLog(20, 1), // Orig B20 I1
				makeLog(20, 3), // Orig B20 I3
				makeLog(20, 5), // New B20 I5
				makeLog(20, 8), // New B20 I8
				makeLog(30, 0),
				makeLog(40, 0), // Orig B40 I0
				makeLog(40, 2), // New B40 I2
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := mergeSortedLogs(tc.orig, tc.new)

			actualKeys := getLogKeys(actual)
			if actualKeys == nil {
				t.Fatal("expected non-nil keys")
			}

			expectedKeys := getLogKeys(tc.expected)
			if expectedKeys == nil {
				t.Fatal("expected non-nil keys")
			}

			if !reflect.DeepEqual(actualKeys, expectedKeys) {
				t.Errorf("mergeSortedLogs failed for case '%s':\nExpected keys: %v\nActual keys:   %v",
					tc.name, expectedKeys, actualKeys)
			}
		})
	}
}
