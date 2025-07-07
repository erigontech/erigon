package vm

import (
	"testing"

	zk_consts "github.com/erigontech/erigon-lib/chain"
)

func Test_EmptyBlocksByFork(t *testing.T) {
	tests := []struct {
		name     string
		forkID   uint16
		expected int
	}{
		{
			name:     "ForkID12",
			forkID:   uint16(zk_consts.ForkID12Banana),
			expected: 3450,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// using a "middle of the road" value for smt depth and the default mcp reduction
			batch := NewBatchCounterCollector(25, tt.forkID, 0.6, false, nil)

			count := 0
			for {
				overflow, err := batch.StartNewBlock(false)
				if err != nil {
					t.Fatalf("error starting new block: %v", err)
				}
				if overflow {
					break
				}
				count++
			}

			if count != tt.expected {
				t.Errorf("expected %d empty blocks, got %d", tt.expected, count)
			}
		})
	}
}
