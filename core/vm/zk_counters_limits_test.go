package vm

import (
	"testing"

	zk_consts "github.com/ledgerwatch/erigon-lib/chain"
)

func TestGetTotalSteps(t *testing.T) {
	tests := []struct {
		name     string
		forkID   uint16
		expected int
	}{
		{
			name:     "Pre ForkID10",
			forkID:   uint16(zk_consts.ForkID9Elderberry2),
			expected: preForkId10TotalSteps, // 1 << 23
		},
		{
			name:     "ForkID10",
			forkID:   uint16(zk_consts.ForkID10),
			expected: forkId10TotalSteps - stepDeduction,
		},
		{
			name:     "ForkID11",
			forkID:   uint16(zk_consts.ForkID11),
			expected: forkId11TotalSteps - stepDeduction,
		},
		{
			name:     "ForkID12",
			forkID:   uint16(zk_consts.ForkID12Banana),
			expected: forkId11TotalSteps - stepDeduction,
		},
		{
			name:     "ForkID13",
			forkID:   uint16(zk_consts.ForkId13Durian),
			expected: forkId11TotalSteps - stepDeduction,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getTotalSteps(tt.forkID)
			if got != tt.expected {
				t.Errorf("getTotalSteps(%d) = %d, want %d", tt.forkID, got, tt.expected)
			}
		})
	}
}

func TestCounterLimitsProgression(t *testing.T) {
	// Test that forkid progression is correct from for 9 to 100
	startForkID := uint16(zk_consts.ForkID9Elderberry2)
	endForkID := uint16(100)

	var prevLimit int
	for forkID := startForkID; forkID <= endForkID; forkID++ {
		currentLimit := getTotalSteps(forkID)

		if prevLimit > currentLimit {
			t.Errorf("ForkID %d has fewer steps (%d) than previous forkID %d (%d)!", forkID, currentLimit, forkID-1, prevLimit)
		}

		prevLimit = currentLimit
	}
}
