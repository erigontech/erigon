package misc

import "testing"

func TestCalcGasLimitEIP7783(t *testing.T) {
	tests := []struct {
		blockNum         uint64
		startBlockNum    uint64
		initialGasLimit  uint64
		gasIncreaseRate  uint64
		gasLimitCap      uint64
		expectedGasLimit uint64
	}{
		{1, 1, 100000, 1000, 1000000, 100000},
		{1, 2, 100000, 1000, 1000000, 100000},
		{2, 1, 100000, 1000, 1000000, 101000},
		{2, 2, 100000, 1000, 1000000, 100000},
		{3, 2, 100000, 1000, 1000000, 101000},
		{10, 2, 100000, 1000, 1000000, 108000},
		{10, 2, 100000, 10000, 1000000, 180000},
	}

	for i, test := range tests {
		got := CalcGasLimitEIP7783(test.blockNum, test.startBlockNum, test.initialGasLimit, test.gasIncreaseRate, test.gasLimitCap)
		if got != test.expectedGasLimit {
			t.Errorf("Test %d: expected %d, got %d", i, test.expectedGasLimit, got)
		}
	}
}
