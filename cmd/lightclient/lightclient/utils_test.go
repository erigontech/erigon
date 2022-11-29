package lightclient

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPowerOf2(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			require.EqualValues(t, r, "integer overflow")
		}
	}()

	testCases := []struct {
		input       uint64
		expected    uint64
		shouldPanic bool
	}{
		{0, 1, false},
		{1, 2, false},
		{2, 4, false},
		{16, 65536, false},
		{64, 0, true},
	}

	for _, testCase := range testCases {
		got := powerOf2(testCase.input)
		if !testCase.shouldPanic {
			require.EqualValues(t, got, testCase.expected)
		}
	}
}
