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

	// naively cover all test cases from 0 to 63
	for i := 0; i < 64; i++ {
		got := powerOf2(uint64(i))
		require.EqualValues(t, got, 1<<i)
	}

	// check that 64 panics
	powerOf2(64)
}
