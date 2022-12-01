package lightclient

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPowerOf2(t *testing.T) {
	// naively cover all test cases from 0 to 63
	for i := 0; i < 64; i++ {
		got := powerOf2(uint64(i))
		require.EqualValues(t, got, 1<<i)
	}
}
