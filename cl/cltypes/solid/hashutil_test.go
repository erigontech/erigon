package solid_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestGetDepth(t *testing.T) {
	// Test cases with expected depths
	testCases := map[uint64]uint8{
		0:  0,
		1:  0,
		2:  1,
		3:  1,
		4:  2,
		5:  2,
		6:  2,
		7:  2,
		8:  3,
		9:  3,
		10: 3,
		16: 4,
		17: 4,
		32: 5,
		33: 5,
	}

	for v, expectedDepth := range testCases {
		actualDepth := solid.GetDepth(v)
		require.Equal(t, expectedDepth, actualDepth)
	}
}
