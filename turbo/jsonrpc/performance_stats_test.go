package jsonrpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetPerformanceStats(t *testing.T) {
	// Skip this test for now due to test setup complexity
	t.Skip("Skipping test due to complex test setup requirements")
}

func TestCalculateAverage(t *testing.T) {
	require := require.New(t)

	// Test empty slice
	require.Equal(0.0, calculateAverage([]float64{}))

	// Test single value
	require.Equal(5.0, calculateAverage([]float64{5.0}))

	// Test multiple values
	require.Equal(3.0, calculateAverage([]float64{1.0, 3.0, 5.0}))

	// Test with negative values
	require.Equal(0.0, calculateAverage([]float64{-2.0, 2.0}))
}

func TestCalculateMedian(t *testing.T) {
	require := require.New(t)

	// Test empty slice
	require.Equal(0.0, calculateMedian([]float64{}))

	// Test single value
	require.Equal(5.0, calculateMedian([]float64{5.0}))

	// Test odd number of values
	require.Equal(3.0, calculateMedian([]float64{1.0, 3.0, 5.0}))

	// Test even number of values
	require.Equal(3.5, calculateMedian([]float64{1.0, 3.0, 4.0, 6.0}))

	// Test with negative values
	require.Equal(0.0, calculateMedian([]float64{-2.0, 2.0}))

	// Test with unsorted values
	require.Equal(3.0, calculateMedian([]float64{5.0, 1.0, 3.0}))
}
