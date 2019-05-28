package util

import (
	"testing"
	"time"

	"github.com/blendlabs/go-assert"
)

func TestMin(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	minValue := Math.Min(values)
	assert.Equal(1.0, minValue)
}

func TestMinRev(t *testing.T) {
	assert := assert.New(t)
	values := []float64{10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0}
	minValue := Math.Min(values)
	assert.Equal(1.0, minValue)
}

func TestMinEmpty(t *testing.T) {
	assert := assert.New(t)
	min := Math.Min([]float64{})
	assert.Zero(min)
}

func TestMax(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	maxValue := Math.Max(values)
	assert.Equal(10.0, maxValue)
}

func TestMaxEmpty(t *testing.T) {
	assert := assert.New(t)
	max := Math.Max([]float64{})
	assert.Zero(max)
}

func TestSum(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	sum := Math.Sum(values)
	assert.Equal(55.0, sum)
}

func TestSumOfDuration(t *testing.T) {
	assert := assert.New(t)
	values := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second}
	sum := Math.SumOfDuration(values)
	assert.Equal(6*time.Second, sum)
}

func TestSumEmpty(t *testing.T) {
	assert := assert.New(t)
	sum := Math.Sum([]float64{})
	assert.Zero(sum)
}

func TestMean(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	mean := Math.Mean(values)
	assert.Equal(5.5, mean)
}

func TestMeanOfDuration(t *testing.T) {
	assert := assert.New(t)
	values := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second}
	mean := Math.MeanOfDuration(values)
	assert.Equal(2*time.Second, mean)
}

func TestMeanEmpty(t *testing.T) {
	assert := assert.New(t)
	mean := Math.Mean([]float64{})
	assert.Zero(mean)
}

func TestMedian(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	value := Math.Median(values)
	assert.Equal(5.5, value)
}

func TestMedianOdd(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0}
	value := Math.Median(values)
	assert.Equal(2.0, value)
}

func TestMedianEmpty(t *testing.T) {
	assert := assert.New(t)
	median := Math.Median([]float64{})
	assert.Zero(median)
}

func TestModeSingleInput(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0}
	value := Math.Mode(values)
	assert.Equal([]float64{1.0}, value)
}

func TestModeAllDifferent(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	value := Math.Mode(values)
	assert.Equal([]float64{}, value)
}

func TestModeOddLength(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 1.0, 3.0, 4.0, 5.0}
	value := Math.Mode(values)
	assert.Equal([]float64{1.0}, value)
}

func TestModeEvenLength(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 1.0, 1.0, 2.0, 3.0, 4.0}
	value := Math.Mode(values)
	assert.Equal([]float64{1.0}, value)
}

func TestModeEvenLengthExtra(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 5.0, 5.0}
	value := Math.Mode(values)
	assert.Equal([]float64{5.0}, value)
}

func TestModeEmpty(t *testing.T) {
	assert := assert.New(t)
	empty := Math.Mode([]float64{})
	assert.Empty(empty)
}

func TestVariance(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	value := Math.Variance(values, 0)
	assert.Equal(8.25, value)
}

func TestVarianceEmpty(t *testing.T) {
	assert := assert.New(t)
	zero := Math.Variance([]float64{}, 0)
	assert.Zero(zero)
}

func TestVarP(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	value := Math.VarP(values)
	assert.Equal(8.25, value)
}

func TestVarPEmpty(t *testing.T) {
	assert := assert.New(t)
	zero := Math.VarP([]float64{})
	assert.Zero(zero)
}

func TestVarS(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}
	value := Math.VarS(values)
	assert.Equal(7.5, value)
}

func TestVarSEmpty(t *testing.T) {
	assert := assert.New(t)
	zero := Math.VarS([]float64{})
	assert.Zero(zero)
}

func TestStdDevP(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	value := Math.StdDevP(values)
	assert.InDelta(2.872, value, 0.001)
}

func TestStdDevPZero(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0}
	value := Math.StdDevP(values)
	assert.Equal(0.0, value)
}

func TestStdDevPEmpty(t *testing.T) {
	assert := assert.New(t)
	zero := Math.StdDevP([]float64{})
	assert.Zero(zero)
}

func TestStdDevS(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	value := Math.StdDevS(values)
	assert.InDelta(3.027, value, 0.001)
}

func TestStdDevSZero(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 1.0, 1.0, 1.0, 1.0, 1.0}
	value := Math.StdDevS(values)
	assert.Equal(0.0, value)
}

func TestStdDevSEmpty(t *testing.T) {
	assert := assert.New(t)
	zero := Math.StdDevS([]float64{})
	assert.Zero(zero)
}

func TestRound(t *testing.T) {
	assert := assert.New(t)
	value := Math.Round(0.55, 1)
	assert.InDelta(0.6, value, 0.01)
}

func TestRoundDown(t *testing.T) {
	assert := assert.New(t)
	value := Math.Round(0.54, 1)
	assert.InDelta(0.5, value, 0.01)
}

func TestRoundNegative(t *testing.T) {
	assert := assert.New(t)

	value := Math.Round(-0.55, 1)
	assert.InDelta(-0.6, value, 0.01)
}

func TestPercentile(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	value := Math.Percentile(values, 90.0)
	assert.InDelta(9.5, value, 0.0001)
}

func TestPercentileNonInteger(t *testing.T) {
	assert := assert.New(t)
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	value := Math.Percentile(values, 92.0)
	assert.InDelta(9.0, value, 0.0001)
}

func TestPercentileEmpty(t *testing.T) {
	assert := assert.New(t)
	zero := Math.Percentile([]float64{}, 80.0)
	assert.Zero(zero)
}

func TestInEpsilon(t *testing.T) {
	assert := assert.New(t)

	assert.True(Math.InEpsilon(0.0, 1-1))
	assert.False(Math.InEpsilon(0.001, 1-1))
}
