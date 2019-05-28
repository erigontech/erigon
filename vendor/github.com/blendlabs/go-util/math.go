package util

import (
	"math"
	"sort"
	"time"
)

var (
	// Math is a namespace for math utilities.
	Math = mathUtil{}
)

const (
	_pi   = math.Pi
	_2pi  = 2 * math.Pi
	_3pi4 = (3 * math.Pi) / 4.0
	_4pi3 = (4 * math.Pi) / 3.0
	_3pi2 = (3 * math.Pi) / 2.0
	_5pi4 = (5 * math.Pi) / 4.0
	_7pi4 = (7 * math.Pi) / 4.0
	_pi2  = math.Pi / 2.0
	_pi4  = math.Pi / 4.0
	_d2r  = (math.Pi / 180.0)
	_r2d  = (180.0 / math.Pi)

	// Epsilon represents the minimum amount of relevant delta we care about.
	Epsilon = 0.00000001
)

type mathUtil struct{}

// PowOfInt returns the base to the power.
func (mu mathUtil) PowOfInt(base, power uint) int {
	if base == 2 {
		return 1 << power
	}
	return float64ToInt(math.Pow(float64(base), float64(power)))
}

// MinAndMax returns both the min and max in one pass.
func (mu mathUtil) MinAndMax(values ...float64) (min float64, max float64) {
	if len(values) == 0 {
		return
	}
	min = values[0]
	max = values[0]
	for _, v := range values {
		if max < v {
			max = v
		}
		if min > v {
			min = v
		}
	}
	return
}

// MinAndMaxOfInt returns both the min and max in one pass.
func (mu mathUtil) MinAndMaxOfInt(values ...int) (min int, max int) {
	if len(values) == 0 {
		return
	}
	min = values[0]
	max = values[0]
	for _, v := range values {
		if max < v {
			max = v
		}
		if min > v {
			min = v
		}
	}
	return
}

// MinAndMaxOfInt64 returns both the min and max in one pass.
func (mu mathUtil) MinAndMaxOfInt64(values ...int64) (min int64, max int64) {
	if len(values) == 0 {
		return
	}
	min = values[0]
	max = values[0]
	for _, v := range values {
		if max < v {
			max = v
		}
		if min > v {
			min = v
		}
	}
	return
}

// Min finds the lowest value in a slice.
func (mu mathUtil) Min(input []float64) float64 {
	if len(input) == 0 {
		return 0
	}

	min := input[0]

	for i := 1; i < len(input); i++ {
		if input[i] < min {
			min = input[i]
		}
	}
	return min
}

// MinOfInt finds the lowest value in a slice.
func (mu mathUtil) MinOfInt(input []int) int {
	if len(input) == 0 {
		return 0
	}

	min := input[0]

	for i := 1; i < len(input); i++ {
		if input[i] < min {
			min = input[i]
		}
	}
	return min
}

// MinOfDuration finds the lowest value in a slice.
func (mu mathUtil) MinOfDuration(input []time.Duration) time.Duration {
	if len(input) == 0 {
		return time.Duration(0)
	}

	min := input[0]

	for i := 1; i < len(input); i++ {
		if input[i] < min {
			min = input[i]
		}
	}
	return min
}

// Max finds the highest value in a slice.
func (mu mathUtil) Max(input []float64) float64 {

	if len(input) == 0 {
		return 0
	}

	max := input[0]

	for i := 1; i < len(input); i++ {
		if input[i] > max {
			max = input[i]
		}
	}

	return max
}

// MaxOfInt finds the highest value in a slice.
func (mu mathUtil) MaxOfInt(input []int) int {
	if len(input) == 0 {
		return 0
	}

	max := input[0]

	for i := 1; i < len(input); i++ {
		if input[i] > max {
			max = input[i]
		}
	}

	return max
}

// MaxOfDuration finds the highest value in a slice.'
func (mu mathUtil) MaxOfDuration(input []time.Duration) time.Duration {
	if len(input) == 0 {
		return time.Duration(0)
	}

	max := input[0]

	for i := 1; i < len(input); i++ {
		if input[i] > max {
			max = input[i]
		}
	}

	return max
}

// Sum adds all the numbers of a slice together
func (mu mathUtil) Sum(input []float64) float64 {

	if len(input) == 0 {
		return 0
	}

	sum := float64(0)

	// Add em up
	for _, n := range input {
		sum += n
	}

	return sum
}

// SumOfInt adds all the numbers of a slice together
func (mu mathUtil) SumOfInt(values []int) int {
	total := 0
	for x := 0; x < len(values); x++ {
		total += values[x]
	}

	return total
}

// SumOfDuration adds all the values of a slice together
func (mu mathUtil) SumOfDuration(values []time.Duration) time.Duration {
	total := time.Duration(0)
	for x := 0; x < len(values); x++ {
		total += values[x]
	}

	return total
}

// Mean gets the average of a slice of numbers
func (mu mathUtil) Mean(input []float64) float64 {
	if len(input) == 0 {
		return 0
	}

	sum := mu.Sum(input)

	return sum / float64(len(input))
}

// MeanOfInt gets the average of a slice of numbers
func (mu mathUtil) MeanOfInt(input []int) float64 {
	if len(input) == 0 {
		return 0
	}

	sum := mu.SumOfInt(input)
	return float64(sum) / float64(len(input))
}

// MeanOfDuration gets the average of a slice of numbers
func (mu mathUtil) MeanOfDuration(input []time.Duration) time.Duration {
	if len(input) == 0 {
		return 0
	}

	sum := mu.SumOfDuration(input)
	mean := uint64(sum) / uint64(len(input))
	return time.Duration(mean)
}

// Median gets the median number in a slice of numbers
func (mu mathUtil) Median(input []float64) float64 {
	median := float64(0)
	l := len(input)
	if l == 0 {
		return 0
	}
	c := copyslice(input)
	sort.Float64s(c)

	if l%2 == 0 {
		median = mu.Mean(c[l/2-1 : l/2+1])
	} else {
		median = float64(c[l/2])
	}

	return median
}

// Mode gets the mode of a slice of numbers
// `Mode` generally is the most frequently occurring values within the input set.
func (mu mathUtil) Mode(input []float64) []float64 {

	l := len(input)
	if l == 1 {
		return input
	} else if l == 0 {
		return []float64{}
	}

	m := make(map[float64]int)
	for _, v := range input {
		m[v]++
	}

	mode := []float64{}

	var current int
	for k, v := range m {
		switch {
		case v < current:
		case v > current:
			current = v
			mode = append(mode[:0], k)
		default:
			mode = append(mode, k)
		}
	}

	lm := len(mode)
	if l == lm {
		return []float64{}
	}

	return mode
}

// Variance finds the variance for both population and sample data
func (mu mathUtil) Variance(input []float64, sample int) float64 {

	if len(input) == 0 {
		return 0
	}

	variance := float64(0)
	m := mu.Mean(input)

	for _, n := range input {
		variance += (float64(n) - m) * (float64(n) - m)
	}

	// When getting the mean of the squared differences
	// "sample" will allow us to know if it's a sample
	// or population and wether to subtract by one or not
	return variance / float64((len(input) - (1 * sample)))
}

// VarP finds the amount of variance within a population
func (mu mathUtil) VarP(input []float64) float64 {
	return mu.Variance(input, 0)
}

// VarS finds the amount of variance within a sample
func (mu mathUtil) VarS(input []float64) float64 {
	return mu.Variance(input, 1)
}

// StdDevP finds the amount of variation from the population
func (mu mathUtil) StdDevP(input []float64) float64 {

	if len(input) == 0 {
		return 0
	}

	// stdev is generally the square root of the variance
	return math.Pow(mu.VarP(input), 0.5)
}

// StdDevS finds the amount of variation from a sample
func (mu mathUtil) StdDevS(input []float64) float64 {

	if len(input) == 0 {
		return 0
	}

	// stdev is generally the square root of the variance
	return math.Pow(mu.VarS(input), 0.5)
}

// Round a float to a specific decimal place or precision
func (mu mathUtil) Round(input float64, places int) float64 {
	if math.IsNaN(input) {
		return 0.0
	}

	sign := 1.0
	if input < 0 {
		sign = -1
		input *= -1
	}

	rounded := float64(0)
	precision := math.Pow(10, float64(places))
	digit := input * precision
	_, decimal := math.Modf(digit)

	if decimal >= 0.5 {
		rounded = math.Ceil(digit)
	} else {
		rounded = math.Floor(digit)
	}

	return rounded / precision * sign
}

// RoundUp rounds up to a given roundTo value.
func (mu mathUtil) RoundUp(value, roundTo float64) float64 {
	d1 := math.Ceil(value / roundTo)
	return d1 * roundTo
}

// RoundDown rounds down to a given roundTo value.
func (mu mathUtil) RoundDown(value, roundTo float64) float64 {
	d1 := math.Floor(value / roundTo)
	return d1 * roundTo
}

// Percentile finds the relative standing in a slice of floats.
// `percent` should be given on the interval [0,100.0).
func (mu mathUtil) Percentile(input []float64, percent float64) float64 {
	if len(input) == 0 {
		return 0
	}

	c := copyslice(input)
	sort.Float64s(c)
	index := (percent / 100.0) * float64(len(c))

	percentile := float64(0)
	if index == float64(int64(index)) {
		i := float64ToInt(index)
		percentile = mu.Mean([]float64{c[i-1], c[i]})
	} else {
		i := float64ToInt(index)
		percentile = c[i-1]
	}

	return percentile
}

type durations []time.Duration

func (dl durations) Len() int {
	return len(dl)
}

func (dl durations) Less(i, j int) bool {
	return dl[i] < dl[j]
}

func (dl durations) Swap(i, j int) {
	dl[i], dl[j] = dl[j], dl[i]
}

// Percentile finds the relative standing in a slice of floats
func (mu mathUtil) PercentileOfDuration(input []time.Duration, percentile float64) time.Duration {
	if len(input) == 0 {
		return 0
	}

	sort.Sort(durations(input))
	index := (percentile / 100.0) * float64(len(input))
	if index == float64(int64(index)) {
		i := int(Math.Round(index, 0))

		if i < 1 {
			return time.Duration(0)
		}

		return mu.MeanOfDuration([]time.Duration{input[i-1], input[i]})
	}

	i := int(Math.Round(index, 0))
	if i < 1 {
		return time.Duration(0)
	}

	return input[i-1]
}

// AbsInt returns the absolute value of an integer.
func (mu mathUtil) AbsInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

// Normalize returns a set of numbers on the interval [0,1] for a given set of inputs.
// An example: 4,3,2,1 => 0.4, 0.3, 0.2, 0.1
// Caveat; the total may be < 1.0; there are going to be issues with irrational numbers etc.
func (mu mathUtil) Normalize(values ...float64) []float64 {
	var total float64
	for _, v := range values {
		total += v
	}
	output := make([]float64, len(values))
	for x, v := range values {
		output[x] = mu.RoundDown(v/total, 0.0001)
	}
	return output
}

// PercentDifference computes the percentage difference between two values.
// The formula is (v2-v1)/v1.
func (mu mathUtil) PercentDifference(v1, v2 float64) float64 {
	if v1 == 0 {
		return 0
	}
	return (v2 - v1) / v1
}

// DegreesToRadians returns degrees as radians.
func (mu mathUtil) DegreesToRadians(degrees float64) float64 {
	return degrees * _d2r
}

// RadiansToDegrees translates a radian value to a degree value.
func (mu mathUtil) RadiansToDegrees(value float64) float64 {
	return math.Mod(value, _2pi) * _r2d
}

// PercentToRadians converts a normalized value (0,1) to radians.
func (mu mathUtil) PercentToRadians(pct float64) float64 {
	return mu.DegreesToRadians(360.0 * pct)
}

// RadianAdd adds a delta to a base in radians.
func (mu mathUtil) RadianAdd(base, delta float64) float64 {
	value := base + delta
	if value > _2pi {
		return math.Mod(value, _2pi)
	} else if value < 0 {
		return math.Mod(_2pi+value, _2pi)
	}
	return value
}

// DegreesAdd adds a delta to a base in radians.
func (mu mathUtil) DegreesAdd(baseDegrees, deltaDegrees float64) float64 {
	value := baseDegrees + deltaDegrees
	if value > _2pi {
		return math.Mod(value, 360.0)
	} else if value < 0 {
		return math.Mod(360.0+value, 360.0)
	}
	return value
}

// DegreesToCompass returns the degree value in compass / clock orientation.
func (mu mathUtil) DegreesToCompass(deg float64) float64 {
	return mu.DegreesAdd(deg, -90.0)
}

func (mu mathUtil) InEpsilon(a, b float64) bool {
	return (a-b) < Epsilon && (b-a) < Epsilon
}

// float64ToInt rounds a float64 to an int
func float64ToInt(input float64) (output int) {
	r := Math.Round(input, 0)
	return int(r)
}

// copyslice copies a slice of float64s
func copyslice(input []float64) []float64 {
	s := make([]float64, len(input))
	copy(s, input)
	return s
}
