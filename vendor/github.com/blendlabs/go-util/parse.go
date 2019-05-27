package util

import (
	"strconv"

	exception "github.com/blendlabs/go-exception"
)

// Parse contains utilities to parse strings.
var Parse = new(parseUtil)

type parseUtil struct{}

// ParseFloat64 parses a float64
func (pu parseUtil) Float64(input string) float64 {
	result, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return 0.0
	}
	return result
}

// ParseFloat32 parses a float32
func (pu parseUtil) Float32(input string) float32 {
	result, err := strconv.ParseFloat(input, 32)
	if err != nil {
		return 0.0
	}
	return float32(result)
}

// ParseInt parses an int
func (pu parseUtil) Int(input string) int {
	result, err := strconv.Atoi(input)
	if err != nil {
		return 0
	}
	return result
}

// ParseInt32 parses an int
func (pu parseUtil) Int32(input string) int32 {
	result, err := strconv.Atoi(input)
	if err != nil {
		return 0
	}
	return int32(result)
}

// ParseInt64 parses an int64
func (pu parseUtil) Int64(input string) int64 {
	result, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		return int64(0)
	}
	return result
}

// Ints parses a list of values as integers.
func (pu parseUtil) Ints(values ...string) (output []int, err error) {
	output = make([]int, len(values))
	var val int
	for i, v := range values {
		val, err = strconv.Atoi(v)
		if err != nil {
			err = exception.Wrap(err)
			return
		}
		output[i] = val
	}
	return
}

// Int64s parses a list of values as integers.
func (pu parseUtil) Int64s(values ...string) (output []int64, err error) {
	output = make([]int64, len(values))
	var val int64
	for i, v := range values {
		val, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			err = exception.Wrap(err)
			return
		}
		output[i] = val
	}
	return
}

// Float64s parses a list of values as floats.
func (pu parseUtil) Float64s(values ...string) (output []float64, err error) {
	output = make([]float64, len(values))
	var val float64
	for i, v := range values {
		val, err = strconv.ParseFloat(v, 64)
		if err != nil {
			err = exception.Wrap(err)
			return
		}
		output[i] = val
	}
	return
}
