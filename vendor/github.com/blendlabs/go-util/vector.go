package util

import (
	"errors"
	"math"
)

var (
	// Vector is a namespace for vector functions.
	Vector = vectorUtil{}
)

type vectorUtil struct{}

// MultiplyByElement multiplies two slices elementwise.
func (vu vectorUtil) MultiplyByElement(a []float64, b []float64) ([]float64, error) {
	if len(a) != len(b) {
		return nil, errors.New("Arrays must be the same length for element multiplication")
	}

	result := make([]float64, len(a))
	for i := range a {
		result[i] = a[i] * b[i]
	}

	return result, nil
}

// Normalize normalizes a slice, if it is a zero vector, we return a vector of same size with an error.
func (vu vectorUtil) Normalize(input []float64) ([]float64, error) {
	if len(input) == 0 {
		return input, nil
	}

	result := make([]float64, len(input))
	m := vu.GetMagnitude(input)
	if Math.InEpsilon(m, 0) {
		return result, errors.New("zero vector")
	}
	for i := range input {
		result[i] = input[i] / m
	}

	return result, nil
}

// Magnitude returns the magnitude of a vector.
func (vu vectorUtil) GetMagnitude(a []float64) float64 {
	sum := float64(0)
	for _, elem := range a {
		sum += elem * elem
	}
	return math.Sqrt(sum)
}
