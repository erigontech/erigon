package chart

import (
	"fmt"
	"math"
)

// LogRange represents a boundary for a set of numbers on logarithmic scale.
type LogRange struct {
	Min        float64
	Max        float64
	Domain     int
	Descending bool
}

// IsDescending returns if the range is descending.
func (r LogRange) IsDescending() bool {
	return r.Descending
}

// IsZero returns if the LogRange has been set or not.
func (r LogRange) IsZero() bool {
	return (r.Min == 0 || math.IsNaN(r.Min)) &&
		(r.Max == 0 || math.IsNaN(r.Max)) &&
		r.Domain == 0
}

// GetMin gets the min value for the continuous range.
func (r LogRange) GetMin() float64 {
	return r.Min
}

// SetMin sets the min value for the continuous range.
func (r *LogRange) SetMin(min float64) {
	r.Min = min
}

// GetMax returns the max value for the continuous range.
func (r LogRange) GetMax() float64 {
	return r.Max
}

// SetMax sets the max value for the continuous range.
func (r *LogRange) SetMax(max float64) {
	r.Max = max
}

// GetDelta returns the difference between the min and max value.
func (r LogRange) GetDelta() float64 {
	return r.Max - r.Min
}

// GetDomain returns the range domain.
func (r LogRange) GetDomain() int {
	return r.Domain
}

// SetDomain sets the range domain.
func (r *LogRange) SetDomain(domain int) {
	r.Domain = domain
}

// String returns a simple string for the LogRange.
func (r LogRange) String() string {
	return fmt.Sprintf("LogRange [%.2f,%.2f] => %d", r.Min, r.Max, r.Domain)
}

// Translate maps a given value into the LogRange space.
func (r LogRange) Translate(value float64) int {
	normalized := math.Log(1.0+value) - math.Log(1.0+r.Min)
	ratio := normalized / (math.Log(1.0+r.Max) - math.Log(1.0+r.Min))

	if r.IsDescending() {
		return r.Domain - int(math.Ceil(ratio*float64(r.Domain)))
	}

	return int(math.Ceil(ratio * float64(r.Domain)))
}
