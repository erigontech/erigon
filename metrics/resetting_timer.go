package metrics

import (
	"time"
)

// ResettingTimer is used for storing aggregated values for timers, which are reset on every flush interval.
type ResettingTimer interface {
	Values() []int64
	Snapshot() ResettingTimer
	Percentiles([]float64) []int64
	Mean() float64
	Time(func())
	Update(time.Duration)
	UpdateSince(time.Time)
}
