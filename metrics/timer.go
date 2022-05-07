package metrics

import (
	"time"
)

// Timers capture the duration and rate of events.
type Timer interface {
	Count() int64
	Max() int64
	Mean() float64
	Min() int64
	Percentile(float64) float64
	Percentiles([]float64) []float64
	Rate1() float64
	Rate5() float64
	Rate15() float64
	RateMean() float64
	Snapshot() Timer
	StdDev() float64
	Stop()
	Sum() int64
	Time(func())
	Update(time.Duration)
	UpdateSince(time.Time)
	Variance() float64
}

// TimerSnapshot is a read-only copy of another Timer.
type TimerSnapshot struct {
	histogram *HistogramSnapshot
	meter     *MeterSnapshot
}

// Count returns the number of events recorded at the time the snapshot was
// taken.
func (t *TimerSnapshot) Count() int64 { return t.histogram.Count() }

// Max returns the maximum value at the time the snapshot was taken.
func (t *TimerSnapshot) Max() int64 { return t.histogram.Max() }

// Mean returns the mean value at the time the snapshot was taken.
func (t *TimerSnapshot) Mean() float64 { return t.histogram.Mean() }

// Min returns the minimum value at the time the snapshot was taken.
func (t *TimerSnapshot) Min() int64 { return t.histogram.Min() }

// Percentile returns an arbitrary percentile of sampled values at the time the
// snapshot was taken.
func (t *TimerSnapshot) Percentile(p float64) float64 {
	return t.histogram.Percentile(p)
}

// Percentiles returns a slice of arbitrary percentiles of sampled values at
// the time the snapshot was taken.
func (t *TimerSnapshot) Percentiles(ps []float64) []float64 {
	return t.histogram.Percentiles(ps)
}

// Rate1 returns the one-minute moving average rate of events per second at the
// time the snapshot was taken.
func (t *TimerSnapshot) Rate1() float64 { return t.meter.Rate1() }

// Rate5 returns the five-minute moving average rate of events per second at
// the time the snapshot was taken.
func (t *TimerSnapshot) Rate5() float64 { return t.meter.Rate5() }

// Rate15 returns the fifteen-minute moving average rate of events per second
// at the time the snapshot was taken.
func (t *TimerSnapshot) Rate15() float64 { return t.meter.Rate15() }

// RateMean returns the meter's mean rate of events per second at the time the
// snapshot was taken.
func (t *TimerSnapshot) RateMean() float64 { return t.meter.RateMean() }

// Snapshot returns the snapshot.
func (t *TimerSnapshot) Snapshot() Timer { return t }

// StdDev returns the standard deviation of the values at the time the snapshot
// was taken.
func (t *TimerSnapshot) StdDev() float64 { return t.histogram.StdDev() }

// Stop is a no-op.
func (t *TimerSnapshot) Stop() {}

// Sum returns the sum at the time the snapshot was taken.
func (t *TimerSnapshot) Sum() int64 { return t.histogram.Sum() }

// Time panics.
func (*TimerSnapshot) Time(func()) {
	panic("Time called on a TimerSnapshot")
}

// Update panics.
func (*TimerSnapshot) Update(time.Duration) {
	panic("Update called on a TimerSnapshot")
}

// UpdateSince panics.
func (*TimerSnapshot) UpdateSince(time.Time) {
	panic("UpdateSince called on a TimerSnapshot")
}

// Variance returns the variance of the values at the time the snapshot was
// taken.
func (t *TimerSnapshot) Variance() float64 { return t.histogram.Variance() }
