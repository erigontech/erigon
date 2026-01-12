package metrics

import (
	"sync/atomic"

	"golang.org/x/exp/constraints"
)

// EMA holds the Exponential Moving Average of a float64 with a the given
// default α value. Safe to access concurrently.
// https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average.
// It can also optionally have β which controls decrease rate seperately.
type EMA[T constraints.Integer | constraints.Float] struct {
	defaultAlpha float64
	defaultBeta  float64
	defaultValue T
	v            atomic.Pointer[T]
}

// New creates an EMA with the given default value and alpha
func NewEma[T constraints.Integer | constraints.Float](defaultValue T, defaultAlpha float64) *EMA[T] {
	return NewEmaWithBeta(defaultValue, defaultAlpha, defaultAlpha)
}

// NewWithBeta is the same as New but supplies a seperate beta to control the
// decrease rate
func NewEmaWithBeta[T constraints.Integer | constraints.Float](defaultValue T, defaultAlpha float64, defaultBeta float64) *EMA[T] {
	return &EMA[T]{defaultAlpha: defaultAlpha, defaultBeta: defaultBeta, defaultValue: defaultValue}
}

// UpdateAlpha calculates and stores new EMA based on the duration and α
// value passed in.
func (e *EMA[T]) UpdateAlpha(v T, alpha float64) T {
	return e.updateAlphaBeta(v, alpha, alpha)
}

// UpdateAlphaBeta is the same as UpdateAlpha but calculates new EMA based on β
// if new value is smaller than the current EMA.
func (e *EMA[T]) updateAlphaBeta(v T, alpha float64, beta float64) T {
	oldEma := e.v.Load()
	var newEma T
	if oldEma == nil {
		newEma = v
	} else {
		if v >= *oldEma {
			newEma = T((1-alpha)*float64(*oldEma) + alpha*float64(v))
		} else {
			newEma = T((1-beta)*float64(*oldEma) + beta*float64(v))
		}
	}
	e.v.Store(&newEma)
	return newEma
}

// Update is like UpdateAlphaBeta but using the default alpha and beta
func (e *EMA[T]) Update(v T) T {
	return e.updateAlphaBeta(v, e.defaultAlpha, e.defaultBeta)
}

// Set sets the EMA directly.
func (e *EMA[T]) Set(v T) {
	e.v.Store(&v)
}

// Clear clears the EMA
func (e *EMA[T]) Clear() {
	e.v.Store(nil)
}

// Get gets the EMA
func (e *EMA[T]) Get() T {
	ema := e.v.Load()
	if ema == nil {
		return e.defaultValue
	}
	return *ema
}
