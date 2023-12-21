package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Counter interface {
	prometheus.Counter
	ValueGetter
	AddInt(v int)
	AddUint64(v uint64)
}

type counter struct {
	prometheus.Counter
}

// GetValue returns native float64 value stored by this counter
func (c *counter) GetValue() float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		panic(fmt.Errorf("calling GetValue with invalid metric: %w", err))
	}

	return m.GetCounter().GetValue()
}

// GetValueUint64 returns native float64 value stored by this counter cast to
// an uint64 value for convenience
func (c *counter) GetValueUint64() uint64 {
	return uint64(c.GetValue())
}

// AddInt adds an int value to the native float64 value stored by this counter.
//
// This is a convenience function for better UX which is safe for int values up
// to 2^53 (mantissa bits).
//
// This is fine for all usages in our codebase, and it is
// unlikely we will hit issues with this.
//
// If, however there is a new requirement that requires accuracy for more than
// 2^53 we can implement our own simple intCounter that satisfies the Counter
// interface.
func (c *counter) AddInt(v int) {
	c.Add(float64(v))
}

// AddUint64 adds an uint64 value to the native float64 value stored by this counter.
//
// This is a convenience function for better UX which is safe for int values up
// to 2^53 (mantissa bits).
//
// This is fine for all usages in our codebase, and it is
// unlikely we will hit issues with this.
//
// If, however there is a new requirement that requires accuracy for more than
// 2^53 we can implement our own simple uintCounter that satisfies the Counter
// interface.
func (c *counter) AddUint64(v uint64) {
	c.Add(float64(v))
}
