package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Counter interface {
	prometheus.Counter
	ValueGetter
}

type counter struct {
	prometheus.Counter
}

// GetValue returns native float64 value stored by this counter
func (c counter) GetValue() float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		panic(fmt.Errorf("calling GetValue with invalid metric: %w", err))
	}

	return m.GetCounter().GetValue()
}

// GetValueUint64 returns native float64 value stored by this counter cast to
// an uint64 value for convenience
func (c counter) GetValueUint64() uint64 {
	return uint64(c.GetValue())
}
