package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func GetCounterValue(metric prometheus.Counter) float64 {
	var m dto.Metric
	if err := metric.Write(&m); err != nil {
		panic(fmt.Errorf("calling GetCounterValue with invalid metric: %w", err))
	}

	return m.GetCounter().GetValue()
}

func GetCounterValueUint64(metric prometheus.Counter) uint64 {
	return uint64(GetCounterValue(metric))
}

func GetGaugeValue(metric prometheus.Gauge) float64 {
	var m dto.Metric
	if err := metric.Write(&m); err != nil {
		panic(fmt.Errorf("calling GetCounterValue with invalid metric: %w", err))
	}

	return m.GetGauge().GetValue()
}

func GetGaugeValueUint64(metric prometheus.Gauge) uint64 {
	return uint64(GetGaugeValue(metric))
}
