package metrics

import (
	metrics2 "github.com/VictoriaMetrics/metrics"
)

func GetOrCreateCounter(s string, isGauge ...bool) *metrics2.Counter {
	counter := metrics2.GetOrCreateCounter(s, isGauge...)
	DefaultRegistry.Register(s, counter)
	metrics2.GetDefaultSet().UnregisterMetric(s)
	return counter
}

func GetOrCreateGauge(s string, f func() float64) *metrics2.Gauge {
	gauge := metrics2.GetOrCreateGauge(s, f)
	DefaultRegistry.Register(s, gauge)
	metrics2.GetDefaultSet().UnregisterMetric(s)
	return gauge
}

func GetOrCreateFloatCounter(s string) *metrics2.FloatCounter {
	floatCounter := metrics2.GetOrCreateFloatCounter(s)
	DefaultRegistry.Register(s, floatCounter)
	metrics2.GetDefaultSet().UnregisterMetric(s)
	return floatCounter
}

func GetOrCreateSummary(s string) *metrics2.Summary {
	summary := metrics2.GetOrCreateSummary(s)
	DefaultRegistry.Register(s, summary)
	metrics2.GetDefaultSet().UnregisterMetric(s)
	return summary
}

func GetOrCreateHistogram(s string) *metrics2.Histogram {
	histogram := metrics2.GetOrCreateHistogram(s)
	DefaultRegistry.Register(s, histogram)
	metrics2.GetDefaultSet().UnregisterMetric(s)
	return histogram
}
