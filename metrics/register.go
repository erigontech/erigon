package metrics

import (
	metrics2 "github.com/VictoriaMetrics/metrics"
)

func GetOrCreateCounter(s string, isGauge ...bool) *metrics2.Counter {
	counter := metrics2.GetOrCreateCounter(s, isGauge...)
	DefaultRegistry.Register(s, counter)
	return counter
}

func GetOrCreateGauge(s string, f func() float64) *metrics2.Gauge {
	gauge := metrics2.GetOrCreateGauge(s, f)
	DefaultRegistry.Register(s, gauge)
	return gauge
}

func GetOrCreateFloatCounter(s string) *metrics2.FloatCounter {
	floatCounter := metrics2.GetOrCreateFloatCounter(s)
	DefaultRegistry.Register(s, floatCounter)
	return floatCounter
}

func GetOrCreateSummary(s string) *metrics2.Summary {
	summary := metrics2.GetOrCreateSummary(s)
	DefaultRegistry.Register(s, summary)
	return summary
}

func GetOrCreateHistogram(s string) *metrics2.Histogram {
	histogram := metrics2.GetOrCreateHistogram(s)
	DefaultRegistry.Register(s, histogram)
	return histogram
}
