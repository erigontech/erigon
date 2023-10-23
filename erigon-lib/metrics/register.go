package metrics

import (
	"time"

	vm "github.com/VictoriaMetrics/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const UsePrometheusClient = false

type Summary interface {
	UpdateDuration(time.Time)
}

type Counter interface {
	Inc()
	Dec()
	Add(n int)
	Set(n uint64)
	Get() uint64
}

type intCounter struct {
	prometheus.Gauge
}

func (c intCounter) Add(n int) {
	c.Gauge.Add(float64(n))
}

func (c intCounter) Set(n uint64) {
	c.Gauge.Set(float64(n))
}

func (c intCounter) Get() uint64 {
	var m dto.Metric
	c.Gauge.Write(&m)
	return uint64(m.GetGauge().GetValue())
}

func GetOrCreateCounter(s string, isGauge ...bool) Counter {
	if UsePrometheusClient {
		counter := defaultSet.GetOrCreateGauge(s)
		return intCounter{counter}
	} else {
		counter := vm.GetOrCreateCounter(s, isGauge...)
		DefaultRegistry.Register(s, counter)
		vm.GetDefaultSet().UnregisterMetric(s)
		return counter
	}
}

func GetOrCreateGaugeFunc(s string, f func() float64) prometheus.GaugeFunc {
	return defaultSet.GetOrCreateGaugeFunc(s, f)
}

type summary struct {
	prometheus.Summary
}

func (sm summary) UpdateDuration(startTime time.Time) {
	sm.Observe(time.Since(startTime).Seconds())
}

func GetOrCreateSummary(s string) Summary {
	if UsePrometheusClient {
		s := defaultSet.GetOrCreateSummary(s)
		return summary{s}
	} else {
		summary := vm.GetOrCreateSummary(s)
		DefaultRegistry.Register(s, summary)
		vm.GetDefaultSet().UnregisterMetric(s)
		return summary
	}
}

func GetOrCreateHistogram(s string) prometheus.Histogram {
	return defaultSet.GetOrCreateHistogram(s)
}
