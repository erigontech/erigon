package metrics

import (
	"fmt"
	"time"

	vm "github.com/VictoriaMetrics/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const UsePrometheusClient = true

type Histogram interface {
	// UpdateDuration updates request duration based on the given startTime.
	UpdateDuration(time.Time)

	// Update updates h with v.
	//
	// Negative values and NaNs are ignored.
	Update(float64)
}

type Summary interface {
	Histogram
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
	if err := c.Gauge.Write(&m); err != nil {
		panic(fmt.Errorf("calling intCounter.Get on invalid metric: %w", err))
	}
	return uint64(m.GetGauge().GetValue())
}

// NewCounter registers and returns new counter with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned counter is safe to use from concurrent goroutines.
func NewCounter(s string) Counter {
	if UsePrometheusClient {
		counter, err := defaultSet.NewGauge(s)
		if err != nil {
			panic(fmt.Errorf("could not create new counter: %w", err))
		}

		return intCounter{counter}
	} else {
		return vm.GetDefaultSet().NewCounter(s)
	}
}

// GetOrCreateCounter returns registered counter with the given name
// or creates new counter if the registry doesn't contain counter with
// the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned counter is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewCounter instead of GetOrCreateCounter.
func GetOrCreateCounter(s string, isGauge ...bool) Counter {
	if UsePrometheusClient {
		counter, err := defaultSet.GetOrCreateGauge(s)
		if err != nil {
			panic(fmt.Errorf("could not get or create new counter: %w", err))
		}

		return intCounter{counter}
	} else {
		if counter := DefaultRegistry.Get(s); counter != nil {
			if counter, ok := counter.(Counter); ok {
				return counter
			}
		}

		counter := vm.GetOrCreateCounter(s, isGauge...)
		DefaultRegistry.Register(s, counter)
		vm.GetDefaultSet().UnregisterMetric(s)
		return counter
	}
}

// NewGaugeFunc registers and returns gauge with the given name, which calls f
// to obtain gauge value.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// f must be safe for concurrent calls.
//
// The returned gauge is safe to use from concurrent goroutines.
func NewGaugeFunc(s string, f func() float64) prometheus.GaugeFunc {
	gf, err := defaultSet.NewGaugeFunc(s, f)
	if err != nil {
		panic(fmt.Errorf("could not create new gauge func: %w", err))
	}

	return gf
}

// GetOrCreateGaugeFunc returns registered gauge with the given name
// or creates new gauge if the registry doesn't contain gauge with
// the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned gauge is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewGauge instead of GetOrCreateGauge.
func GetOrCreateGaugeFunc(s string, f func() float64) prometheus.GaugeFunc {
	gf, err := defaultSet.GetOrCreateGaugeFunc(s, f)
	if err != nil {
		panic(fmt.Errorf("could not get or create new gauge func: %w", err))
	}

	return gf
}

type summary struct {
	prometheus.Summary
}

func (sm summary) UpdateDuration(startTime time.Time) {
	sm.Observe(time.Since(startTime).Seconds())
}

func (sm summary) Update(v float64) {
	sm.Observe(v)
}

// NewSummary creates and returns new summary with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned summary is safe to use from concurrent goroutines.
func NewSummary(s string) Summary {
	if UsePrometheusClient {
		s, err := defaultSet.NewSummary(s)
		if err != nil {
			panic(fmt.Errorf("could not create new summary: %w", err))
		}

		return summary{s}
	} else {
		summary := vm.NewSummary(s)
		DefaultRegistry.Register(s, summary)
		vm.GetDefaultSet().UnregisterMetric(s)
		return summary
	}
}

// GetOrCreateSummary returns registered summary with the given name
// or creates new summary if the registry doesn't contain summary with
// the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned summary is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewSummary instead of GetOrCreateSummary.
func GetOrCreateSummary(s string) Summary {
	if UsePrometheusClient {
		s, err := defaultSet.GetOrCreateSummary(s)
		if err != nil {
			panic(fmt.Errorf("could not get or create new summary: %w", err))
		}

		return summary{s}
	} else {
		summary := vm.GetOrCreateSummary(s)
		DefaultRegistry.Register(s, summary)
		vm.GetDefaultSet().UnregisterMetric(s)
		return summary
	}
}

type histogram struct {
	prometheus.Histogram
}

func (h histogram) UpdateDuration(startTime time.Time) {
	h.Observe(time.Since(startTime).Seconds())
}

func (h histogram) Update(v float64) {
	h.Observe(v)
}

// NewHistogram creates and returns new histogram with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned histogram is safe to use from concurrent goroutines.
func NewHistogram(s string) Histogram {
	if UsePrometheusClient {
		h, err := defaultSet.NewHistogram(s)
		if err != nil {
			panic(fmt.Errorf("could not create new histogram: %w", err))
		}

		return histogram{h}
	} else {
		return vm.NewHistogram(s)
	}
}

// GetOrCreateHistogram returns registered histogram with the given name
// or creates new histogram if the registry doesn't contain histogram with
// the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned histogram is safe to use from concurrent goroutines.
//
// Performance tip: prefer NewHistogram instead of GetOrCreateHistogram.
func GetOrCreateHistogram(s string) Histogram {
	if UsePrometheusClient {
		h, err := defaultSet.GetOrCreateHistogram(s)
		if err != nil {
			panic(fmt.Errorf("could not get or create new histogram: %w", err))
		}

		return histogram{h}
	} else {
		summary := vm.GetOrCreateHistogram(s)
		DefaultRegistry.Register(s, summary)
		vm.GetDefaultSet().UnregisterMetric(s)
		return summary
	}
}
