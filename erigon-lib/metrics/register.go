package metrics

import (
	"fmt"
)

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
func NewCounter(name string) Counter {
	c, err := defaultSet.NewCounter(name)
	if err != nil {
		panic(fmt.Errorf("could not create new counter: %w", err))
	}

	return &counter{c}
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
func GetOrCreateCounter(name string) Counter {
	c, err := defaultSet.GetOrCreateCounter(name)
	if err != nil {
		panic(fmt.Errorf("could not get or create new counter: %w", err))
	}

	return &counter{c}
}

// NewGauge registers and returns gauge with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned gauge is safe to use from concurrent goroutines.
func NewGauge(name string) Gauge {
	g, err := defaultSet.NewGauge(name)
	if err != nil {
		panic(fmt.Errorf("could not create new gauge: %w", err))
	}

	return &gauge{g}
}

// GetOrCreateGauge returns registered gauge with the given name
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
func GetOrCreateGauge(name string) Gauge {
	g, err := defaultSet.GetOrCreateGauge(name)
	if err != nil {
		panic(fmt.Errorf("could not get or create new gauge: %w", err))
	}

	return &gauge{g}
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
func NewSummary(name string) Summary {
	s, err := defaultSet.NewSummary(name)
	if err != nil {
		panic(fmt.Errorf("could not create new summary: %w", err))
	}

	return &summary{s}
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
func GetOrCreateSummary(name string) Summary {
	s, err := defaultSet.GetOrCreateSummary(name)
	if err != nil {
		panic(fmt.Errorf("could not get or create new summary: %w", err))
	}

	return &summary{s}
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
func NewHistogram(name string) Histogram {
	h, err := defaultSet.NewHistogram(name)
	if err != nil {
		panic(fmt.Errorf("could not create new histogram: %w", err))
	}

	return &histogram{h}
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
func GetOrCreateHistogram(name string) Histogram {
	h, err := defaultSet.GetOrCreateHistogram(name)
	if err != nil {
		panic(fmt.Errorf("could not get or create new histogram: %w", err))
	}

	return &histogram{h}
}
