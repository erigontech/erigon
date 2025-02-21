// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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

// GetOrCreateGaugeVec returns registered GaugeVec with the given name
// or creates a new GaugeVec if the registry doesn't contain a GaugeVec with
// the given name and labels.
//
// name must be a valid Prometheus-compatible metric with possible labels.
// labels are the names of the dimensions associated with the gauge vector.
// For instance,
//
//   - foo, with labels []string{"bar", "baz"}
//
// The returned GaugeVec is safe to use from concurrent goroutines.
func GetOrCreateGaugeVec(name string, labels []string, help ...string) *GaugeVec {
	gv, err := defaultSet.GetOrCreateGaugeVec(name, labels, help...)
	if err != nil {
		panic(fmt.Errorf("could not get or create new gaugevec: %w", err))
	}

	return &GaugeVec{gv}
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

// add labels to metric name
func buildLabeledName(baseName string, labelNames, labelValues []string) string {
	if len(labelNames) == 0 {
		return baseName
	}
	baseName += "{"
	baseName += fmt.Sprintf(`%s="%s"`, labelNames[0], labelValues[0])

	for i := 1; i < len(labelNames); i++ {
		baseName += fmt.Sprintf(`,%s="%s"`, labelNames[i], labelValues[i])
	}
	baseName += "}"
	return baseName
}

func GetOrCreateSummaryWithLabels(name string, labelNames, labelValues []string) Summary {
	labeledName := buildLabeledName(name, labelNames, labelValues)
	return GetOrCreateSummary(labeledName)
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
func NewHistogram(name string, buckets []float64) Histogram {
	h, err := defaultSet.NewHistogram(name, buckets)
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
