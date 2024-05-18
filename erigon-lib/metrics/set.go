package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type namedMetric struct {
	name   string
	metric prometheus.Metric
	isAux  bool
}

// Set is a set of metrics.
//
// Metrics belonging to a set are exported separately from global metrics.
//
// Set.WritePrometheus must be called for exporting metrics from the set.
type Set struct {
	mu sync.Mutex
	a  []*namedMetric
	m  map[string]*namedMetric
}

var defaultSet = NewSet()

// NewSet creates new set of metrics.
//
// Pass the set to RegisterSet() function in order to export its metrics via global WritePrometheus() call.
func NewSet() *Set {
	return &Set{
		m: make(map[string]*namedMetric),
	}
}

func (s *Set) Describe(ch chan<- *prometheus.Desc) {
	lessFunc := func(i, j int) bool {
		return s.a[i].name < s.a[j].name
	}
	s.mu.Lock()
	if !sort.SliceIsSorted(s.a, lessFunc) {
		sort.Slice(s.a, lessFunc)
	}
	sa := append([]*namedMetric(nil), s.a...)
	s.mu.Unlock()
	for _, nm := range sa {
		ch <- nm.metric.Desc()
	}
}

func (s *Set) Collect(ch chan<- prometheus.Metric) {
	lessFunc := func(i, j int) bool {
		return s.a[i].name < s.a[j].name
	}
	s.mu.Lock()
	if !sort.SliceIsSorted(s.a, lessFunc) {
		sort.Slice(s.a, lessFunc)
	}
	sa := append([]*namedMetric(nil), s.a...)
	s.mu.Unlock()
	for _, nm := range sa {
		ch <- nm.metric
	}
}

// NewHistogram creates and returns new histogram in s with the given name.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned histogram is safe to use from concurrent goroutines.
func (s *Set) NewHistogram(name string, buckets []float64, help ...string) (prometheus.Histogram, error) {
	h, err := newHistogram(name, buckets, help...)
	if err != nil {
		return nil, err
	}

	s.registerMetric(name, h)
	return h, nil
}

func newHistogram(name string, buckets []float64, help ...string) (prometheus.Histogram, error) {
	name, labels, err := parseMetric(name)
	if err != nil {
		return nil, err
	}

	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        name,
		ConstLabels: labels,
		Buckets:     buckets,
		Help:        strings.Join(help, " "),
	}), nil
}

// GetOrCreateHistogram returns registered histogram in s with the given name
// or creates new histogram if s doesn't contain histogram with the given name.
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
func (s *Set) GetOrCreateHistogram(name string, help ...string) (prometheus.Histogram, error) {
	s.mu.Lock()
	nm := s.m[name]
	s.mu.Unlock()
	if nm == nil {
		metric, err := newHistogram(name, nil, help...)
		if err != nil {
			return nil, fmt.Errorf("invalid metric name %q: %w", name, err)
		}

		nmNew := &namedMetric{
			name:   name,
			metric: metric,
		}

		s.mu.Lock()
		nm = s.m[name]
		if nm == nil {
			nm = nmNew
			s.m[name] = nm
			s.a = append(s.a, nm)
		}
		s.mu.Unlock()
	}

	h, ok := nm.metric.(prometheus.Histogram)
	if !ok {
		return nil, fmt.Errorf("metric %q isn't a Histogram. It is %T", name, nm.metric)
	}

	return h, nil
}

// NewCounter registers and returns new counter with the given name in the s.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned counter is safe to use from concurrent goroutines.
func (s *Set) NewCounter(name string, help ...string) (prometheus.Counter, error) {
	c, err := newCounter(name, help...)
	if err != nil {
		return nil, err
	}

	s.registerMetric(name, c)
	return c, nil
}

func newCounter(name string, help ...string) (prometheus.Counter, error) {
	name, labels, err := parseMetric(name)
	if err != nil {
		return nil, err
	}

	return prometheus.NewCounter(prometheus.CounterOpts{
		Name:        name,
		Help:        strings.Join(help, " "),
		ConstLabels: labels,
	}), nil
}

// GetOrCreateCounter returns registered counter in s with the given name
// or creates new counter if s doesn't contain counter with the given name.
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
func (s *Set) GetOrCreateCounter(name string, help ...string) (prometheus.Counter, error) {
	s.mu.Lock()
	nm := s.m[name]
	s.mu.Unlock()
	if nm == nil {
		// Slow path - create and register missing counter.
		metric, err := newCounter(name, help...)
		if err != nil {
			return nil, fmt.Errorf("invalid metric name %q: %w", name, err)
		}

		nmNew := &namedMetric{
			name:   name,
			metric: metric,
		}
		s.mu.Lock()
		nm = s.m[name]
		if nm == nil {
			nm = nmNew
			s.m[name] = nm
			s.a = append(s.a, nm)
		}
		s.mu.Unlock()
	}

	c, ok := nm.metric.(prometheus.Counter)
	if !ok {
		return nil, fmt.Errorf("metric %q isn't a Counter. It is %T", name, nm.metric)
	}

	return c, nil
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
// f must be safe for concurrent calls.
//
// The returned gauge is safe to use from concurrent goroutines.
func (s *Set) NewGauge(name string, help ...string) (prometheus.Gauge, error) {
	g, err := newGauge(name, help...)
	if err != nil {
		return nil, err
	}

	s.registerMetric(name, g)
	return g, nil
}

func newGauge(name string, help ...string) (prometheus.Gauge, error) {
	name, labels, err := parseMetric(name)
	if err != nil {
		return nil, err
	}

	return prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        name,
		Help:        strings.Join(help, " "),
		ConstLabels: labels,
	}), nil
}

// GetOrCreateGauge returns registered gauge with the given name in s
// or creates new gauge if s doesn't contain gauge with the given name.
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
func (s *Set) GetOrCreateGauge(name string, help ...string) (prometheus.Gauge, error) {
	s.mu.Lock()
	nm := s.m[name]
	s.mu.Unlock()
	if nm == nil {
		// Slow path - create and register missing gauge.
		metric, err := newGauge(name, help...)
		if err != nil {
			return nil, fmt.Errorf("invalid metric name %q: %w", name, err)
		}

		nmNew := &namedMetric{
			name:   name,
			metric: metric,
		}
		s.mu.Lock()
		nm = s.m[name]
		if nm == nil {
			nm = nmNew
			s.m[name] = nm
			s.a = append(s.a, nm)
		}
		s.mu.Unlock()
	}

	g, ok := nm.metric.(prometheus.Gauge)
	if !ok {
		return nil, fmt.Errorf("metric %q isn't a Gauge. It is %T", name, nm.metric)
	}

	return g, nil
}

const defaultSummaryWindow = 5 * time.Minute

var defaultSummaryQuantiles = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.97: 0.003, 0.99: 0.001}

// NewSummary creates and returns new summary with the given name in s.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned summary is safe to use from concurrent goroutines.
func (s *Set) NewSummary(name string, help ...string) (prometheus.Summary, error) {
	return s.NewSummaryExt(name, defaultSummaryWindow, defaultSummaryQuantiles, help...)
}

func newSummary(name string, window time.Duration, quantiles map[float64]float64, help ...string) (prometheus.Summary, error) {
	name, labels, err := parseMetric(name)
	if err != nil {
		return nil, err
	}

	return prometheus.NewSummary(prometheus.SummaryOpts{
		Name:        name,
		ConstLabels: labels,
		Objectives:  quantiles,
		MaxAge:      window,
		Help:        strings.Join(help, " "),
	}), nil
}

// GetOrCreateSummary returns registered summary with the given name in s
// or creates new summary if s doesn't contain summary with the given name.
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
func (s *Set) GetOrCreateSummary(name string, help ...string) (prometheus.Summary, error) {
	return s.GetOrCreateSummaryExt(name, defaultSummaryWindow, defaultSummaryQuantiles, help...)
}

// NewSummaryExt creates and returns new summary in s with the given name,
// window and quantiles.
//
// name must be valid Prometheus-compatible metric with possible labels.
// For instance,
//
//   - foo
//   - foo{bar="baz"}
//   - foo{bar="baz",aaa="b"}
//
// The returned summary is safe to use from concurrent goroutines.
func (s *Set) NewSummaryExt(name string, window time.Duration, quantiles map[float64]float64, help ...string) (prometheus.Summary, error) {
	metric, err := newSummary(name, window, quantiles, help...)
	if err != nil {
		return nil, fmt.Errorf("invalid metric name %q: %w", name, err)
	}

	s.registerMetric(name, metric)
	return metric, nil
}

// GetOrCreateSummaryExt returns registered summary with the given name,
// window and quantiles in s or creates new summary if s doesn't
// contain summary with the given name.
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
// Performance tip: prefer NewSummaryExt instead of GetOrCreateSummaryExt.
func (s *Set) GetOrCreateSummaryExt(name string, window time.Duration, quantiles map[float64]float64, help ...string) (prometheus.Summary, error) {
	s.mu.Lock()
	nm := s.m[name]
	s.mu.Unlock()
	if nm == nil {
		// Slow path - create and register missing summary.
		metric, err := newSummary(name, window, quantiles, help...)
		if err != nil {
			return nil, fmt.Errorf("invalid metric name %q: %w", name, err)
		}

		nmNew := &namedMetric{
			name:   name,
			metric: metric,
		}
		s.mu.Lock()
		nm = s.m[name]
		if nm == nil {
			nm = nmNew
			s.m[name] = nm
			s.a = append(s.a, nm)
		}
		s.mu.Unlock()
	}

	sm, ok := nm.metric.(prometheus.Summary)
	if !ok {
		return nil, fmt.Errorf("metric %q isn't a Summary. It is %T", name, nm.metric)
	}

	return sm, nil
}

func (s *Set) registerMetric(name string, m prometheus.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mustRegisterLocked(name, m)
}

// mustRegisterLocked registers given metric with the given name.
//
// Panics if the given name was already registered before.
func (s *Set) mustRegisterLocked(name string, m prometheus.Metric) {
	_, ok := s.m[name]
	if !ok {
		nm := &namedMetric{
			name:   name,
			metric: m,
		}
		s.m[name] = nm
		s.a = append(s.a, nm)
	}
	if ok {
		panic(fmt.Errorf("metric %q is already registered", name))
	}
}

// UnregisterMetric removes metric with the given name from s.
//
// True is returned if the metric has been removed.
// False is returned if the given metric is missing in s.
func (s *Set) UnregisterMetric(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	nm, ok := s.m[name]
	if !ok {
		return false
	}
	return s.unregisterMetricLocked(nm)
}

func (s *Set) unregisterMetricLocked(nm *namedMetric) bool {
	name := nm.name
	delete(s.m, name)

	deleteFromList := func(metricName string) {
		for i, nm := range s.a {
			if nm.name == metricName {
				s.a = append(s.a[:i], s.a[i+1:]...)
				return
			}
		}
		panic(fmt.Errorf("cannot find metric %q in the list of registered metrics", name))
	}

	// remove metric from s.a
	deleteFromList(name)

	return true
}

// UnregisterAllMetrics de-registers all metrics registered in s.
func (s *Set) UnregisterAllMetrics() {
	metricNames := s.ListMetricNames()
	for _, name := range metricNames {
		s.UnregisterMetric(name)
	}
}

// ListMetricNames returns sorted list of all the metrics in s.
func (s *Set) ListMetricNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	metricNames := make([]string, 0, len(s.m))
	for _, nm := range s.m {
		if nm.isAux {
			continue
		}
		metricNames = append(metricNames, nm.name)
	}
	sort.Strings(metricNames)
	return metricNames
}
