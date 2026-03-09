package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		// Exact match
		{"db_size", "db_size", true},
		{"db_size", "db_count", false},

		// Wildcard * tests
		{"db_*", "db_size", true},
		{"db_*", "db_count", true},
		{"db_*", "cache_size", false},
		{"*_size", "db_size", true},
		{"*_size", "cache_size", true},
		{"*_size", "db_count", false},
		{"db_*_size", "db_block_size", true},
		{"db_*_size", "db_cache_size", true},
		{"db_*_size", "cache_size", false},
		{"db*size", "db_size", true}, // Match without underscore requirement

		// Wildcard ? tests
		{"db_?ize", "db_size", true},
		{"db_?ize", "db_rize", true},
		{"db_?ize", "db__ize", true},
		{"db_?ize", "db_siize", false},

		// Combined wildcards
		{"db_*_?ize", "db_block_size", true},
		{"db_*_?ize", "db_cache_rize", true},
		{"*_db_*", "app_db_size", true},
		{"*_db_*", "app_db_cache_size", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			got := matchPattern(tt.pattern, tt.name)
			if got != tt.want {
				t.Errorf("matchPattern(%q, %q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
			}
		})
	}
}

func TestListMetricNames(t *testing.T) {
	// Register a test metric
	testCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_mcp_counter",
		Help: "Test counter for MCP",
	})
	prometheus.MustRegister(testCounter)
	defer prometheus.Unregister(testCounter)

	testCounter.Inc()

	names, err := ListMetricNames()
	if err != nil {
		t.Fatalf("ListMetricNames() error = %v", err)
	}

	if len(names) == 0 {
		t.Error("ListMetricNames() returned empty list")
	}

	// Check if our test metric is in the list
	found := false
	for _, name := range names {
		if name == "test_mcp_counter" {
			found = true
			break
		}
	}

	if !found {
		t.Error("test_mcp_counter not found in metric names")
	}
}

func TestGatherMetrics(t *testing.T) {
	// Register test metrics
	testCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_mcp_gather_counter",
		Help: "Test counter for gather",
	})
	testGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_mcp_gather_gauge",
		Help: "Test gauge for gather",
	})

	prometheus.MustRegister(testCounter)
	prometheus.MustRegister(testGauge)
	defer prometheus.Unregister(testCounter)
	defer prometheus.Unregister(testGauge)

	testCounter.Inc()
	testGauge.Set(42.5)

	metrics, err := GatherMetrics()
	if err != nil {
		t.Fatalf("GatherMetrics() error = %v", err)
	}

	if len(metrics) == 0 {
		t.Error("GatherMetrics() returned empty map")
	}

	// Check counter
	if counterData, ok := metrics["test_mcp_gather_counter"]; ok {
		counterMap := counterData.(map[string]any)
		if counterMap["type"] != "COUNTER" {
			t.Errorf("Counter type = %v, want COUNTER", counterMap["type"])
		}
	} else {
		t.Error("test_mcp_gather_counter not found in metrics")
	}

	// Check gauge
	if gaugeData, ok := metrics["test_mcp_gather_gauge"]; ok {
		gaugeMap := gaugeData.(map[string]any)
		if gaugeMap["type"] != "GAUGE" {
			t.Errorf("Gauge type = %v, want GAUGE", gaugeMap["type"])
		}
	} else {
		t.Error("test_mcp_gather_gauge not found in metrics")
	}
}

func TestGatherMetricsFiltered(t *testing.T) {
	// Register test metrics with different prefixes
	counter1 := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_filter_db_size",
		Help: "Test db metric",
	})
	counter2 := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_filter_cache_size",
		Help: "Test cache metric",
	})
	counter3 := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_filter_db_count",
		Help: "Test db count metric",
	})

	prometheus.MustRegister(counter1)
	prometheus.MustRegister(counter2)
	prometheus.MustRegister(counter3)
	defer prometheus.Unregister(counter1)
	defer prometheus.Unregister(counter2)
	defer prometheus.Unregister(counter3)

	tests := []struct {
		name        string
		pattern     string
		wantCount   int
		wantMetrics []string
		dontWant    []string
	}{
		{
			name:        "filter by db prefix",
			pattern:     "test_filter_db_*",
			wantCount:   2,
			wantMetrics: []string{"test_filter_db_size", "test_filter_db_count"},
			dontWant:    []string{"test_filter_cache_size"},
		},
		{
			name:        "filter by size suffix",
			pattern:     "test_filter_*_size",
			wantCount:   2,
			wantMetrics: []string{"test_filter_db_size", "test_filter_cache_size"},
			dontWant:    []string{"test_filter_db_count"},
		},
		{
			name:        "exact match",
			pattern:     "test_filter_cache_size",
			wantCount:   1,
			wantMetrics: []string{"test_filter_cache_size"},
			dontWant:    []string{"test_filter_db_size", "test_filter_db_count"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics, err := GatherMetricsFiltered(tt.pattern)
			if err != nil {
				t.Fatalf("GatherMetricsFiltered(%q) error = %v", tt.pattern, err)
			}

			// Check that we got expected metrics
			for _, wantMetric := range tt.wantMetrics {
				if _, ok := metrics[wantMetric]; !ok {
					t.Errorf("Expected metric %q not found", wantMetric)
				}
			}

			// Check that we didn't get unwanted metrics
			for _, dontWantMetric := range tt.dontWant {
				if _, ok := metrics[dontWantMetric]; ok {
					t.Errorf("Unexpected metric %q found", dontWantMetric)
				}
			}
		})
	}
}

func TestGatherMetricsFilteredEmpty(t *testing.T) {
	// Test with a pattern that matches nothing
	metrics, err := GatherMetricsFiltered("nonexistent_metric_pattern_*")
	if err != nil {
		t.Fatalf("GatherMetricsFiltered() error = %v", err)
	}

	if len(metrics) != 0 {
		t.Errorf("Expected empty result for non-matching pattern, got %d metrics", len(metrics))
	}
}
