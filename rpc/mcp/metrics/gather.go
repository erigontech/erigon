package metrics

import (
	"fmt"
	"path"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// ListMetricNames returns a sorted list of all available metric names.
func ListMetricNames() ([]string, error) {
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil, fmt.Errorf("failed to gather metrics: %w", err)
	}

	names := make([]string, 0, len(metricFamilies))
	for _, mf := range metricFamilies {
		if mf != nil && mf.Name != nil {
			names = append(names, *mf.Name)
		}
	}
	return names, nil
}

// GatherMetricsFiltered gathers metrics from the Prometheus default registry
// with optional filtering by metric name pattern (supports wildcards like "db_*" or "*_size").
// If pattern is empty, returns all metrics.
func GatherMetricsFiltered(pattern string) (map[string]any, error) {
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil, fmt.Errorf("failed to gather metrics: %w", err)
	}

	result := make(map[string]any)

	for _, mf := range metricFamilies {
		if mf == nil || mf.Name == nil {
			continue
		}

		metricName := *mf.Name

		// Apply filter if pattern is provided
		if pattern != "" {
			ok, err := matchPattern(pattern, metricName)
			if err != nil {
				return nil, fmt.Errorf("invalid pattern %q: %w", pattern, err)
			}
			if !ok {
				continue
			}
		}

		metricType := mf.Type.String()
		metrics := make([]map[string]any, 0, len(mf.Metric))

		for _, m := range mf.Metric {
			metricData := make(map[string]any)

			// Add labels
			if len(m.Label) > 0 {
				labels := make(map[string]string)
				for _, label := range m.Label {
					if label.Name != nil && label.Value != nil {
						labels[*label.Name] = *label.Value
					}
				}
				metricData["labels"] = labels
			}

			// Extract value based on metric type
			switch *mf.Type {
			case dto.MetricType_COUNTER:
				if m.Counter != nil && m.Counter.Value != nil {
					metricData["value"] = *m.Counter.Value
				}
			case dto.MetricType_GAUGE:
				if m.Gauge != nil && m.Gauge.Value != nil {
					metricData["value"] = *m.Gauge.Value
				}
			case dto.MetricType_HISTOGRAM:
				if m.Histogram != nil {
					histData := make(map[string]any)
					if m.Histogram.SampleCount != nil {
						histData["sample_count"] = *m.Histogram.SampleCount
					}
					if m.Histogram.SampleSum != nil {
						histData["sample_sum"] = *m.Histogram.SampleSum
					}
					if len(m.Histogram.Bucket) > 0 {
						buckets := make([]map[string]any, 0, len(m.Histogram.Bucket))
						for _, bucket := range m.Histogram.Bucket {
							b := make(map[string]any)
							if bucket.UpperBound != nil {
								b["upper_bound"] = *bucket.UpperBound
							}
							if bucket.CumulativeCount != nil {
								b["cumulative_count"] = *bucket.CumulativeCount
							}
							buckets = append(buckets, b)
						}
						histData["buckets"] = buckets
					}
					metricData["histogram"] = histData
				}
			case dto.MetricType_SUMMARY:
				if m.Summary != nil {
					summaryData := make(map[string]any)
					if m.Summary.SampleCount != nil {
						summaryData["sample_count"] = *m.Summary.SampleCount
					}
					if m.Summary.SampleSum != nil {
						summaryData["sample_sum"] = *m.Summary.SampleSum
					}
					if len(m.Summary.Quantile) > 0 {
						quantiles := make([]map[string]any, 0, len(m.Summary.Quantile))
						for _, quantile := range m.Summary.Quantile {
							q := make(map[string]any)
							if quantile.Quantile != nil {
								q["quantile"] = *quantile.Quantile
							}
							if quantile.Value != nil {
								q["value"] = *quantile.Value
							}
							quantiles = append(quantiles, q)
						}
						summaryData["quantiles"] = quantiles
					}
					metricData["summary"] = summaryData
				}
			case dto.MetricType_UNTYPED:
				if m.Untyped != nil && m.Untyped.Value != nil {
					metricData["value"] = *m.Untyped.Value
				}
			}

			// Add timestamp if present
			if m.TimestampMs != nil {
				metricData["timestamp_ms"] = *m.TimestampMs
			}

			metrics = append(metrics, metricData)
		}

		result[metricName] = map[string]any{
			"type":    metricType,
			"help":    mf.GetHelp(),
			"metrics": metrics,
		}
	}

	return result, nil
}

// matchPattern reports whether a metric name matches a glob pattern. Prometheus
// names carry no "/", so path.Match's separator handling never applies.
func matchPattern(pattern, name string) (bool, error) {
	return path.Match(pattern, name)
}
