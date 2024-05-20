package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Histogram interface {
	prometheus.Histogram
	DurationObserver
}

type histogram struct {
	prometheus.Summary
}

func (h *histogram) ObserveDuration(start time.Time) {
	h.Observe(secondsSince(start))
}
