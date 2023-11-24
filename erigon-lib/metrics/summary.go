package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Summary interface {
	prometheus.Summary
	DurationObserver
}

type summary struct {
	prometheus.Summary
}

func (s *summary) ObserveDuration(start time.Time) {
	s.Observe(secondsSince(start))
}
