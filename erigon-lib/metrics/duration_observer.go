package metrics

import (
	"time"
)

type DurationObserver interface {
	// ObserveDuration observes duration since start time
	ObserveDuration(start time.Time)
}

func secondsSince(start time.Time) float64 {
	return time.Since(start).Seconds()
}
