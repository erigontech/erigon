package metrics

import (
	"time"
)

type DurationObserver interface {
	ObserveDuration(since time.Time)
}
