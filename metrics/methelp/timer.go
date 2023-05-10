package methelp

import (
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

type HistTimer struct {
	*metrics.Histogram

	start time.Time

	name string
}

func NewHistTimer(name string) *HistTimer {
	return &HistTimer{
		Histogram: metrics.NewHistogram(name),
		start:     time.Now(),
		name:      name,
	}
}

func (h *HistTimer) PutSince() {
	h.Histogram.UpdateDuration(h.start)
}

func (h *HistTimer) Child(suffix string) *HistTimer {
	suffix = strings.TrimPrefix(suffix, "_")
	return NewHistTimer(h.name + "_" + suffix)
}
