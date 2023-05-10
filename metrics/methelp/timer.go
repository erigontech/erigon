package methelp

import (
	"fmt"
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
	rawName := strings.Split(name, "{")
	return &HistTimer{
		Histogram: metrics.GetOrCreateCompatibleHistogram(name),
		start:     time.Now(),
		name:      rawName[0],
	}
}

func (h *HistTimer) PutSince() {
	h.Histogram.UpdateDuration(h.start)
}

func (h *HistTimer) Tag(pairs ...string) *HistTimer {
	if len(pairs)%2 != 0 {
		pairs = append(pairs, "UNEQUAL_KEY_VALUE_TAGS")
	}
	toJoin := []string{}
	for i := 0; i < len(pairs); i = i + 2 {
		toJoin = append(toJoin, fmt.Sprintf(`%s="%s"`, pairs[i], pairs[i+1]))
	}
	tags := ""
	if len(toJoin) > 0 {
		tags = "{" + strings.Join(toJoin, ",") + "}"
	}
	return &HistTimer{
		Histogram: metrics.GetOrCreateCompatibleHistogram(h.name + tags),
		start:     time.Now(),
		name:      h.name,
	}
}

func (h *HistTimer) Child(suffix string) *HistTimer {
	suffix = strings.TrimPrefix(suffix, "_")
	return NewHistTimer(h.name + "_" + suffix)
}
