package metrics

import (
	"testing"
	"time"
)

func TestMeterDecay(t *testing.T) {
	ma := meterArbiter{
		ticker: time.NewTicker(time.Millisecond),
		meters: make(map[*StandardMeter]struct{}),
	}
	defer ma.ticker.Stop()
	m := newStandardMeter()
	ma.meters[m] = struct{}{}
	m.Mark(1)
	ma.tickMeters()
	rateMean := m.RateMean()
	time.Sleep(100 * time.Millisecond)
	ma.tickMeters()
	if m.RateMean() >= rateMean {
		t.Error("m.RateMean() didn't decrease")
	}
}
