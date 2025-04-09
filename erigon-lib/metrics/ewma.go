package metrics

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ewma is the standard implementation of an EWMA and tracks the number
// of uncounted events and processes them on each tick. 
type Ewma struct {
	uncounted int64 // /!\ this should be the first member to ensure 64-bit alignment
	alpha     float64
	rate      float64
	init      bool
	mutex     sync.Mutex
	lastTick  time.Time
}

func NewEwma() *Ewma {
	return &Ewma{alpha: 1 - math.Exp(-5.0/60.0/1), lastTick: time.Now()}
}

// Rate returns the moving average rate of events per second.
func (a *Ewma) Rate() float64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.rate * float64(time.Second)
}

// Tick ticks the clock to update the moving average.  It assumes it is called
// every five seconds.
func (a *Ewma) Tick() {
	count := atomic.LoadInt64(&a.uncounted)
	atomic.AddInt64(&a.uncounted, -count)
	lastTick := a.lastTick
	a.lastTick = time.Now()
	instantRate := float64(count) / time.Since(lastTick).Seconds()
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if a.init {
		a.rate += a.alpha * (instantRate - a.rate)
	} else {
		a.init = true
		a.rate = instantRate
	}
}

// Update adds n uncounted events.
func (a *Ewma) Update(n int64) {
	atomic.AddInt64(&a.uncounted, n)
}
