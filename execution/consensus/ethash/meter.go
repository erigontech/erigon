// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package ethash

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common/dbg"
)

func newHashRateMeter() *hashRateMeter {
	m := newMeter()
	arbiter.mu.Lock()
	defer arbiter.mu.Unlock()
	arbiter.meters[m] = struct{}{}
	if !arbiter.started {
		arbiter.started = true
		go arbiter.tick()
	}
	return m
}

// meterSnapshot is a read-only copy of another Meter.
type meterSnapshot struct {
	// WARNING: The `temp` field is accessed atomically.
	// On 32 bit platforms, only 64-bit aligned fields can be atomic. The struct is
	// guaranteed to be so aligned, so take advantage of that. For more information,
	// see https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	temp  int64
	count int64
	rate  float64
}

// Count returns the count of events at the time the snapshot was taken.
func (m *meterSnapshot) Count() int64 { return m.count }

// Mark panics.
func (*meterSnapshot) Mark(n int64) {
	panic("Mark called on a MeterSnapshot")
}

// Rate1 returns the one-minute moving average rate of events per second at the
// time the snapshot was taken.
func (m *meterSnapshot) Rate() float64 { return m.rate }

// Stop is a no-op.
func (m *meterSnapshot) Stop() {}

// StandardMeter is the standard implementation of a Meter.
type hashRateMeter struct {
	lock      sync.RWMutex
	snapshot  *meterSnapshot
	a1        *ewma
	startTime time.Time
	stopped   uint32
}

func newMeter() *hashRateMeter {
	return &hashRateMeter{
		snapshot:  &meterSnapshot{},
		a1:        &ewma{alpha: 1 - math.Exp(-5.0/60.0/1)},
		startTime: time.Now(),
	}
}

// Stop stops the meter, Mark() will be a no-op if you use it after being stopped.
func (m *hashRateMeter) Stop() {
	stopped := atomic.SwapUint32(&m.stopped, 1)
	if stopped != 1 {
		arbiter.mu.Lock()
		delete(arbiter.meters, m)
		arbiter.mu.Unlock()
	}
}

// Count returns the number of events recorded.
// It updates the meter to be as accurate as possible
func (m *hashRateMeter) Count() int64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.updateMeter()
	return m.snapshot.count
}

// Mark records the occurrence of n events.
func (m *hashRateMeter) Mark(n int64) {
	m.lock.Lock()
	m.snapshot.temp = n
	m.lock.Unlock()
}

// Rate returns the one-minute moving average rate of events per second.
func (m *hashRateMeter) Rate() float64 {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.snapshot.rate
}

// Snapshot returns a read-only copy of the meter.
func (m *hashRateMeter) Snapshot() *meterSnapshot {
	m.lock.RLock()
	snapshot := *m.snapshot
	m.lock.RUnlock()
	return &snapshot
}

func (m *hashRateMeter) updateSnapshot() {
	// should run with write lock held on m.lock
	snapshot := m.snapshot
	snapshot.rate = m.a1.Rate()
}

func (m *hashRateMeter) updateMeter() {
	// should only run with write lock held on m.lock
	n := atomic.SwapInt64(&m.snapshot.temp, 0)
	m.snapshot.count += n
	m.a1.Update(n)
}

func (m *hashRateMeter) tick() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.updateMeter()
	m.a1.Tick()
	m.updateSnapshot()
}

// meterArbiter ticks meters every 5s from a single goroutine.
// meters are references in a set for future stopping.
type meterArbiter struct {
	mu      sync.RWMutex
	started bool
	meters  map[*hashRateMeter]struct{}
	ticker  *time.Ticker
}

var arbiter = meterArbiter{ticker: time.NewTicker(5 * time.Second), meters: make(map[*hashRateMeter]struct{})}

// Ticks meters on the scheduled interval
func (ma *meterArbiter) tick() {
	defer dbg.LogPanic()
	for range ma.ticker.C {
		ma.tickMeters()
	}
}

func (ma *meterArbiter) tickMeters() {
	ma.mu.RLock()
	defer ma.mu.RUnlock()
	for meter := range ma.meters {
		meter.tick()
	}
}

// ewma is the standard implementation of an EWMA and tracks the number
// of uncounted events and processes them on each tick.  It uses the
// sync/atomic package to manage uncounted events.
type ewma struct {
	uncounted int64 // /!\ this should be the first member to ensure 64-bit alignment
	alpha     float64
	rate      float64
	init      bool
	mutex     sync.Mutex
}

// Rate returns the moving average rate of events per second.
func (a *ewma) Rate() float64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.rate * float64(time.Second)
}

// Tick ticks the clock to update the moving average.  It assumes it is called
// every five seconds.
func (a *ewma) Tick() {
	count := atomic.LoadInt64(&a.uncounted)
	atomic.AddInt64(&a.uncounted, -count)
	instantRate := float64(count) / float64(5*time.Second)
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
func (a *ewma) Update(n int64) {
	atomic.AddInt64(&a.uncounted, n)
}
