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

package dbg

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

const FileCloseLogLevel = log.LvlTrace

// gcLeakCheckEnabled - finalizer-based leak detection. Requires both ASSERT and SLOW_TX.
var gcLeakCheckEnabled = AssertEnabled && SlowTx() > 0

// ArmGCLeakCheck panics if obj is garbage-collected before DisarmGCLeakCheck(obj)
// is called. No-op unless ASSERT and SLOW_TX are both enabled.
func ArmGCLeakCheck(name string, obj any) {
	if !gcLeakCheckEnabled {
		return
	}
	armGCLeakFinalizer(name, StackSkip(2), obj)
}

// DisarmGCLeakCheck clears the finalizer set by ArmGCLeakCheck. Call it from the
// resource's Close/Rollback path.
func DisarmGCLeakCheck(obj any) {
	if !gcLeakCheckEnabled {
		return
	}
	runtime.SetFinalizer(obj, nil)
}

func armGCLeakFinalizer(name, stack string, obj any) {
	typ := fmt.Sprintf("%T", obj)
	runtime.SetFinalizer(obj, func(any) {
		panic(fmt.Sprintf("[dbg.%s] %s garbage-collected without close — leak; created at:\n%s", name, typ, stack))
	})
}

// LeakDetector - use it to find which resource was created but not closed (leaked)
// periodically does print in logs resources which living longer than 1min with their creation stack trace
// For example db transactions can call Add/Del from Begin/Commit/Rollback methods
type LeakDetector struct {
	name          string
	panicOnLeak   bool
	enabled       atomic.Bool
	slowThreshold atomic.Pointer[time.Duration]
	autoIncrement atomic.Uint64

	list     map[uint64]LeakDetectorItem
	listLock sync.Mutex
}

type LeakDetectorItem struct {
	stack   string
	started time.Time
}

func NewLeakDetector(name string, slowThreshold time.Duration) *LeakDetector {
	enabled := slowThreshold > 0
	if !enabled {
		return nil
	}
	d := &LeakDetector{name: name, panicOnLeak: AssertEnabled, list: map[uint64]LeakDetectorItem{}}
	d.SetSlowThreshold(slowThreshold)

	go func() {
		logEvery := time.NewTicker(60 * time.Second)
		defer logEvery.Stop()

		for range logEvery.C {
			if list := d.slowList(); len(list) > 0 {
				log.Info(fmt.Sprintf("[dbg.%s] long living resources", name), "list", strings.Join(d.slowList(), ", "))
			}
		}
	}()
	return d
}

func (d *LeakDetector) slowList() (res []string) {
	if d == nil || !d.Enabled() {
		return res
	}
	slowThreshold := *d.slowThreshold.Load()

	d.listLock.Lock()
	defer d.listLock.Unlock()
	i := 0
	for key, value := range d.list {
		living := time.Since(value.started)
		if living > slowThreshold {
			res = append(res, fmt.Sprintf("%d(%s): %s", key, living, value.stack))
		}
		i++
		if i > 10 { // protect logs from too many output
			break
		}
	}
	return res
}

func (d *LeakDetector) Del(id uint64, obj any) {
	if d == nil || !d.Enabled() {
		return
	}
	if d.panicOnLeak {
		runtime.SetFinalizer(obj, nil)
	}
	d.listLock.Lock()
	defer d.listLock.Unlock()
	delete(d.list, id)
}
func (d *LeakDetector) Add(obj any) uint64 {
	if d == nil || !d.Enabled() {
		return 0
	}
	ac := LeakDetectorItem{
		stack:   StackSkip(2),
		started: time.Now(),
	}
	id := d.autoIncrement.Add(1)
	d.listLock.Lock()
	d.list[id] = ac
	d.listLock.Unlock()
	if d.panicOnLeak {
		armGCLeakFinalizer(d.name, ac.stack, obj)
	}
	return id
}

func (d *LeakDetector) Enabled() bool { return d.enabled.Load() }
func (d *LeakDetector) SetSlowThreshold(t time.Duration) {
	d.slowThreshold.Store(&t)
	d.enabled.Store(t > 0)
}
