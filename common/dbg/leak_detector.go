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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

const FileCloseLogLevel = log.LvlTrace

// LeakDetector - use it to find which resource was created but not closed (leaked)
// periodically does print in logs resources which living longer than 1min with their creation stack trace
// For example db transactions can call Add/Del from Begin/Commit/Rollback methods
type LeakDetector struct {
	enabled       atomic.Bool
	slowThreshold atomic.Pointer[time.Duration]
	autoIncrement atomic.Uint64
	extraInfo     atomic.Pointer[func() []any]

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
	d := &LeakDetector{list: map[uint64]LeakDetectorItem{}}
	d.SetSlowThreshold(slowThreshold)

	go func() {
		logEvery := time.NewTicker(60 * time.Second)
		defer logEvery.Stop()

		for range logEvery.C {
			if list := d.slowList(); len(list) > 0 {
				args := []any{"list", strings.Join(list, ", ")}
				if fn := d.extraInfo.Load(); fn != nil {
					args = append(args, (*fn)()...)
				}
				log.Info(fmt.Sprintf("[dbg.%s] long living resources", name), args...)
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

	type entry struct {
		key   uint64
		item  LeakDetectorItem
		alive time.Duration
	}

	d.listLock.Lock()
	now := time.Now()
	slow := make([]entry, 0, len(d.list))
	for key, value := range d.list {
		alive := now.Sub(value.started)
		if alive > slowThreshold {
			slow = append(slow, entry{key, value, alive})
		}
	}
	d.listLock.Unlock()

	// Sort oldest first so we deterministically surface the longest-living
	// resource — important on busy DBs where len(d.list) can be in the
	// hundreds and random map iteration would otherwise hide the actual leak.
	sort.Slice(slow, func(i, j int) bool { return slow[i].alive > slow[j].alive })
	if len(slow) > 10 { // protect logs from too many output
		slow = slow[:10]
	}
	for _, e := range slow {
		res = append(res, fmt.Sprintf("%d(%s): %s", e.key, e.alive, e.item.stack))
	}
	return res
}

func (d *LeakDetector) Del(id uint64) {
	if d == nil || !d.Enabled() {
		return
	}
	d.listLock.Lock()
	defer d.listLock.Unlock()
	delete(d.list, id)
}
func (d *LeakDetector) Add() uint64 {
	if d == nil || !d.Enabled() {
		return 0
	}
	ac := LeakDetectorItem{
		stack:   StackSkip(2),
		started: time.Now(),
	}
	id := d.autoIncrement.Add(1)
	d.listLock.Lock()
	defer d.listLock.Unlock()
	d.list[id] = ac
	return id
}

func (d *LeakDetector) Enabled() bool { return d.enabled.Load() }
func (d *LeakDetector) SetSlowThreshold(t time.Duration) {
	d.slowThreshold.Store(&t)
	d.enabled.Store(t > 0)
}

// SetExtraInfo attaches a callback whose return value is appended as key/value
// pairs to the periodic "long living resources" log line. Use it to surface
// owner-specific context (e.g. MDBX db size, reclaimable space) next to the
// list of leaked resources.
func (d *LeakDetector) SetExtraInfo(fn func() []any) {
	if d == nil {
		return
	}
	d.extraInfo.Store(&fn)
}
