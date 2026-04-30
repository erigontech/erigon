// Copyright 2026 The Erigon Authors
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

package harness

import (
	"sync"
	"time"
)

// Schedule fires actions at offsets from a fixed start time. Tests use
// it to drive Inventory mutations on a deterministic timeline — "at
// t=100ms, mark file X local" is one Schedule.At call.
//
// All offsets are relative to the Schedule's start (set at NewSchedule).
// Actions run on their own goroutines; the caller observes their effects
// through Inventory state and read-handle outputs. Wait() blocks until
// every scheduled action has fired, useful at end-of-test to ensure no
// leaked goroutine outlives the assertion.
type Schedule struct {
	start time.Time
	wg    sync.WaitGroup
}

// NewSchedule starts the timeline now.
func NewSchedule() *Schedule {
	return &Schedule{start: time.Now()}
}

// At schedules fn to fire at start + offset. If offset has already
// elapsed (e.g. start was several ms ago), fn fires immediately on its
// own goroutine.
func (s *Schedule) At(offset time.Duration, fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		d := time.Until(s.start.Add(offset))
		if d > 0 {
			time.Sleep(d)
		}
		fn()
	}()
}

// Wait blocks until every action scheduled via At has run.
func (s *Schedule) Wait() {
	s.wg.Wait()
}

// Elapsed returns wall-clock duration since the Schedule started.
func (s *Schedule) Elapsed() time.Duration {
	return time.Since(s.start)
}
