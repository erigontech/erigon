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

package concurrent

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestClosingWaitGroup_BeginCloseIdempotent(t *testing.T) {
	var g ClosingWaitGroup
	if !g.BeginClose() {
		t.Fatal("first BeginClose should latch and return true")
	}
	if g.BeginClose() {
		t.Fatal("second BeginClose should return false")
	}
}

func TestClosingWaitGroup_TryAddRefusedAfterClose(t *testing.T) {
	var g ClosingWaitGroup
	if !g.TryAdd() {
		t.Fatal("TryAdd before close should succeed")
	}
	g.Done()

	g.BeginClose()
	if g.TryAdd() {
		t.Fatal("TryAdd after BeginClose should be refused")
	}
}

func TestClosingWaitGroup_Go(t *testing.T) {
	var g ClosingWaitGroup
	var ran atomic.Bool
	if !g.Go(func() { ran.Store(true) }) {
		t.Fatal("Go before close should start the goroutine")
	}

	g.BeginClose()
	g.Wait()
	if !ran.Load() {
		t.Fatal("Wait should join the goroutine started by Go")
	}
	if g.Go(func() { t.Error("f must not run after BeginClose") }) {
		t.Fatal("Go after BeginClose should be refused")
	}
}

// Wait must not return until every registered goroutine has called Done — this
// is the join guarantee Close relies on before tearing down shared resources.
func TestClosingWaitGroup_WaitJoinsInFlight(t *testing.T) {
	var g ClosingWaitGroup
	if !g.TryAdd() {
		t.Fatal("TryAdd should succeed")
	}

	var doneRan atomic.Bool
	go func() {
		time.Sleep(50 * time.Millisecond)
		doneRan.Store(true)
		g.Done()
	}()

	g.BeginClose()
	g.Wait()
	if !doneRan.Load() {
		t.Fatal("Wait returned before the in-flight goroutine finished")
	}
}
