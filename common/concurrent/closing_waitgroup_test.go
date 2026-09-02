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

func TestClosingWaitGroup_TryGo(t *testing.T) {
	var g ClosingWaitGroup
	var ran atomic.Bool
	if !g.TryGo(func() { ran.Store(true) }) {
		t.Fatal("TryGo before close should start the goroutine")
	}

	g.BeginClose()
	g.Wait()
	if !ran.Load() {
		t.Fatal("Wait should join the goroutine started by TryGo")
	}
	if g.TryGo(func() { t.Error("f must not run after BeginClose") }) {
		t.Fatal("TryGo after BeginClose should be refused")
	}
}

// Wait must not return until every registered goroutine has called Done — this
// is the join guarantee Close relies on before tearing down shared resources.
func TestClosingWaitGroup_WaitJoinsInFlight(t *testing.T) {
	var g ClosingWaitGroup
	if !g.TryAdd() {
		t.Fatal("TryAdd should succeed")
	}

	release := make(chan struct{})
	var doneRan atomic.Bool
	go func() {
		<-release
		doneRan.Store(true)
		g.Done()
	}()

	g.BeginClose()

	waited := make(chan struct{})
	go func() {
		g.Wait()
		close(waited)
	}()

	// The goroutine is in-flight (Done not yet called), so Wait must still block.
	select {
	case <-waited:
		t.Fatal("Wait returned before the in-flight goroutine called Done")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)

	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after Done")
	}
	if !doneRan.Load() {
		t.Fatal("Wait returned before the in-flight goroutine finished")
	}
}
