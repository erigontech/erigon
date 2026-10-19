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

package state

import (
	"context"
	"sync"
)

// coldIOGate lets background HDD work yield to commitment compute, which reads the same disk. Pauses nest:
// the gate opens when every Pause has its Resume.
type coldIOGate struct {
	mu     sync.Mutex
	pauses int
	open   chan struct{}
}

func newColdIOGate() *coldIOGate {
	open := make(chan struct{})
	close(open)
	return &coldIOGate{open: open}
}

func (g *coldIOGate) Pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pauses == 0 {
		g.open = make(chan struct{})
	}
	g.pauses++
}

func (g *coldIOGate) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pauses == 0 {
		return
	}
	g.pauses--
	if g.pauses == 0 {
		close(g.open)
	}
}

func (g *coldIOGate) Wait(ctx context.Context) error {
	g.mu.Lock()
	open := g.open
	g.mu.Unlock()
	select {
	case <-open:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PauseColdIO holds background HDD copies until the returned resume is called.
func (at *AggregatorRoTx) PauseColdIO() (resume func()) {
	at.a.coldIO.Pause()
	return at.a.coldIO.Resume
}

func (at *AggregatorRoTx) ColdIOPaused() bool {
	at.a.coldIO.mu.Lock()
	defer at.a.coldIO.mu.Unlock()
	return at.a.coldIO.pauses > 0
}
