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

// Package lifecycle provides Start/Stop primitives for Caplin
// sub-components. Its Bundle helper packages a cancellable context and
// a WaitGroup so that a component's Stop can synchronously drain its
// owned goroutines; the Group composer starts components in
// registration order and stops them in reverse.
package lifecycle

import (
	"context"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// Bundle is the goroutine-ownership primitive for a single component.
// It packages a cancellable ctx and a WaitGroup: Go tracks each spawn,
// Stop cancels the ctx and blocks until every tracked goroutine returns.
//
// Zero value is NOT ready to use: call Start(parent) first.
type Bundle struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBundle returns an unstarted Bundle. Call Start before Go/Ctx.
func NewBundle() *Bundle { return &Bundle{} }

// Start installs a cancellable ctx derived from parent. Safe to re-call
// after Stop for a new lifecycle generation.
func (b *Bundle) Start(parent context.Context) {
	b.ctx, b.cancel = context.WithCancel(parent)
}

// Ctx returns the bundle's cancellable ctx. Panics if Start was not called.
func (b *Bundle) Ctx() context.Context {
	if b.ctx == nil {
		panic("lifecycle.Bundle: Ctx before Start")
	}
	return b.ctx
}

// Go spawns fn as a tracked background goroutine. fn receives the
// bundle's ctx and MUST return when that ctx is cancelled.
func (b *Bundle) Go(fn func(ctx context.Context)) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		fn(b.ctx)
	}()
}

// Stop cancels the bundle's ctx and blocks until every goroutine
// spawned via Go has returned. No-op if never started.
func (b *Bundle) Stop() {
	if b.cancel == nil {
		return
	}
	b.cancel()
	b.wg.Wait()
	b.ctx = nil
	b.cancel = nil
}

// Group is an ordered registry of Stop hooks. Components register
// their Stop closure as they are constructed; Group.Stop invokes them
// in REVERSE registration order and waits for each to return before
// invoking the next. Idempotent.
type Group struct {
	mu      sync.Mutex
	entries []entry
	stopped bool
	logger  log.Logger
}

type entry struct {
	name string
	stop func()
}

// NewGroup returns an empty Group.
func NewGroup(logger log.Logger) *Group {
	return &Group{logger: logger}
}

// OnStop registers stop under name. Stop MUST synchronously drain the
// component's goroutines before returning. If Stop was already called
// on the group, stop is invoked immediately (guards against
// register-after-Stop races).
func (g *Group) OnStop(name string, stop func()) {
	if stop == nil {
		return
	}
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		stop()
		return
	}
	g.entries = append(g.entries, entry{name: name, stop: stop})
	g.mu.Unlock()
}

// Stop invokes each registered stop in reverse registration order.
// Each stop must synchronously drain before returning. Idempotent.
func (g *Group) Stop() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	g.stopped = true
	entries := append([]entry(nil), g.entries...)
	g.entries = nil
	g.mu.Unlock()
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		start := time.Now()
		e.stop()
		if g.logger != nil {
			g.logger.Debug("[caplin-lifecycle] stopped component", "name", e.name, "elapsed", time.Since(start))
		}
	}
}
