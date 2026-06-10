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

// Package caplin is the CL flow-orchestration component. It subscribes
// to flow.UnwindCompleted on the storage bus and tears down + relaunches
// the in-process Caplin runtime so the CL renegotiates its anchor (via
// normal checkpoint-sync) against the post-unwind EL head.
//
// The component owns no CL-internal primitives: the restart path goes
// through CaplinService, and the EL-side catchup is driven by Erigon's
// existing engineapi initialCycle + snapshot-backed BlockReader path.
// See docs/plans/20260609-mode-b-cl-rewind-gap.md for the design.
package caplin

import (
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

// Restarter is the subset of CaplinService the component drives. Defined
// as an interface so unit tests can stub it.
type Restarter interface {
	Restart() error
}

// Provider is the CL component runtime state.
type Provider struct {
	mu         sync.Mutex
	bus        event.EventBus
	log        log.Logger
	restarter  Restarter
	subscribed bool
}

// NewProvider constructs the CL component. The Restarter is set
// separately via SetRestarter once CaplinService is constructed —
// backend wiring builds them in two steps because the CaplinService
// closure captures backend deps not yet available when NewProvider runs.
func NewProvider(logger log.Logger) *Provider {
	return &Provider{log: logger}
}

// SetRestarter wires the CaplinService into the component. Must be
// called before BindBus or the first UnwindCompleted event would no-op
// with a fatal log.
func (p *Provider) SetRestarter(r Restarter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.restarter = r
}

// BindBus subscribes the component to flow.UnwindCompleted events on
// the given bus. Returns an error if the subscription fails or if the
// component was already bound.
func (p *Provider) BindBus(bus event.EventBus) error {
	if p == nil {
		return fmt.Errorf("caplin.BindBus: nil provider")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.subscribed {
		return fmt.Errorf("caplin.BindBus: already bound")
	}
	if bus == nil {
		return fmt.Errorf("caplin.BindBus: nil bus")
	}
	p.bus = bus
	if err := bus.Subscribe(func(e flow.UnwindCompleted) {
		p.onUnwindCompleted(e)
	}); err != nil {
		return fmt.Errorf("caplin.BindBus: subscribe UnwindCompleted: %w", err)
	}
	p.subscribed = true
	return nil
}

// onUnwindCompleted is the bus handler — it triggers a full Caplin
// restart so the runtime checkpoint-syncs fresh against the post-unwind
// EL head. EL's engineapi initialCycle path then drives Execution
// forward through snapshot-backed blocks until the gap closes.
func (p *Provider) onUnwindCompleted(e flow.UnwindCompleted) {
	p.mu.Lock()
	r := p.restarter
	p.mu.Unlock()
	if r == nil {
		p.log.Error("[caplin-component] UnwindCompleted received but no Restarter wired; CL will wedge",
			"toBlock", e.ToBlock, "tipBlock", e.TipBlock)
		return
	}
	p.log.Info("[caplin-component] restart trigger",
		"toBlock", e.ToBlock, "tipBlock", e.TipBlock)
	if err := r.Restart(); err != nil {
		p.log.Error("[caplin-component] Caplin restart failed", "err", err,
			"toBlock", e.ToBlock, "tipBlock", e.TipBlock)
	}
}
