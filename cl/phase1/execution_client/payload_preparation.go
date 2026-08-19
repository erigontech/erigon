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

package execution_client

import (
	"context"
	"errors"
	"sync"
)

// ErrPayloadPreparationPreempted reports that non-speculative execution work superseded a payload
// preparation.
var ErrPayloadPreparationPreempted = errors.New("payload preparation preempted by execution work")

type payloadPreparationBinding struct {
	cancel context.CancelCauseFunc
}

// payloadPreparationCoordinator gives non-speculative execution calls priority over preparation.
// Starting critical work cancels active bindings and prevents new preparation until every critical
// call finishes.
type payloadPreparationCoordinator struct {
	mu       sync.Mutex
	critical int
	bindings map[*payloadPreparationBinding]struct{}
}

func (c *payloadPreparationCoordinator) bind(ctx context.Context) (context.Context, func()) {
	prepareCtx, cancel := context.WithCancelCause(ctx)
	binding := &payloadPreparationBinding{cancel: cancel}

	c.mu.Lock()
	if c.critical > 0 {
		c.mu.Unlock()
		cancel(ErrPayloadPreparationPreempted)
	} else {
		if c.bindings == nil {
			c.bindings = map[*payloadPreparationBinding]struct{}{}
		}
		c.bindings[binding] = struct{}{}
		c.mu.Unlock()
	}

	return prepareCtx, sync.OnceFunc(func() {
		cancel(context.Canceled)
		c.mu.Lock()
		delete(c.bindings, binding)
		c.mu.Unlock()
	})
}

func (c *payloadPreparationCoordinator) beginCritical() func() {
	c.mu.Lock()
	c.critical++
	bindings := make([]*payloadPreparationBinding, 0, len(c.bindings))
	for binding := range c.bindings {
		bindings = append(bindings, binding)
	}
	c.mu.Unlock()

	for _, binding := range bindings {
		binding.cancel(ErrPayloadPreparationPreempted)
	}

	return sync.OnceFunc(func() {
		c.mu.Lock()
		c.critical--
		c.mu.Unlock()
	})
}

type payloadPreparationBinder interface {
	bindPayloadPreparation(context.Context) (context.Context, func())
}

// BindPayloadPreparation lets execution-critical calls preempt speculative preparation on clients
// that share an in-process execution layer. The caller must release the binding when preparation
// ends.
func BindPayloadPreparation(ctx context.Context, engine ExecutionEngine) (context.Context, func()) {
	binder, ok := engine.(payloadPreparationBinder)
	if !ok {
		return ctx, func() {}
	}
	return binder.bindPayloadPreparation(ctx)
}
