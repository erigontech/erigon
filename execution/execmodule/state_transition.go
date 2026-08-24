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

package execmodule

import "context"

// StateTransitionPoint identifies an RPC or forkchoice lifecycle boundary.
type StateTransitionPoint uint8

const (
	StateTransitionRPCViewBound StateTransitionPoint = iota
	StateTransitionUnwindComplete
	StateTransitionOverlayPublished
	StateTransitionCommitComplete
	StateTransitionOverlayCleared
)

// StateTransitionObserver runs synchronously at each lifecycle boundary.
type StateTransitionObserver func(context.Context, StateTransitionPoint)

func (c *Cache) observeStateTransition(ctx context.Context, point StateTransitionPoint) {
	if c.stateTransitionObserver != nil {
		c.stateTransitionObserver(ctx, point)
	}
}

func (e *ExecModule) observeStateTransition(ctx context.Context, point StateTransitionPoint) {
	if e.stateTransitionObserver != nil {
		e.stateTransitionObserver(ctx, point)
	}
}
