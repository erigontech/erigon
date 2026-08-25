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

// StateTransitionPoint identifies where an integration test may pause RPC view
// binding or forkchoice processing.
type StateTransitionPoint uint8

const (
	// StateTransitionRPCViewBound means the RPC getter has selected its
	// SharedDomains or database view, but no state has been read yet.
	StateTransitionRPCViewBound StateTransitionPoint = iota
	// StateTransitionUnwindComplete means forkchoice unwind replay has finished,
	// before replacement canonical state is published.
	StateTransitionUnwindComplete
	// StateTransitionOverlayPublished means new RPC views can read the FCU result
	// from SharedDomains before it is durable.
	StateTransitionOverlayPublished
	// StateTransitionCommitComplete means the FCU result is durable while the
	// published SharedDomains remains available to RPC views.
	StateTransitionCommitComplete
	// StateTransitionOverlayCleared means new RPC views fall back to the
	// committed database after the FCU-published SharedDomains is unpublished.
	StateTransitionOverlayCleared
)

// StateTransitionObserver is an integration-test hook that runs inline at each
// lifecycle boundary. A blocking observer must return when its context ends.
type StateTransitionObserver func(context.Context, StateTransitionPoint)

func (e *ExecModule) observeStateTransition(ctx context.Context, point StateTransitionPoint) {
	if e.stateTransitionObserver != nil {
		e.stateTransitionObserver(ctx, point)
	}
}
