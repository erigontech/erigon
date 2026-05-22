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

package storage

import "context"

// StateAggregator is the subset of db/state.Aggregator that the storage
// Provider depends on. Depending on this interface rather than the concrete
// *state.Aggregator lets the p2p_integration harness inject a mock whose
// accessor builds are no-op-success, so the real Provider lifecycle (driver
// + orchestrator + validators) can run against synthetic fixtures that the
// real index-builder would reject. *state.Aggregator satisfies it structurally.
type StateAggregator interface {
	Files() []string
	OpenFolder() error
	BuildMissedAccessors(ctx context.Context, workers int) error
	LockCollation()
	UnlockCollation()
}
