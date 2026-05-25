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
	"context"

	"github.com/erigontech/erigon/db/services"
	storagecomp "github.com/erigontech/erigon/node/components/storage"
)

// mockAggregator stands in for db/state.Aggregator below the storage
// Provider. Its accessor builds are no-op-success so the real Provider
// lifecycle can run against synthetic harness fixtures that a real E3
// index build would reject.
type mockAggregator struct{}

var _ storagecomp.StateAggregator = mockAggregator{}

func (mockAggregator) Files() []string                                 { return nil }
func (mockAggregator) OpenFolder() error                               { return nil }
func (mockAggregator) BuildMissedAccessors(context.Context, int) error { return nil }
func (mockAggregator) LockCollation()                                  {}
func (mockAggregator) UnlockCollation()                                {}
func (mockAggregator) StepSize() uint64                                { return 0 }

// noopDBEventNotifier is the harness stand-in for shards.Events — the
// Provider only forwards OnNewSnapshot through it, which the harness has
// no consumer for.
type noopDBEventNotifier struct{}

var _ services.DBEventNotifier = noopDBEventNotifier{}

func (noopDBEventNotifier) OnNewSnapshot() {}
