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

import "context"

type MaintenancePoint uint8

const (
	// MaintenanceBuildStarted marks an eligible collation attempt, before worker locks that could block execution.
	MaintenanceBuildStarted MaintenancePoint = iota
	// MaintenanceFilesBuilt precedes integration after all builders for a step have succeeded.
	MaintenanceFilesBuilt
	// MaintenanceFilesPublished follows integration of that step into new read views.
	MaintenanceFilesPublished
	// MaintenancePruneReady means CollateAndPrune has run its prune callback, before committing its transaction.
	MaintenancePruneReady
	// MaintenancePruneCommitted means that transaction committed, before starting another file build.
	MaintenancePruneCommitted
)

// MaintenanceObserver is an inline integration-test hook. Calls may overlap;
// a blocking observer must return when its context ends. Nil disables observation.
type MaintenanceObserver func(context.Context, MaintenancePoint)

func (a *Aggregator) SetMaintenanceObserver(observer MaintenanceObserver) {
	if observer == nil {
		a.maintenanceObserver.Store(nil)
		return
	}
	a.maintenanceObserver.Store(&observer)
}

func (a *Aggregator) observeMaintenance(ctx context.Context, point MaintenancePoint) {
	if observer := a.maintenanceObserver.Load(); observer != nil {
		(*observer)(ctx, point)
	}
}
