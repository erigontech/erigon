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

	"github.com/erigontech/erigon/db/kv"
)

type MaintenancePoint uint8

const (
	// MaintenanceBuildStarted marks an eligible collation attempt.
	MaintenanceBuildStarted MaintenancePoint = iota
	// MaintenanceFilesBuilt means all builders for a step succeeded, before
	// integration into in-process read views. Files already exist on disk and
	// can be discovered after a restart.
	MaintenanceFilesBuilt
	// MaintenanceFilesPublished follows integration of that step's files into
	// new in-process read views.
	MaintenanceFilesPublished
	// MaintenancePruneReady means CollateAndPrune has run its prune callback, before committing its transaction.
	MaintenancePruneReady
	// MaintenancePruneCommitted means that transaction committed, before starting another file build.
	MaintenancePruneCommitted
)

type MaintenanceEvent struct {
	Point MaintenancePoint
	// FilesDone is set at BuildStarted for background jobs. It closes after
	// that job's build and any merge it runs finish, including on failure;
	// it does not wait for a merge already running in another job.
	FilesDone <-chan struct{}
	// PruneTx is a read-only view of the pending transaction at PruneReady.
	// It is valid only during the observer call.
	PruneTx kv.Tx
}

// MaintenanceObserver is an inline integration-test hook. Calls may overlap.
// Its context ends when the operation is cancelled or Aggregator.Close starts;
// a blocking observer must then return. Nil disables observation.
type MaintenanceObserver func(context.Context, MaintenanceEvent)

func (a *Aggregator) SetMaintenanceObserver(observer MaintenanceObserver) {
	if observer == nil {
		a.maintenanceObserver.Store(nil)
		return
	}
	a.maintenanceObserver.Store(&observer)
}

func (a *Aggregator) observeMaintenance(ctx context.Context, event MaintenanceEvent) {
	if observer := a.maintenanceObserver.Load(); observer != nil {
		observerCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(a.observerCtx, cancel)
		defer stop()
		(*observer)(observerCtx, event)
	}
}
