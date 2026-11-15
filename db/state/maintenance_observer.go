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
	// MaintenanceFilesBuilt follows a successful step build, before integration
	// into in-process read views. Any new files are already discoverable on disk.
	// A step already covered by files may need no new files.
	MaintenanceFilesBuilt
	// MaintenanceFilesPublished follows integration into new in-process read
	// views. It also fires when the step was already covered by files.
	MaintenanceFilesPublished
	// MaintenancePruneReady means CollateAndPrune has run its prune callback, before committing its transaction.
	MaintenancePruneReady
	// MaintenancePruneCommitted follows that pass's commit, before it tries to
	// start file building. Other jobs may already be building or merging.
	MaintenancePruneCommitted
)

type MaintenanceEvent struct {
	Point MaintenancePoint
	// FilesDone, when supplied at BuildStarted, closes after that job's build
	// and any merge it runs finish, including on failure. It is nil for jobs
	// without a completion signal and does not wait for another job's merge.
	FilesDone <-chan struct{}
	// PruneTx borrows the pending transaction at PruneReady for inspection.
	// It is valid only during the observer call; do not write, commit or roll it back.
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
