// Copyright 2024 The Erigon Authors
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

import (
	"time"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

var (
	updateForkChoiceArrivalDelay  = metrics.NewSummary(`update_fork_choice{type="arrival_delay"}`)
	updateForkChoiceDuration      = metrics.NewSummary(`update_fork_choice{type="execution_duration"}`)
	updateForkChoiceDepth         = metrics.NewSummary(`update_fork_choice{type="fork_depth"}`)
	updateForkChoicePruneDuration = metrics.NewSummary(`update_fork_choice{type="prune_duration"}`)
	validateChainDuration         = metrics.NewSummary(`validate_chain{type="execution_duration"}`)
	insertBlocksDuration          = metrics.NewSummary(`insert_blocks{type="execution_duration"}`)

	// Semaphore wait inside InsertBlocks, isolated from the insert body: how
	// long the next block waited for the previous FCU's durability tail to
	// release the module semaphore.
	insertBlocksSemaphoreWait = metrics.NewSummary(`insert_blocks{type="semaphore_wait"}`)
	// Time the module semaphore stays held after the FCU result was returned
	// early to the consensus client (flush+commit+prune tail). The quantity the
	// tail-decoupling work is meant to shrink.
	updateForkChoiceTailHold = metrics.NewSummary(`update_fork_choice{type="tail_hold"}`)

	// Count of ops that bailed with Busy because the semaphore was held (a
	// ValidateChain bail surfaces to the CL as SYNCING).
	semaphoreBusyValidateChain    = metrics.NewCounter(`execmodule_semaphore_busy{op="validate_chain"}`)
	semaphoreBusyUpdateForkChoice = metrics.NewCounter(`execmodule_semaphore_busy{op="update_fork_choice"}`)
)

// observeForkChoiceTailHold records how long the semaphore was held past the
// early FCU return. No-op when start is zero (paths that never returned early).
func observeForkChoiceTailHold(start time.Time) {
	if start.IsZero() {
		return
	}
	updateForkChoiceTailHold.ObserveDuration(start)
}

func UpdateForkChoiceArrivalDelay(blockTime uint64) {
	t := time.Unix(int64(blockTime), 0)
	updateForkChoiceArrivalDelay.ObserveDuration(t)
}

func UpdateForkChoiceDuration(start time.Time) {
	updateForkChoiceDuration.ObserveDuration(start)
}

func UpdateForkChoiceDepth(depth uint64) {
	updateForkChoiceDepth.Observe(float64(depth))
}

func UpdateForkChoicePruneDuration(start time.Time) {
	updateForkChoicePruneDuration.ObserveDuration(start)
}
