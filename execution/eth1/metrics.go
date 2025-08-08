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

package eth1

import (
	"time"

	"github.com/erigontech/erigon-lib/metrics"
)

var (
	updateForkChoiceArrivalDelay  = metrics.NewSummary(`update_fork_choice{type="arrival_delay"}`)
	updateForkChoiceDuration      = metrics.NewSummary(`update_fork_choice{type="execution_duration"}`)
	updateForkChoiceDepth         = metrics.NewSummary(`update_fork_choice{type="fork_depth"}`)
	updateForkChoicePruneDuration = metrics.NewSummary(`update_fork_choice{type="prune_duration"}`)
)

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
