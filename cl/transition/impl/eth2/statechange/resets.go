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

package statechange

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func ProcessEth1DataReset(s abstract.BeaconState) {
	nextEpoch := state.Epoch(s) + 1
	if nextEpoch%s.BeaconConfig().EpochsPerEth1VotingPeriod == 0 {
		s.ResetEth1DataVotes()
	}
}

func ProcessSlashingsReset(s abstract.BeaconState) {
	s.SetSlashingSegmentAt(int(state.Epoch(s)+1)%int(s.BeaconConfig().EpochsPerSlashingsVector), 0)

}

func ProcessRandaoMixesReset(s abstract.BeaconState) {
	currentEpoch := state.Epoch(s)
	nextEpoch := state.Epoch(s) + 1
	s.SetRandaoMixAt(int(nextEpoch%s.BeaconConfig().EpochsPerHistoricalVector), s.GetRandaoMixes(currentEpoch))
}

func ProcessParticipationFlagUpdates(state abstract.BeaconState) {
	defer monitor.ObserveElaspedTime(monitor.ProcessParticipationFlagUpdatesTime).End()
	state.ResetEpochParticipation()
}
