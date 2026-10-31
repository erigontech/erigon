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
	"errors"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// ErrSyncCommitteeNotInState reports a slot whose sync committee is neither the current nor the next one of the state.
var ErrSyncCommitteeNotInState = errors.New("sync committee of the slot is not in the state")

// SyncCommitteeForMessageSlot returns the committee whose members sign sync committee messages and
// contributions for slot. Members assigned to slot + 1 sign for slot, so the period of slot + 1 decides,
// not the period of the state's slot: the head may still be one slot behind at the last slot of a period.
func SyncCommitteeForMessageSlot(s *CachingBeaconState, slot uint64) (*solid.SyncCommittee, error) {
	cfg := s.BeaconConfig()
	statePeriod := cfg.SyncCommitteePeriod(s.Slot())
	switch cfg.SyncCommitteePeriod(slot + 1) {
	case statePeriod:
		return s.CurrentSyncCommittee(), nil
	case statePeriod + 1:
		return s.NextSyncCommittee(), nil
	}
	return nil, ErrSyncCommitteeNotInState
}
