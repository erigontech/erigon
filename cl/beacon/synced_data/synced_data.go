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

package synced_data

import (
	"sync/atomic"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

type SyncedDataManager struct {
	enabled   bool
	cfg       *clparams.BeaconChainConfig
	headState atomic.Value
}

func NewSyncedDataManager(enabled bool, cfg *clparams.BeaconChainConfig) *SyncedDataManager {
	return &SyncedDataManager{
		enabled: enabled,
		cfg:     cfg,
	}
}

func (s *SyncedDataManager) OnHeadState(newState *state.CachingBeaconState) (err error) {
	if !s.enabled {
		return
	}
	st, err := newState.Copy()
	if err != nil {
		return err
	}
	s.headState.Store(st)

	return
}

func (s *SyncedDataManager) HeadState() *state.CachingBeaconState {
	if !s.enabled {
		return nil
	}
	if ret, ok := s.headState.Load().(*state.CachingBeaconState); ok {
		return ret
	}
	return nil
}

func (s *SyncedDataManager) HeadStateReader() abstract.BeaconStateReader {
	headstate := s.HeadState()
	if headstate == nil {
		return nil
	}
	return headstate
}

func (s *SyncedDataManager) HeadStateMutator() abstract.BeaconStateMutator {
	headstate := s.HeadState()
	if headstate == nil {
		return nil
	}
	return headstate
}

func (s *SyncedDataManager) Syncing() bool {
	if !s.enabled {
		return false
	}
	return s.headState.Load() == nil
}

func (s *SyncedDataManager) HeadSlot() uint64 {
	if !s.enabled {
		return 0
	}
	st, ok := s.headState.Load().(*state.CachingBeaconState)
	if !ok {
		return 0
	}
	return st.Slot()
}
