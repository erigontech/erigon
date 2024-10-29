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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

var _ SyncedData = (*SyncedDataManager)(nil)

type SyncedDataManager struct {
	enabled bool
	cfg     *clparams.BeaconChainConfig

	headRoot atomic.Value
	headSlot atomic.Uint64

	headState *state.CachingBeaconState

	mu              sync.RWMutex
	isTryingToWrite atomic.Bool
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
	s.isTryingToWrite.Store(true)
	defer func() {
		s.isTryingToWrite.Store(false)
	}()
	s.mu.Lock()
	defer s.mu.Unlock()

	var blkRoot common.Hash

	if s.headState == nil {
		s.headState, err = newState.Copy()
	} else {
		err = newState.CopyInto(s.headState)
	}
	if err != nil {
		return err
	}
	blkRoot, err = newState.BlockRoot()
	if err != nil {
		return err
	}
	s.headSlot.Store(newState.Slot())
	s.headRoot.Store(blkRoot)
	return err
}

func (s *SyncedDataManager) waitUntilNotWriting() {
	for {
		if !s.isTryingToWrite.Load() {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func EmptyCancel() {}

var x atomic.Int64

func (s *SyncedDataManager) HeadState() (*state.CachingBeaconState, CancelFn) {
	s.waitUntilNotWriting()
	_, synced := s.headRoot.Load().(common.Hash)
	if !s.enabled || !synced {
		return nil, EmptyCancel
	}
	isCanceled := false
	var mu sync.Mutex

	a := debug.Stack()

	s.mu.RLock()
	return s.headState, func() {
		mu.Lock()
		defer mu.Unlock()
		if isCanceled {
			return
		}
		isCanceled = true
		s.mu.RUnlock()
	}
}

func (s *SyncedDataManager) HeadStateReader() (abstract.BeaconStateReader, CancelFn) {
	return s.HeadState()
}

func (s *SyncedDataManager) Syncing() bool {
	if !s.enabled {
		return false
	}
	return s.headState == nil
}

func (s *SyncedDataManager) HeadSlot() uint64 {
	if !s.enabled {
		return 0
	}
	return s.headSlot.Load()
}

func (s *SyncedDataManager) HeadRoot() common.Hash {
	if !s.enabled {
		return common.Hash{}
	}
	root, ok := s.headRoot.Load().(common.Hash)
	if !ok {
		return common.Hash{}
	}
	return root
}
