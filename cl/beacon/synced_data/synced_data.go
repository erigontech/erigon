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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

var ErrNotSynced = errors.New("not synced")

var _ SyncedData = (*SyncedDataManager)(nil)

func EmptyCancel() {}

const MinHeadStateDelay = 0 * time.Millisecond

type SyncedDataManager struct {
	enabled bool
	cfg     *clparams.BeaconChainConfig

	headRoot atomic.Value
	headSlot atomic.Uint64

	headState         *state.CachingBeaconState
	minHeadStateDelay time.Duration

	mu sync.RWMutex
}

func NewSyncedDataManager(cfg *clparams.BeaconChainConfig, enabled bool, minHeadStateDelay time.Duration) *SyncedDataManager {
	return &SyncedDataManager{
		enabled:           enabled,
		cfg:               cfg,
		minHeadStateDelay: minHeadStateDelay,
	}
}

func (s *SyncedDataManager) OnHeadState(newState *state.CachingBeaconState) (err error) {
	if !s.enabled {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var blkRoot common.Hash
	start := time.Now()
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
	took := time.Since(start)
	// Delay head update to avoid being out of sync with slower nodes.
	if took < s.minHeadStateDelay {
		time.Sleep(s.minHeadStateDelay - took)
	}
	fmt.Println("DONE")
	return err
}

func (s *SyncedDataManager) ViewHeadState(fn ViewHeadStateFn) error {
	_, synced := s.headRoot.Load().(common.Hash)
	if !s.enabled || !synced {
		return ErrNotSynced
	}
	s.mu.RLock()
	if dbg.CaplinSyncedDataMangerDeadlockDetection {
		trace := dbg.Stack()
		ch := make(chan struct{})
		go func() {
			select {
			case <-time.After(100 * time.Second):
				fmt.Println("ViewHeadState timeout", trace)
			case <-ch:
				return
			}
		}()
		defer close(ch)
	}
	defer s.mu.RUnlock()
	if err := fn(s.headState); err != nil {
		return err
	}
	return nil
}

func (s *SyncedDataManager) Syncing() bool {
	_, synced := s.headRoot.Load().(common.Hash)
	return !synced
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

func (s *SyncedDataManager) CommitteeCount(epoch uint64) uint64 {
	return s.headState.CommitteeCount(epoch)
}

func (s *SyncedDataManager) UnsetHeadState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.headRoot = atomic.Value{}
	s.headSlot.Store(uint64(0))
	s.headState = nil
}
