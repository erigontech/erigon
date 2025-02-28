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
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

var (
	ErrNotSynced                 = errors.New("not synced")
	ErrPreviousStateNotAvailable = errors.New("previous state not available")
)

var _ SyncedData = (*SyncedDataManager)(nil)

func EmptyCancel() {}

type SyncedDataManager struct {
	enabled bool
	cfg     *clparams.BeaconChainConfig

	headRoot atomic.Value
	headSlot atomic.Uint64

	headState         *state.CachingBeaconState
	previousHeadState *state.CachingBeaconState

	accessLock sync.RWMutex // lock used for accessing atomic methods
	mu         sync.RWMutex
}

func NewSyncedDataManager(cfg *clparams.BeaconChainConfig, enabled bool) *SyncedDataManager {
	return &SyncedDataManager{
		enabled: enabled,
		cfg:     cfg,
	}
}

// OnHeadState updates the current head state and tracks the previous state.
func (s *SyncedDataManager) OnHeadState(newState *state.CachingBeaconState) (err error) {
	if !s.enabled {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.accessLock.Lock()
	defer s.accessLock.Unlock()

	// Save current state as previous state, if available.
	if s.headState != nil {
		if s.previousHeadState != nil {
			err = s.headState.CopyInto(s.previousHeadState)
		} else {
			s.previousHeadState, err = s.headState.Copy()
		}
		if err != nil {
			return err
		}
	}

	var blkRoot common.Hash

	// Update headState with the new state.
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
	return nil
}

// ViewHeadState allows safe, read-only access to the current head state.
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

// ViewPreviousHeadState allows safe, read-only access to the previous head state.
func (s *SyncedDataManager) ViewPreviousHeadState(fn ViewHeadStateFn) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.previousHeadState == nil {
		return ErrPreviousStateNotAvailable
	}
	return fn(s.previousHeadState)
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
	s.accessLock.Lock()
	defer s.accessLock.Unlock()
	s.headRoot = atomic.Value{}
	s.headSlot.Store(uint64(0))
	s.headState = nil
	s.previousHeadState = nil
}

func (s *SyncedDataManager) ValidatorPublicKeyByIndex(index int) (common.Bytes48, error) {
	s.accessLock.RLock()
	defer s.accessLock.RUnlock()
	if s.headState == nil {
		return common.Bytes48{}, ErrNotSynced
	}
	return s.headState.ValidatorPublicKey(index)
}

func (s *SyncedDataManager) ValidatorIndexByPublicKey(pubkey common.Bytes48) (uint64, bool, error) {
	s.accessLock.RLock()
	defer s.accessLock.RUnlock()
	if s.headState == nil {
		return 0, false, ErrNotSynced
	}
	ret, found := s.headState.ValidatorIndexByPubkey(pubkey)
	return ret, found, nil
}

func (s *SyncedDataManager) HistoricalRootElementAtIndex(index int) (common.Hash, error) {
	s.accessLock.RLock()
	defer s.accessLock.RUnlock()
	if s.headState == nil {
		return common.Hash{}, ErrNotSynced
	}
	if s.headState.HistoricalRootsLength() <= uint64(index) {
		return common.Hash{}, errors.New("HistoricalRootElementAtIndex: index out of range")
	}

	return s.headState.HistoricalRoot(index), nil
}

func (s *SyncedDataManager) HistoricalSummaryElementAtIndex(index int) (*cltypes.HistoricalSummary, error) {
	s.accessLock.RLock()
	defer s.accessLock.RUnlock()
	if s.headState == nil {
		return nil, ErrNotSynced
	}
	if s.headState.HistoricalSummariesLength() <= uint64(index) {
		return nil, errors.New("HistoricalSummaryElementAtIndex: index out of range")
	}

	return s.headState.HistoricalSummary(index), nil
}
