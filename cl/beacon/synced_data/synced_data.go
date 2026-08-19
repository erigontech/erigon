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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
)

var (
	ErrNotSynced                 = errors.New("not synced")
	ErrPreviousStateNotAvailable = errors.New("previous state not available")
)

var _ SyncedData = (*SyncedDataManager)(nil)

type SyncedDataManager struct {
	enabled bool
	cfg     *clparams.BeaconChainConfig

	selectedHead atomic.Pointer[headIdentity]
	stateHead    atomic.Pointer[headIdentity]

	headState         *state.CachingBeaconState
	previousHeadState *state.CachingBeaconState

	selectedHeadMu sync.RWMutex
	accessLock     sync.RWMutex // lock used for accessing atomic methods
	mu             sync.RWMutex
}

type headIdentity struct {
	root common.Hash
	slot uint64
}

func NewSyncedDataManager(cfg *clparams.BeaconChainConfig, enabled bool) *SyncedDataManager {
	return &SyncedDataManager{
		enabled: enabled,
		cfg:     cfg,
	}
}

func (s *SyncedDataManager) OnSelectedHead(blockRoot common.Hash, blockSlot uint64) {
	if !s.enabled {
		return
	}
	s.selectedHeadMu.Lock()
	defer s.selectedHeadMu.Unlock()
	s.selectedHead.Store(&headIdentity{root: blockRoot, slot: blockSlot})
}

func (s *SyncedDataManager) SelectedHead() (common.Hash, uint64, bool) {
	if !s.enabled {
		return common.Hash{}, 0, false
	}
	head := s.selectedHead.Load()
	if head == nil {
		return common.Hash{}, 0, false
	}
	return head.root, head.slot, true
}

// ViewSelectedHead keeps the selected head unchanged while fn acts on it. Head publication waits
// for fn, so callers must keep the work bounded.
func (s *SyncedDataManager) ViewSelectedHead(fn ViewSelectedHeadFn) error {
	if !s.enabled {
		return ErrNotSynced
	}
	s.selectedHeadMu.RLock()
	defer s.selectedHeadMu.RUnlock()
	head := s.selectedHead.Load()
	if head == nil {
		return ErrNotSynced
	}
	return fn(head.root, head.slot)
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
	s.stateHead.Store(&headIdentity{root: blkRoot, slot: newState.Slot()})
	return nil
}

// OnHeadStateWithBlockRoot updates the head state with a known block root,
// avoiding recomputation of BlockRoot() which can produce incorrect results
// when the state's incremental hashing cache has been dirtied by operations
// like unrealized justification/finality processing.
func (s *SyncedDataManager) OnHeadStateWithBlockRoot(newState *state.CachingBeaconState, blockRoot common.Hash) (err error) {
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

	// Update headState with the new state.
	if s.headState == nil {
		s.headState, err = newState.Copy()
	} else {
		err = newState.CopyInto(s.headState)
	}
	if err != nil {
		return err
	}
	s.stateHead.Store(&headIdentity{root: blockRoot, slot: newState.Slot()})
	return nil
}

// ViewHeadState allows safe, read-only access to the current head state.
func (s *SyncedDataManager) ViewHeadState(fn ViewHeadStateFn) error {
	if !s.enabled || s.stateHead.Load() == nil {
		return ErrNotSynced
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.stateHead.Load() == nil || s.headState == nil {
		return ErrNotSynced
	}
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
	if err := fn(s.headState); err != nil {
		return err
	}
	return nil
}

func (s *SyncedDataManager) ViewHeadStateWithIdentity(fn ViewHeadStateWithIdentityFn) error {
	return s.ViewHeadState(func(headState *state.CachingBeaconState) error {
		head := s.stateHead.Load()
		if head == nil {
			return ErrNotSynced
		}
		return fn(headState, head.root, head.slot)
	})
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
	return s.stateHead.Load() == nil
}

func (s *SyncedDataManager) HeadSlot() uint64 {
	head := s.stateHead.Load()
	if !s.enabled || head == nil {
		return 0
	}
	return head.slot
}

func (s *SyncedDataManager) HeadRoot() common.Hash {
	head := s.stateHead.Load()
	if !s.enabled || head == nil {
		return common.Hash{}
	}
	return head.root
}

func (s *SyncedDataManager) StateHead() (common.Hash, uint64, bool) {
	if !s.enabled {
		return common.Hash{}, 0, false
	}
	head := s.stateHead.Load()
	if head == nil {
		return common.Hash{}, 0, false
	}
	return head.root, head.slot, true
}

func (s *SyncedDataManager) CommitteeCount(epoch uint64) uint64 {
	s.accessLock.RLock()
	defer s.accessLock.RUnlock()
	if s.headState == nil {
		return 0
	}
	return s.headState.CommitteeCount(epoch)
}

func (s *SyncedDataManager) UnsetHeadState() {
	s.mu.Lock()
	s.accessLock.Lock()
	s.stateHead.Store(nil)
	s.headState = nil
	s.previousHeadState = nil
	s.accessLock.Unlock()
	s.mu.Unlock()

	s.selectedHeadMu.Lock()
	s.selectedHead.Store(nil)
	s.selectedHeadMu.Unlock()
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
