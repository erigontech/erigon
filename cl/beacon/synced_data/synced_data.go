package synced_data

import (
	"sync"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

type SyncedDataManager struct {
	enabled   bool
	cfg       *clparams.BeaconChainConfig
	headState *state.CachingBeaconState

	mu sync.RWMutex
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headState == nil {
		s.headState, err = newState.Copy()
	}
	err = newState.CopyInto(s.headState)
	if err != nil {
		log.Error("failed to copy head state", "err", err)
	}

	return
}

func (s *SyncedDataManager) HeadState() (state *state.CachingBeaconState, cancel func()) {
	if !s.enabled {
		return nil, func() {}
	}
	s.mu.RLock()
	return s.headState, s.mu.RUnlock
}

func (s *SyncedDataManager) Syncing() bool {
	if !s.enabled {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.headState == nil {
		return false
	}

	headEpoch := utils.GetCurrentEpoch(s.headState.GenesisTime(), s.cfg.SecondsPerSlot, s.cfg.SlotsPerEpoch)
	// surplusMargin, give it a go if we are within 2 epochs of the head
	surplusMargin := s.cfg.SlotsPerEpoch * 2
	return s.headState.Slot()+surplusMargin < headEpoch
}

func (s *SyncedDataManager) HeadSlot() uint64 {
	if !s.enabled {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.headState == nil {
		return 0
	}
	return s.headState.Slot()
}
