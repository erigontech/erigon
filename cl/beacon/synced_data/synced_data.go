package synced_data

import (
	"sync/atomic"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
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

func (s *SyncedDataManager) HeadStateReader() state.BeaconStateReader {
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
