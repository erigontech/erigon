package peerdasstate

import (
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon-p2p/enode"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
)

// PeerDasState stores the state parameters for peer das. Keep it simple.
type PeerDasState struct {
	beaconConfig *clparams.BeaconChainConfig
	nodeID       atomic.Pointer[enode.ID]

	// cgc related
	cgcMutex      sync.RWMutex
	realCgc       uint64 // real custody group count
	advertisedCgc uint64 // advertised custody group count

	// earliest available slot
	earliestAvailableSlot atomic.Uint64

	// cache
	custodyColumnsCache atomic.Pointer[map[cltypes.CustodyIndex]bool] // map[cltypes.CustodyIndex]bool
}

func NewPeerDasState(beaconConfig *clparams.BeaconChainConfig, nodeID enode.ID) *PeerDasState {
	return &PeerDasState{
		nodeID:              atomic.Pointer[enode.ID]{},
		beaconConfig:        beaconConfig,
		realCgc:             beaconConfig.CustodyRequirement,
		advertisedCgc:       beaconConfig.CustodyRequirement,
		custodyColumnsCache: atomic.Pointer[map[cltypes.CustodyIndex]bool]{},
	}
}

func (s *PeerDasState) GetEarliestAvailableSlot() uint64 {
	return s.earliestAvailableSlot.Load()
}

func (s *PeerDasState) SetEarliestAvailableSlot(slot uint64) {
	s.earliestAvailableSlot.Store(slot)
}

func (s *PeerDasState) GetRealCgc() uint64 {
	s.cgcMutex.RLock()
	defer s.cgcMutex.RUnlock()
	return s.realCgc
}

func (s *PeerDasState) SetCustodyGroupCount(cgc uint64) bool {
	s.cgcMutex.Lock()
	defer s.cgcMutex.Unlock()
	s.realCgc = cgc
	if cgc > s.advertisedCgc {
		s.advertisedCgc = cgc
		s.custodyColumnsCache.Store(nil) // clear the cache
		// maybe need to update earliest available slot
		return true
	}
	return false
}

func (s *PeerDasState) GetAdvertisedCgc() uint64 {
	s.cgcMutex.RLock()
	defer s.cgcMutex.RUnlock()
	return s.advertisedCgc
}

func (s *PeerDasState) GetMyCustodyColumns() (map[cltypes.CustodyIndex]bool, error) {
	custodyColumns := s.custodyColumnsCache.Load()
	if custodyColumns != nil {
		return *custodyColumns, nil
	}
	updatedCustodyColumns, err := peerdasutils.GetCustodyColumns(*s.nodeID.Load(), s.GetAdvertisedCgc())
	if err != nil {
		return nil, err
	}
	s.custodyColumnsCache.Store(&updatedCustodyColumns)
	return updatedCustodyColumns, nil
}

func (s *PeerDasState) SetNodeID(nodeID enode.ID) {
	s.nodeID.Store(&nodeID)
	s.custodyColumnsCache.Store(nil) // clear the cache
}
