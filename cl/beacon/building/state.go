package building

import (
	"errors"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

var ErrInvalidSubnetTopic = errors.New("invalid subnet topic")
var ErrFailedAddAttestation = errors.New("failed to add aggregate")

type State struct {
	BeaconConfig *clparams.BeaconChainConfig

	feeRecipients     map[int]common.Address
	validAttestations map[uint64]map[common.Bytes96]*ReceivedAttestation
	mu                sync.RWMutex
}

type ReceivedAttestation struct {
	Attestation *solid.Attestation
}

func NewState() *State {
	return &State{
		feeRecipients:     map[int]common.Address{},
		validAttestations: map[uint64]map[common.Bytes96]*ReceivedAttestation{},
	}
}

func (s *State) AddAttestation(subnetTopicInt uint64, a *solid.Attestation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.validAttestations[subnetTopicInt]; !ok {
		s.validAttestations[subnetTopicInt] = make(map[common.Bytes96]*ReceivedAttestation)
	}
	s.validAttestations[subnetTopicInt][a.Signature()] = &ReceivedAttestation{Attestation: a}
	return nil
}

func (s *State) SetFeeRecipient(idx int, address common.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.feeRecipients[idx] = address
}
