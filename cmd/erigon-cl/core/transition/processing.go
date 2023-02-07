package transition

import (
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func computeSigningRootEpoch(epoch uint64, domain []byte) (libcommon.Hash, error) {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint64(b, epoch)
	return utils.Keccak256(b, domain), nil
}

func (s *StateTransistor) ProcessBlockHeader(block *cltypes.BeaconBlock) error {
	if !s.noValidate {
		if block.Slot != s.state.Slot() {
			return fmt.Errorf("state slot: %d, not equal to block slot: %d", s.state.Slot(), block.Slot)
		}
		if block.Slot <= s.state.LatestBlockHeader().Slot {
			return fmt.Errorf("slock slot: %d, not greater than latest block slot: %d", block.Slot, s.state.LatestBlockHeader().Slot)
		}
		propInd, err := s.state.GetBeaconProposerIndex()
		if err != nil {
			return fmt.Errorf("error in GetBeaconProposerIndex: %v", err)
		}
		if block.ProposerIndex != propInd {
			return fmt.Errorf("block proposer index: %d, does not match beacon proposer index: %d", block.ProposerIndex, propInd)
		}
		latestRoot, err := s.state.LatestBlockHeader().HashSSZ()
		if err != nil {
			return fmt.Errorf("unable to hash tree root of latest block header: %v", err)
		}
		if block.ParentRoot != latestRoot {
			return fmt.Errorf("block parent root: %x, does not match latest block root: %x", block.ParentRoot, latestRoot)
		}
	}

	bodyRoot, err := block.Body.HashSSZ()
	if err != nil {
		return fmt.Errorf("unable to hash tree root of block body: %v", err)
	}
	s.state.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		BodyRoot:      bodyRoot,
	})

	proposer, err := s.state.ValidatorAt(int(block.ProposerIndex))
	if err != nil {
		return err
	}
	if proposer.Slashed {
		return fmt.Errorf("proposer: %d is slashed", block.ProposerIndex)
	}
	return nil
}

func (s *StateTransistor) ProcessRandao(randao [96]byte, proposerIndex uint64) error {
	epoch := s.state.Epoch()
	proposer, err := s.state.ValidatorAt(int(proposerIndex))
	if err != nil {
		return err
	}
	if !s.noValidate {
		domain, err := s.state.GetDomain(s.beaconConfig.DomainRandao, epoch)
		if err != nil {
			return fmt.Errorf("ProcessRandao: unable to get domain: %v", err)
		}
		signingRoot, err := computeSigningRootEpoch(epoch, domain)
		if err != nil {
			return fmt.Errorf("ProcessRandao: unable to compute signing root: %v", err)
		}
		valid, err := bls.Verify(randao[:], signingRoot[:], proposer.PublicKey[:])
		if err != nil {
			return fmt.Errorf("ProcessRandao: unable to verify public key: %x, with signing root: %x, and signature: %x, %v", proposer.PublicKey[:], signingRoot[:], randao[:], err)
		}
		if !valid {
			return fmt.Errorf("ProcessRandao: invalid signature: public key: %x, signing root: %x, signature: %x", proposer.PublicKey[:], signingRoot[:], randao[:])
		}
	}

	randaoMixes := s.state.GetRandaoMixes(epoch)
	randaoHash := utils.Keccak256(randao[:])
	mix := [32]byte{}
	for i := range mix {
		mix[i] = randaoMixes[i] ^ randaoHash[i]
	}

	s.state.SetRandaoMixAt(int(epoch%s.beaconConfig.EpochsPerHistoricalVector), mix)
	return nil
}

func (s *StateTransistor) ProcessEth1Data(eth1Data *cltypes.Eth1Data) error {
	s.state.AddEth1DataVote(eth1Data)
	newVotes := s.state.Eth1DataVotes()

	// Count how many times body.Eth1Data appears in the votes.
	numVotes := 0
	for i := 0; i < len(newVotes); i++ {
		if eth1Data.Equal(newVotes[i]) {
			numVotes += 1
		}
	}

	if uint64(numVotes*2) > s.beaconConfig.EpochsPerEth1VotingPeriod*s.beaconConfig.SlotsPerEpoch {
		s.state.SetEth1Data(eth1Data)
	}
	return nil
}
