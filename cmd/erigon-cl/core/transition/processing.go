package transition

import (
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

func computeSigningRootEpoch(epoch uint64, domain []byte) (libcommon.Hash, error) {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint64(b, epoch)
	return utils.Keccak256(b, domain), nil
}

func ProcessBlockHeader(state *state.BeaconState, block *cltypes.BeaconBlock, fullValidation bool) error {
	if fullValidation {
		if block.Slot != state.Slot() {
			return fmt.Errorf("state slot: %d, not equal to block slot: %d", state.Slot(), block.Slot)
		}
		if block.Slot <= state.LatestBlockHeader().Slot {
			return fmt.Errorf("slock slot: %d, not greater than latest block slot: %d", block.Slot, state.LatestBlockHeader().Slot)
		}
		propInd, err := state.GetBeaconProposerIndex()
		if err != nil {
			return fmt.Errorf("error in GetBeaconProposerIndex: %v", err)
		}
		if block.ProposerIndex != propInd {
			return fmt.Errorf("block proposer index: %d, does not match beacon proposer index: %d", block.ProposerIndex, propInd)
		}
		blockHeader := state.LatestBlockHeader()
		latestRoot, err := (&blockHeader).HashSSZ()
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
	state.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		BodyRoot:      bodyRoot,
	})

	proposer, err := state.ValidatorForValidatorIndex(int(block.ProposerIndex))
	if err != nil {
		return err
	}
	if proposer.Slashed {
		return fmt.Errorf("proposer: %d is slashed", block.ProposerIndex)
	}
	return nil
}

func ProcessRandao(s *state.BeaconState, randao [96]byte, proposerIndex uint64, fullValidation bool) error {
	epoch := state.Epoch(s.BeaconState)
	proposer, err := s.ValidatorForValidatorIndex(int(proposerIndex))
	if err != nil {
		return err
	}
	if fullValidation {
		domain, err := s.GetDomain(s.BeaconConfig().DomainRandao, epoch)
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

	randaoMixes := s.GetRandaoMixes(epoch)
	randaoHash := utils.Keccak256(randao[:])
	mix := [32]byte{}
	for i := range mix {
		mix[i] = randaoMixes[i] ^ randaoHash[i]
	}
	s.SetRandaoMixAt(int(epoch%s.BeaconConfig().EpochsPerHistoricalVector), mix)
	return nil
}

func ProcessEth1Data(state *state.BeaconState, eth1Data *cltypes.Eth1Data) error {
	state.AddEth1DataVote(eth1Data)
	newVotes := state.Eth1DataVotes()

	// Count how many times body.Eth1Data appears in the votes.
	numVotes := 0
	for i := 0; i < len(newVotes); i++ {
		if eth1Data.Equal(newVotes[i]) {
			numVotes += 1
		}
	}

	if uint64(numVotes*2) > state.BeaconConfig().EpochsPerEth1VotingPeriod*state.BeaconConfig().SlotsPerEpoch {
		state.SetEth1Data(eth1Data)
	}
	return nil
}
