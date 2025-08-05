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

package handler

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	shuffling2 "github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
)

type proposerDuties struct {
	Pubkey         common.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64         `json:"validator_index,string"`
	Slot           uint64         `json:"slot,string"`
}

// isProposerDutyInLookaheadVector checks if the proposer duty is within the lookahead vector.
func (a *ApiHandler) isProposerDutyInLookaheadVector(s *state.CachingBeaconState, epoch uint64) bool {
	return s.Version() >= clparams.FuluVersion && epoch >= state.Epoch(s) && epoch <= state.Epoch(s)+a.beaconChainCfg.MinSeedLookahead
}

func (a *ApiHandler) getDutiesProposer(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	dependentRoot, err := a.getDependentRoot(epoch, false)
	if err != nil {
		return nil, err
	}

	marginEpochs := uint64(2 << 13)

	expectedSlot := epoch * a.beaconChainCfg.SlotsPerEpoch

	duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
	wg := sync.WaitGroup{}

	if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		// Lets do proposer index computation
		mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) %
			a.beaconChainCfg.EpochsPerHistoricalVector

		var mix common.Hash
		if epoch+marginEpochs > a.forkchoiceStore.FinalizedCheckpoint().Epoch {
			// Input for the seed hash.
			mix = s.GetRandaoMix(int(mixPosition))
		} else {
			tx, err := a.indiciesDB.BeginRo(r.Context())
			if err != nil {
				return err
			}
			defer tx.Rollback()
			view := a.caplinStateSnapshots.View()
			defer view.Close()

			// read the mix from the database
			mix, err = a.stateReader.ReadRandaoMixBySlotAndIndex(tx, state_accessors.GetValFnTxAndSnapshot(tx, view), expectedSlot, mixPosition)
			if err != nil {
				return err
			}
			if mix == (common.Hash{}) {
				return beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("mix not found for slot %d and index %d. maybe block was not backfilled or range was pruned", expectedSlot, mixPosition))
			}
		}

		// if the proposer duties are in the lookahead vector, we can use the lookahead vector to fetch the proposers
		if a.isProposerDutyInLookaheadVector(s, epoch) {
			lookaheadVector := s.GetProposerLookahead()
			stateEpoch := state.Epoch(s)
			startLookAheadIndex := (epoch - stateEpoch) * a.beaconChainCfg.SlotsPerEpoch
			for i := uint64(0); i < a.beaconChainCfg.SlotsPerEpoch; i++ {
				proposerIndex := lookaheadVector.Get(int(startLookAheadIndex + i))
				var pk common.Bytes48
				pk, err = s.ValidatorPublicKey(int(proposerIndex))
				if err != nil {
					panic(err)
				}
				duties[i] = proposerDuties{
					Pubkey:         pk,
					ValidatorIndex: proposerIndex,
					Slot:           (epoch * a.beaconChainCfg.SlotsPerEpoch) + i,
				}
			}
			return nil
		}

		for slot := expectedSlot; slot < expectedSlot+a.beaconChainCfg.SlotsPerEpoch; slot++ {

			slotByteArray := make([]byte, 8)
			binary.LittleEndian.PutUint64(slotByteArray, slot)

			input := shuffling2.GetSeed(a.beaconChainCfg, mix, epoch, a.beaconChainCfg.DomainBeaconProposer)
			// Add slot to the end of the input.
			inputWithSlot := append(input[:], slotByteArray...)
			hash := sha256.New()

			// Calculate the hash.
			hash.Write(inputWithSlot)
			seed := hash.Sum(nil)

			indices := s.GetActiveValidatorsIndices(epoch)

			// Write the seed to an array.
			seedArray := [32]byte{}
			copy(seedArray[:], seed)
			wg.Add(1)

			// Do it in parallel
			go func(i, slot uint64, indicies []uint64, seedArray [32]byte) {
				defer wg.Done()
				proposerIndex, err := shuffling2.ComputeProposerIndex(s.BeaconState, indices, seedArray)
				if err != nil {
					panic(err)
				}
				var pk common.Bytes48
				pk, err = s.ValidatorPublicKey(int(proposerIndex))
				if err != nil {
					panic(err)
				}
				duties[i] = proposerDuties{
					Pubkey:         pk,
					ValidatorIndex: proposerIndex,
					Slot:           slot,
				}
			}(slot-expectedSlot, slot, indices, seedArray)
		}
		wg.Wait()
		return nil
	}); err != nil {
		return nil, err
	}

	return newBeaconResponse(duties).WithFinalized(false).WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)).With("dependent_root", dependentRoot), nil
}
