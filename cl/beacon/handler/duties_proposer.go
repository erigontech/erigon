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
	"errors"
	"net/http"
	"sync"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	shuffling2 "github.com/erigontech/erigon/cl/phase1/core/state/shuffling"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
)

type proposerDuties struct {
	Pubkey         libcommon.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64            `json:"validator_index,string"`
	Slot           uint64            `json:"slot,string"`
}

func (a *ApiHandler) getDutiesProposer(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	dependentRoot, err := a.getDependentRoot(epoch)
	if err != nil {
		return nil, err
	}

	if epoch < a.forkchoiceStore.FinalizedCheckpoint().Epoch {
		tx, err := a.indiciesDB.BeginRo(r.Context())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		view := a.caplinStateSnapshots.View()
		defer view.Close()

		indicies, err := state_accessors.ReadProposersInEpoch(state_accessors.GetValFnTxAndSnapshot(tx, view), epoch)
		if err != nil {
			return nil, err
		}
		if len(indicies) == 0 {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no proposers for this epoch. either this range was prune or not backfilled"))
		}
		duties := make([]proposerDuties, len(indicies))
		for i, validatorIndex := range indicies {
			var pk libcommon.Bytes48
			pk, err := state_accessors.ReadPublicKeyByIndex(tx, validatorIndex)
			if err != nil {
				return nil, err
			}
			duties[i] = proposerDuties{
				Pubkey:         pk,
				ValidatorIndex: validatorIndex,
				Slot:           epoch*a.beaconChainCfg.SlotsPerEpoch + uint64(i),
			}
		}
		return newBeaconResponse(duties).
			WithOptimistic(a.forkchoiceStore.IsHeadOptimistic()).
			WithFinalized(true).
			WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)).
			With("dependent_root", dependentRoot), nil
	}

	expectedSlot := epoch * a.beaconChainCfg.SlotsPerEpoch

	duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
	wg := sync.WaitGroup{}

	if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		for slot := expectedSlot; slot < expectedSlot+a.beaconChainCfg.SlotsPerEpoch; slot++ {
			// Lets do proposer index computation
			mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) %
				a.beaconChainCfg.EpochsPerHistoricalVector
			// Input for the seed hash.
			mix := s.GetRandaoMix(int(mixPosition))
			input := shuffling2.GetSeed(a.beaconChainCfg, mix, epoch, a.beaconChainCfg.DomainBeaconProposer)
			slotByteArray := make([]byte, 8)
			binary.LittleEndian.PutUint64(slotByteArray, slot)

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
				var pk libcommon.Bytes48
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
