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
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	shuffling2 "github.com/erigontech/erigon/cl/phase1/core/state/shuffling"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
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
	s := a.syncedData.HeadState()
	if s == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("node is syncing"))
	}
	dependentRoot := a.getDependentRoot(s, epoch)
	if epoch < a.forkchoiceStore.FinalizedCheckpoint().Epoch {
		tx, err := a.indiciesDB.BeginRo(r.Context())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		key := base_encoding.Encode64ToBytes4(epoch)
		indiciesBytes, err := tx.GetOne(kv.Proposers, key)
		if err != nil {
			return nil, err
		}
		if len(indiciesBytes) != int(a.beaconChainCfg.SlotsPerEpoch*4) {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, errors.New("proposer duties is corrupted"))
		}
		duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
		for i := uint64(0); i < a.beaconChainCfg.SlotsPerEpoch; i++ {
			validatorIndex := binary.BigEndian.Uint32(indiciesBytes[i*4 : i*4+4])
			var pk libcommon.Bytes48
			pk, err := state_accessors.ReadPublicKeyByIndex(tx, uint64(validatorIndex))
			if err != nil {
				return nil, err
			}
			duties[i] = proposerDuties{
				Pubkey:         pk,
				ValidatorIndex: uint64(validatorIndex),
				Slot:           epoch*a.beaconChainCfg.SlotsPerEpoch + i,
			}
		}
		return newBeaconResponse(duties).
			WithOptimistic(a.forkchoiceStore.IsHeadOptimistic()).
			WithFinalized(true).
			WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)).
			With("dependent_root", dependentRoot), nil
	}

	// We need to compute our duties
	state := a.syncedData.HeadState()
	if state == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("beacon node is syncing"))

	}

	expectedSlot := epoch * a.beaconChainCfg.SlotsPerEpoch

	duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
	wg := sync.WaitGroup{}

	for slot := expectedSlot; slot < expectedSlot+a.beaconChainCfg.SlotsPerEpoch; slot++ {
		// Lets do proposer index computation
		mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) %
			a.beaconChainCfg.EpochsPerHistoricalVector
		// Input for the seed hash.
		mix := state.GetRandaoMix(int(mixPosition))
		input := shuffling2.GetSeed(a.beaconChainCfg, mix, epoch, a.beaconChainCfg.DomainBeaconProposer)
		slotByteArray := make([]byte, 8)
		binary.LittleEndian.PutUint64(slotByteArray, slot)

		// Add slot to the end of the input.
		inputWithSlot := append(input[:], slotByteArray...)
		hash := sha256.New()

		// Calculate the hash.
		hash.Write(inputWithSlot)
		seed := hash.Sum(nil)

		indices := state.GetActiveValidatorsIndices(epoch)

		// Write the seed to an array.
		seedArray := [32]byte{}
		copy(seedArray[:], seed)
		wg.Add(1)

		// Do it in parallel
		go func(i, slot uint64, indicies []uint64, seedArray [32]byte) {
			defer wg.Done()
			proposerIndex, err := shuffling2.ComputeProposerIndex(state.BeaconState, indices, seedArray)
			if err != nil {
				panic(err)
			}
			var pk libcommon.Bytes48
			pk, err = state.ValidatorPublicKey(int(proposerIndex))
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

	return newBeaconResponse(duties).WithFinalized(false).WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)).With("dependent_root", dependentRoot), nil
}
