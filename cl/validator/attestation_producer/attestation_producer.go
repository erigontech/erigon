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

package attestation_producer

import (
	"errors"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/transition"

	libcommon "github.com/erigontech/erigon-lib/common"
)

var (
	ErrHeadStateNotAvailable = errors.New("head state not available")
)

const attestationsCacheSize = 21

type attestationProducer struct {
	beaconCfg *clparams.BeaconChainConfig

	attestationsCache *lru.Cache[uint64, solid.AttestationData] // Epoch => Base AttestationData
}

func New(beaconCfg *clparams.BeaconChainConfig) AttestationDataProducer {
	attestationsCache, err := lru.New[uint64, solid.AttestationData]("attestations", attestationsCacheSize)
	if err != nil {
		panic(err)
	}

	return &attestationProducer{
		beaconCfg:         beaconCfg,
		attestationsCache: attestationsCache,
	}
}

func (ap *attestationProducer) ProduceAndCacheAttestationData(baseState *state.CachingBeaconState, slot uint64, committeeIndex uint64) (solid.AttestationData, error) {
	epoch := slot / ap.beaconCfg.SlotsPerEpoch
	baseStateBlockRoot, err := baseState.BlockRoot()
	if err != nil {
		return solid.AttestationData{}, err
	}
	if baseAttestationData, ok := ap.attestationsCache.Get(slot); ok {
		beaconBlockRoot := baseStateBlockRoot
		if baseState.Slot() > slot {
			beaconBlockRoot, err = baseState.GetBlockRootAtSlot(slot)
			if err != nil {
				return solid.AttestationData{}, err
			}
		}
		return solid.NewAttestionDataFromParameters(
			slot,
			committeeIndex,
			beaconBlockRoot,
			baseAttestationData.Source(),
			baseAttestationData.Target(),
		), nil
	}
	stateEpoch := state.Epoch(baseState)

	if baseState.Slot() > slot {
		return solid.AttestationData{}, errors.New("head state slot is bigger than requested slot, the attestation should have been cached, try again later.")
	}

	if stateEpoch < epoch {
		baseState, err = baseState.Copy()
		if err != nil {
			return solid.AttestationData{}, err
		}
		if err := transition.DefaultMachine.ProcessSlots(baseState, slot); err != nil {
			return solid.AttestationData{}, err
		}
	}

	targetEpoch := state.Epoch(baseState)
	epochStartTargetSlot := targetEpoch * ap.beaconCfg.SlotsPerEpoch
	var targetRoot libcommon.Hash
	if epochStartTargetSlot == baseState.Slot() {
		targetRoot = baseStateBlockRoot
	} else {
		targetRoot, err = baseState.GetBlockRootAtSlot(epochStartTargetSlot)
		if err != nil {
			return solid.AttestationData{}, err
		}
		if targetRoot == (libcommon.Hash{}) {
			targetRoot = baseStateBlockRoot
		}
	}

	baseAttestationData := solid.NewAttestionDataFromParameters(
		0,
		0,
		libcommon.Hash{},
		baseState.CurrentJustifiedCheckpoint(),
		solid.NewCheckpointFromParameters(
			targetRoot,
			targetEpoch,
		),
	)
	ap.attestationsCache.Add(slot, baseAttestationData)
	return solid.NewAttestionDataFromParameters(
		slot,
		committeeIndex,
		baseStateBlockRoot,
		baseAttestationData.Source(),
		baseAttestationData.Target(),
	), nil
}
