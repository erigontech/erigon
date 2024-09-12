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
	"context"
	"errors"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/transition"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
)

var (
	ErrHeadStateNotAvailable = errors.New("head state not available")
	ErrHeadStateBehind       = errors.New("head state is behind")
)

const attestationsCacheSize = 21

type attestationProducer struct {
	beaconCfg *clparams.BeaconChainConfig

	attCacheMutex     sync.RWMutex
	attestationsCache *lru.CacheWithTTL[uint64, solid.AttestationData] // Epoch => Base AttestationData
}

func New(ctx context.Context, beaconCfg *clparams.BeaconChainConfig) AttestationDataProducer {
	ttl := time.Duration(beaconCfg.SecondsPerSlot) * time.Second
	attestationsCache := lru.NewWithTTL[uint64, solid.AttestationData]("attestations", attestationsCacheSize, ttl)
	p := &attestationProducer{
		beaconCfg:         beaconCfg,
		attestationsCache: attestationsCache,
	}
	return p
}

func (ap *attestationProducer) ProduceAndCacheAttestationData(baseState *state.CachingBeaconState, slot uint64, committeeIndex uint64) (solid.AttestationData, error) {
	epoch := slot / ap.beaconCfg.SlotsPerEpoch
	baseStateBlockRoot, err := baseState.BlockRoot()
	if err != nil {
		return solid.AttestationData{}, err
	}

	ap.attCacheMutex.RLock()
	if baseAttestationData, ok := ap.attestationsCache.Get(epoch); ok {
		ap.attCacheMutex.RUnlock()
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
	ap.attCacheMutex.RUnlock()

	// in case the target epoch is not found, let's generate it with lock to avoid everyone trying to generate it
	// at the same time, which would be a waste of memory resources
	ap.attCacheMutex.Lock()
	defer ap.attCacheMutex.Unlock()
	// check again if the target epoch is already generated
	if baseAttestationData, ok := ap.attestationsCache.Get(epoch); ok {
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

	targetEpoch := slot / ap.beaconCfg.SlotsPerEpoch
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
			// if the target root is not found, we can't generate the attestation
			return solid.AttestationData{}, ErrHeadStateBehind
		}
	}

	stateEpoch := state.Epoch(baseState)
	if baseState.Slot() > slot {
		return solid.AttestationData{}, errors.New("head state slot is bigger than requested slot, the attestation should have been cached, try again later")
	}
	if stateEpoch < epoch {
		baseState, err = baseState.Copy()
		if err != nil {
			log.Warn("Failed to copy base state", "slot", slot, "err", err)
			return solid.AttestationData{}, err
		}
		if err := transition.DefaultMachine.ProcessSlots(baseState, slot); err != nil {
			log.Warn("Failed to process slots", "slot", slot, "err", err)
			return solid.AttestationData{}, err
		}
		if err != nil {
			return solid.AttestationData{}, err
		}
	}

	baseAttestationData := solid.NewAttestionDataFromParameters(
		0,                // slot will be filled in later
		0,                // committee index will be filled in later
		libcommon.Hash{}, // beacon block root will be filled in later
		baseState.CurrentJustifiedCheckpoint(),
		solid.NewCheckpointFromParameters(
			targetRoot,
			targetEpoch,
		),
	)
	ap.attestationsCache.Add(epoch, baseAttestationData)
	return solid.NewAttestionDataFromParameters(
		slot,
		committeeIndex,
		baseStateBlockRoot,
		baseAttestationData.Source(),
		baseAttestationData.Target(),
	), nil
}
