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
	"fmt"
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

var a sync.Map

func (ap *attestationProducer) computeTargetCheckpoint(baseState *state.CachingBeaconState, slot uint64) (solid.Checkpoint, error) {
	baseStateBlockRoot, err := baseState.BlockRoot()
	if err != nil {
		return solid.Checkpoint{}, err
	}

	targetEpoch := slot / ap.beaconCfg.SlotsPerEpoch
	epochStartTargetSlot := targetEpoch * ap.beaconCfg.SlotsPerEpoch
	var targetRoot libcommon.Hash

	if epochStartTargetSlot == baseState.Slot() {
		targetRoot = baseStateBlockRoot
	} else {
		targetRoot, err = baseState.GetBlockRootAtSlot(epochStartTargetSlot)
		if err != nil {
			return solid.Checkpoint{}, fmt.Errorf("failed to get targetRoot at slot %d: %w", epochStartTargetSlot, err)
		}
		if targetRoot == (libcommon.Hash{}) {
			// if the target root is not found, we can't generate the attestation
			return solid.Checkpoint{}, ErrHeadStateBehind
		}
	}
	if _, ok := a.Load(targetRoot); !ok {
		fmt.Println("epoch", targetEpoch, "targetRoot", targetRoot)
	}
	a.Store(targetRoot, targetEpoch)
	return solid.Checkpoint{
		Root:  targetRoot,
		Epoch: targetEpoch,
	}, nil
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
				return solid.AttestationData{}, fmt.Errorf("failed to get block root at slot (cache round 1) %d: %w", slot, err)
			}
		}
		targetCheckpoint, err := ap.computeTargetCheckpoint(baseState, slot)
		if err != nil {
			return solid.AttestationData{}, err
		}
		return solid.AttestationData{
			Slot:            slot,
			CommitteeIndex:  committeeIndex,
			BeaconBlockRoot: beaconBlockRoot,
			Source:          baseAttestationData.Source,
			Target:          targetCheckpoint,
		}, nil
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
				return solid.AttestationData{}, fmt.Errorf("failed to get block root at slot (cache round 2) %d: %w", slot, err)
			}
		}
		targetCheckpoint, err := ap.computeTargetCheckpoint(baseState, slot)
		if err != nil {
			return solid.AttestationData{}, err
		}
		return solid.AttestationData{
			Slot:            slot,
			CommitteeIndex:  committeeIndex,
			BeaconBlockRoot: beaconBlockRoot,
			Source:          baseAttestationData.Source,
			Target:          targetCheckpoint,
		}, nil
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

	targetCheckpoint, err := ap.computeTargetCheckpoint(baseState, slot)
	if err != nil {
		return solid.AttestationData{}, err
	}
	baseAttestationData := solid.AttestationData{
		Slot:            0,                // slot will be filled in later
		CommitteeIndex:  0,                // committee index will be filled in later
		BeaconBlockRoot: libcommon.Hash{}, // beacon block root will be filled in later
		Source:          baseState.CurrentJustifiedCheckpoint(),
		Target:          targetCheckpoint,
	}
	ap.attestationsCache.Add(epoch, baseAttestationData)
	return solid.AttestationData{
		Slot:            slot,
		CommitteeIndex:  committeeIndex,
		BeaconBlockRoot: baseStateBlockRoot,
		Source:          baseAttestationData.Source,
		Target:          targetCheckpoint,
	}, nil
}
