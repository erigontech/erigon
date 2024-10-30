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

	"github.com/erigontech/erigon/v3/cl/clparams"
	"github.com/erigontech/erigon/v3/cl/cltypes/solid"
	"github.com/erigontech/erigon/v3/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/v3/cl/phase1/core/state"
	"github.com/erigontech/erigon/v3/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/v3/cl/transition"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
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

func (ap *attestationProducer) computeTargetCheckpoint(tx kv.Tx, baseState *state.CachingBeaconState, baseStateBlockRoot libcommon.Hash, slot uint64) (solid.Checkpoint, error) {
	var err error
	targetEpoch := slot / ap.beaconCfg.SlotsPerEpoch
	epochStartTargetSlot := targetEpoch * ap.beaconCfg.SlotsPerEpoch
	var targetRoot libcommon.Hash
	if tx != nil {
		targetRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, epochStartTargetSlot)
		if err != nil {
			return solid.Checkpoint{}, fmt.Errorf("failed to get targetRoot at slot from db %d: %w", epochStartTargetSlot, err)
		}
		if targetRoot != (libcommon.Hash{}) {
			return solid.Checkpoint{
				Root:  targetRoot,
				Epoch: targetEpoch,
			}, nil
		}
	}

	if epochStartTargetSlot >= baseState.Slot() {
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
	return solid.Checkpoint{
		Root:  targetRoot,
		Epoch: targetEpoch,
	}, nil
}

func (ap *attestationProducer) ProduceAndCacheAttestationData(tx kv.Tx, baseState *state.CachingBeaconState, baseStateBlockRoot libcommon.Hash, slot uint64, committeeIndex uint64) (solid.AttestationData, error) {
	epoch := slot / ap.beaconCfg.SlotsPerEpoch
	var err error

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
		targetCheckpoint, err := ap.computeTargetCheckpoint(tx, baseState, baseStateBlockRoot, slot)
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
		targetCheckpoint, err := ap.computeTargetCheckpoint(tx, baseState, baseStateBlockRoot, slot)
		if err != nil {
			log.Debug("Failed to compute target checkpoint - falling back to the cached one", "slot", slot, "err", err)
			targetCheckpoint = baseAttestationData.Target
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
		if err := transition.DefaultMachine.ProcessSlots(baseState, (epoch*ap.beaconCfg.SlotsPerEpoch)+1); err != nil {
			log.Warn("Failed to process slots", "slot", slot, "err", err)
			return solid.AttestationData{}, err
		}
		if err != nil {
			return solid.AttestationData{}, err
		}

	}

	targetCheckpoint, err := ap.computeTargetCheckpoint(tx, baseState, baseStateBlockRoot, slot)
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
