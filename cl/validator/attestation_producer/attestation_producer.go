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
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

var (
	ErrHeadStateNotAvailable = errors.New("head state not available")
	ErrHeadStateBehind       = errors.New("head state is behind")
)

const attestationsCacheSize = 21

type attestationCacheKey struct {
	epoch    uint64
	headRoot common.Hash
}

type blockRootCacheKey struct {
	slot     uint64
	headRoot common.Hash
}

type attestationProducer struct {
	beaconCfg *clparams.BeaconChainConfig

	attCacheMutex              sync.RWMutex
	attestationsCache          *lru.CacheWithTTL[attestationCacheKey, solid.AttestationData]
	blockRootsUsedForSlotCache *lru.Cache[blockRootCacheKey, common.Hash]
}

func New(beaconCfg *clparams.BeaconChainConfig) AttestationDataProducer {
	ttl := time.Duration(beaconCfg.SecondsPerSlot) * time.Second / 2
	attestationsCache := lru.NewWithTTL[attestationCacheKey, solid.AttestationData]("attestations", attestationsCacheSize, ttl)
	blockRootsUsedForSlotCache, err := lru.New[blockRootCacheKey, common.Hash]("blockRootsUsedForSlot", attestationsCacheSize)
	if err != nil {
		panic(err)
	}
	p := &attestationProducer{
		beaconCfg:                  beaconCfg,
		attestationsCache:          attestationsCache,
		blockRootsUsedForSlotCache: blockRootsUsedForSlotCache,
	}
	return p
}

func (ap *attestationProducer) beaconBlockRootForSlot(baseState *state.CachingBeaconState, baseBlockRoot common.Hash, slot uint64) (common.Hash, error) {
	key := blockRootCacheKey{slot: slot, headRoot: baseBlockRoot}
	if blockRoot, ok := ap.blockRootsUsedForSlotCache.Get(key); ok {
		return blockRoot, nil
	}
	if baseState.Slot() > slot {
		blockRoot, err := baseState.GetBlockRootAtSlot(slot)
		if err != nil {
			return common.Hash{}, fmt.Errorf("failed to get block root at slot %d: %w", slot, err)
		}
		ap.blockRootsUsedForSlotCache.Add(key, blockRoot)
		return blockRoot, nil
	}
	ap.blockRootsUsedForSlotCache.Add(key, baseBlockRoot)
	return baseBlockRoot, nil
}

func (ap *attestationProducer) computeTargetCheckpoint(baseState *state.CachingBeaconState, baseStateBlockRoot common.Hash, slot uint64) (solid.Checkpoint, error) {
	var err error
	targetEpoch := slot / ap.beaconCfg.SlotsPerEpoch
	epochStartTargetSlot := targetEpoch * ap.beaconCfg.SlotsPerEpoch
	var targetRoot common.Hash
	if epochStartTargetSlot >= baseState.Slot() {
		targetRoot = baseStateBlockRoot
	} else {

		targetRoot, err = baseState.GetBlockRootAtSlot(epochStartTargetSlot)
		if err != nil {
			return solid.Checkpoint{}, fmt.Errorf("failed to get targetRoot at slot %d: %w", epochStartTargetSlot, err)
		}

		if targetRoot == (common.Hash{}) {
			// if the target root is not found, we can't generate the attestation
			return solid.Checkpoint{}, ErrHeadStateBehind
		}
	}
	return solid.Checkpoint{
		Root:  targetRoot,
		Epoch: targetEpoch,
	}, nil
}

func (ap *attestationProducer) CachedAttestationData(slot uint64, headRoot common.Hash) (solid.AttestationData, bool, error) {
	epoch := slot / ap.beaconCfg.SlotsPerEpoch
	ap.attCacheMutex.RLock()
	defer ap.attCacheMutex.RUnlock()
	if baseAttestationData, ok := ap.attestationsCache.Get(attestationCacheKey{epoch: epoch, headRoot: headRoot}); ok {
		beaconBlockRoot, ok := ap.blockRootsUsedForSlotCache.Get(blockRootCacheKey{slot: slot, headRoot: headRoot})
		if !ok {
			return solid.AttestationData{}, false, nil
		}
		return solid.AttestationData{
			Slot:            slot,
			BeaconBlockRoot: beaconBlockRoot,
			Source:          baseAttestationData.Source,
			Target:          baseAttestationData.Target,
		}, true, nil
	}
	return solid.AttestationData{}, false, nil
}

func (ap *attestationProducer) ProduceAndCacheAttestationData(_ kv.Tx, baseState *state.CachingBeaconState, baseStateBlockRoot common.Hash, slot uint64) (solid.AttestationData, error) {
	epoch := slot / ap.beaconCfg.SlotsPerEpoch
	key := attestationCacheKey{epoch: epoch, headRoot: baseStateBlockRoot}
	var err error
	ap.attCacheMutex.RLock()
	if baseAttestationData, ok := ap.attestationsCache.Get(key); ok {
		ap.attCacheMutex.RUnlock()
		beaconBlockRoot, err := ap.beaconBlockRootForSlot(baseState, baseStateBlockRoot, slot)
		if err != nil {
			return solid.AttestationData{}, err
		}
		return solid.AttestationData{
			Slot:            slot,
			BeaconBlockRoot: beaconBlockRoot,
			Source:          baseAttestationData.Source,
			Target:          baseAttestationData.Target,
		}, nil
	}
	ap.attCacheMutex.RUnlock()

	// in case the target epoch is not found, let's generate it with lock to avoid everyone trying to generate it
	// at the same time, which would be a waste of memory resources
	ap.attCacheMutex.Lock()
	defer ap.attCacheMutex.Unlock()

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
		if err := transition.DefaultMachine.ProcessSlots(baseState, epoch*ap.beaconCfg.SlotsPerEpoch); err != nil {
			log.Warn("Failed to process slots", "slot", slot, "err", err)
			return solid.AttestationData{}, err
		}
	}

	targetCheckpoint, err := ap.computeTargetCheckpoint(baseState, baseStateBlockRoot, slot)
	if err != nil {
		return solid.AttestationData{}, err
	}
	baseAttestationData := solid.AttestationData{
		Slot:            0,             // slot will be filled in later
		CommitteeIndex:  0,             // committee index is deprecated after Electra
		BeaconBlockRoot: common.Hash{}, // beacon block root will be filled in later
		Source:          baseState.CurrentJustifiedCheckpoint(),
		Target:          targetCheckpoint,
	}
	ap.attestationsCache.Add(key, baseAttestationData)
	ap.blockRootsUsedForSlotCache.Add(blockRootCacheKey{slot: slot, headRoot: baseStateBlockRoot}, baseStateBlockRoot)

	return solid.AttestationData{
		Slot:            slot,
		BeaconBlockRoot: baseStateBlockRoot,
		Source:          baseAttestationData.Source,
		Target:          targetCheckpoint,
	}, nil
}
