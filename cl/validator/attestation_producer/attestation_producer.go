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
)

const attestationsCacheSize = 21

type attestationProducer struct {
	beaconCfg *clparams.BeaconChainConfig

	attestationsCache *lru.Cache[uint64, solid.AttestationData] // Epoch => Base AttestationData
	reqStateAtSlotCh  chan struct {
		slot      uint64
		baseState *state.CachingBeaconState
		resp      chan<- stateAcquireResp
	}
}

type stateAcquireResp struct {
	state *state.CachingBeaconState
	err   error
}

func New(ctx context.Context, beaconCfg *clparams.BeaconChainConfig) AttestationDataProducer {
	attestationsCache, err := lru.New[uint64, solid.AttestationData]("attestations", attestationsCacheSize)
	if err != nil {
		panic(err)
	}

	p := &attestationProducer{
		beaconCfg:         beaconCfg,
		attestationsCache: attestationsCache,
	}
	go p.genStateAtSlot(ctx)
	return p
}

// genStateAtSlot generates beacon state at slot in background and also notify all ch in waiting list. This is used to avoid over-copying of
// beacon state in case of multiple requests for the same slot. (e.g. 10k validators requesting attestation data for the same slot almost at the same time,
// which might lead to 10k copies of the same state in memory)
func (ap *attestationProducer) genStateAtSlot(ctx context.Context) {
	// beaconStateCache *lru.CacheWithTTL[uint64, *state.CachingBeaconState] // Slot => BeaconState
	beaconStateCache := lru.NewWithTTL[uint64, *state.CachingBeaconState]("attestaion_producer_beacon_state_cpy", 128, time.Minute)
	generatingWaitList := map[uint64][]chan<- stateAcquireResp{}
	slotGenerateDone := make(chan uint64, 32)
	for {
		select {
		case req := <-ap.reqStateAtSlotCh:
			slot := req.slot
			baseState := req.baseState
			if state, ok := beaconStateCache.Get(slot); ok {
				// if state is already generated, return it
				req.resp <- stateAcquireResp{state: state, err: nil}
				continue
			}
			if _, ok := generatingWaitList[slot]; ok {
				// append to wait list
				generatingWaitList[slot] = append(generatingWaitList[slot], req.resp)
				continue
			}
			generatingWaitList[slot] = []chan<- stateAcquireResp{req.resp}

			// generate state in background
			go func() {
				defer func() {
					// notify all waiting requests
					slotGenerateDone <- slot
				}()
				cpyBaseState, err := baseState.Copy()
				if err != nil {
					log.Warn("Failed to copy base state", "slot", slot, "err", err)
					return
				}
				if err := transition.DefaultMachine.ProcessSlots(cpyBaseState, slot); err != nil {
					log.Warn("Failed to process slots", "slot", slot, "err", err)
					return
				}
				beaconStateCache.Add(slot, cpyBaseState)
			}()
		case slot := <-slotGenerateDone:
			if reqChs, ok := generatingWaitList[slot]; ok {
				delete(generatingWaitList, slot)
				state, ok := beaconStateCache.Get(slot)
				if !ok {
					log.Warn("Failed to get generated state", "slot", slot)
					for _, ch := range reqChs {
						ch <- stateAcquireResp{state: nil, err: errors.New("failed to get generated state")}
					}
					continue
				}
				for _, ch := range reqChs {
					ch <- stateAcquireResp{state: state, err: nil}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (ap *attestationProducer) acquireBeaconStateAtSlot(baseState *state.CachingBeaconState, slot uint64) (*state.CachingBeaconState, error) {
	respCh := make(chan stateAcquireResp, 1)
	ap.reqStateAtSlotCh <- struct {
		slot      uint64
		baseState *state.CachingBeaconState
		resp      chan<- stateAcquireResp
	}{slot: slot, baseState: baseState, resp: respCh}

	select {
	case resp := <-respCh:
		return resp.state, resp.err
	case <-time.After(5 * time.Second):
		return nil, ErrHeadStateNotAvailable
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
		return solid.AttestationData{}, errors.New("head state slot is bigger than requested slot, the attestation should have been cached, try again later")
	}

	if stateEpoch < epoch {
		baseState, err = ap.acquireBeaconStateAtSlot(baseState, epoch*ap.beaconCfg.SlotsPerEpoch)
		if err != nil {
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
