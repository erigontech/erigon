// Copyright 2026 The Erigon Authors
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

package services

import (
	"fmt"
	"strconv"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
	"golang.org/x/sync/singleflight"
)

type parentStateViewer interface {
	ViewStateAtBlockRoot(blockRoot common.Hash, fn func(*state.CachingBeaconState) error) error
}

type expectedProposerKey struct {
	parentRoot common.Hash
	slot       uint64
}

// expectedProposers reads the expected proposer of a slot on the branch of a parent block. All sidecars of a block
// share its parent and slot, so each pair is read from the parent state once.
type expectedProposers struct {
	cfg     *clparams.BeaconChainConfig
	states  parentStateViewer
	results *lru.Cache[expectedProposerKey, uint64]
	reads   singleflight.Group
}

func newExpectedProposers(cfg *clparams.BeaconChainConfig, states parentStateViewer) *expectedProposers {
	results, err := lru.New[expectedProposerKey, uint64]("sidecarExpectedProposers", 64)
	if err != nil {
		panic(err)
	}
	return &expectedProposers{cfg: cfg, states: states, results: results}
}

// get returns the expected proposer of slot on the branch of parentRoot. It ignores the sidecar when the parent state
// does not give it without being advanced, as the spec allows while the proposers of the branch are not known yet.
func (e *expectedProposers) get(parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
	key := expectedProposerKey{parentRoot: parentRoot, slot: slot}
	if proposer, ok := e.results.Get(key); ok {
		return proposer, nil
	}
	epoch := slot / e.cfg.SlotsPerEpoch
	if !parentStateReadableInPlace(e.cfg, parentSlot/e.cfg.SlotsPerEpoch, epoch) {
		return 0, fmt.Errorf("%w: expected proposer of slot %d is not in the parent state", ErrIgnore, slot)
	}
	proposer, err, _ := e.reads.Do(string(parentRoot[:])+strconv.FormatUint(slot, 10), func() (any, error) {
		var proposer uint64
		found := false
		if err := e.states.ViewStateAtBlockRoot(parentRoot, func(parentState *state.CachingBeaconState) error {
			if state.Epoch(parentState) == epoch {
				var err error
				proposer, err = parentState.GetBeaconProposerIndexForSlot(slot)
				found = err == nil
				return err
			}
			if parentState.Version() == e.cfg.GetCurrentStateVersion(epoch) {
				proposer, found = parentState.ProposerIndexFromLookahead(slot)
			}
			return nil
		}); err != nil {
			return uint64(0), fmt.Errorf("%w: view parent state for the expected proposer: %w", ErrIgnore, err)
		}
		if !found {
			return uint64(0), fmt.Errorf("%w: expected proposer of slot %d is not in the parent state", ErrIgnore, slot)
		}
		e.results.Add(key, proposer)
		return proposer, nil
	})
	if err != nil {
		return 0, err
	}
	return proposer.(uint64), nil
}
