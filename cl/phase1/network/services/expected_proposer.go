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
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"golang.org/x/sync/singleflight"
)

// maxConcurrentSidecarProposerStateRebuilds bounds the parent states rebuilt at once for sidecar proposer checks, as
// maxConcurrentBlockValidationContexts does for blocks.
const maxConcurrentSidecarProposerStateRebuilds = 4

type expectedProposerKey struct {
	parentRoot common.Hash
	slot       uint64
}

// expectedProposers reads the expected proposer of a slot on the branch of a parent block. All sidecars of a block
// share its parent and slot, so each pair is read once.
type expectedProposers struct {
	ctx        context.Context
	cfg        *clparams.BeaconChainConfig
	forkChoice forkchoice.ForkChoiceStorage
	results    *lru.Cache[expectedProposerKey, uint64]
	reads      singleflight.Group
	rebuilds   chan struct{}
}

func newExpectedProposers(ctx context.Context, cfg *clparams.BeaconChainConfig, forkChoice forkchoice.ForkChoiceStorage) *expectedProposers {
	results, err := lru.New[expectedProposerKey, uint64]("sidecarExpectedProposers", 64)
	if err != nil {
		panic(err)
	}
	return &expectedProposers{
		ctx:        ctx,
		cfg:        cfg,
		forkChoice: forkChoice,
		results:    results,
		rebuilds:   make(chan struct{}, maxConcurrentSidecarProposerStateRebuilds),
	}
}

// get returns the expected proposer of slot on the branch of parentRoot, as the spec computes it: from the parent
// state advanced to slot. The parent state fork choice holds is read in place when it already gives the proposer.
// Any other parent state is rebuilt, a bounded number at a time; a read that waits longer than a slot for its turn,
// or whose parent state is gone, ignores the sidecar.
//
// The sidecars of a block share one read. It runs under the service context, not a caller's, so one canceled
// validation does not fail the others; each caller waits for it under its own context.
func (e *expectedProposers) get(ctx context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
	key := expectedProposerKey{parentRoot: parentRoot, slot: slot}
	if proposer, ok := e.results.Get(key); ok {
		return proposer, nil
	}
	read := e.reads.DoChan(string(parentRoot[:])+strconv.FormatUint(slot, 10), func() (any, error) {
		if proposer, ok := e.results.Get(key); ok {
			return proposer, nil
		}
		proposer, err := e.read(e.ctx, parentRoot, parentSlot, slot)
		if err != nil {
			return uint64(0), err
		}
		e.results.Add(key, proposer)
		return proposer, nil
	})
	select {
	case res := <-read:
		if res.Err != nil {
			return 0, res.Err
		}
		return res.Val.(uint64), nil
	case <-ctx.Done():
		return 0, fmt.Errorf("%w: expected proposer read canceled: %w", ErrIgnore, ctx.Err())
	}
}

func (e *expectedProposers) read(ctx context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
	epoch := slot / e.cfg.SlotsPerEpoch
	inPlace := parentStateReadableInPlace(e.cfg, parentSlot/e.cfg.SlotsPerEpoch, epoch)
	var proposer uint64
	found := false
	readInPlace := func(parentState *state.CachingBeaconState) error {
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
	}
	if inPlace {
		if err := e.forkChoice.ViewCurrentStateAtBlockRoot(parentRoot, readInPlace); err != nil {
			return 0, fmt.Errorf("%w: view parent state for the expected proposer: %w", ErrIgnore, err)
		}
		if found {
			return proposer, nil
		}
	}

	wait := time.NewTimer(time.Duration(e.cfg.SecondsPerSlot) * time.Second)
	defer wait.Stop()
	select {
	case e.rebuilds <- struct{}{}:
	case <-wait.C:
		return 0, fmt.Errorf("%w: no parent state rebuild for the expected proposer within a slot", ErrIgnore)
	case <-ctx.Done():
		return 0, fmt.Errorf("%w: expected proposer read stopped: %w", ErrIgnore, ctx.Err())
	}
	defer func() { <-e.rebuilds }()

	if inPlace {
		if err := e.forkChoice.ViewStateAtBlockRoot(parentRoot, readInPlace); err != nil {
			return 0, fmt.Errorf("%w: view parent state for the expected proposer: %w", ErrIgnore, err)
		}
		if found {
			return proposer, nil
		}
	}
	parentState, err := e.forkChoice.GetStateAtBlockRoot(parentRoot)
	if err != nil {
		return 0, fmt.Errorf("%w: get parent state for the expected proposer: %w", ErrIgnore, err)
	}
	if parentState == nil {
		return 0, fmt.Errorf("%w: parent state for the expected proposer not found", ErrIgnore)
	}
	if err := transition.DefaultMachine.ProcessSlots(parentState, slot); err != nil {
		return 0, fmt.Errorf("%w: advance parent state for the expected proposer: %w", ErrIgnore, err)
	}
	proposer, err = parentState.GetBeaconProposerIndexForSlot(slot)
	if err != nil {
		return 0, fmt.Errorf("%w: expected proposer of slot %d: %w", ErrIgnore, slot, err)
	}
	return proposer, nil
}
