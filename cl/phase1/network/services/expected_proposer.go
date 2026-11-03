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
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"golang.org/x/sync/singleflight"
)

const (
	// maxConcurrentSidecarProposerStateRebuilds bounds the parent states rebuilt at once for sidecar proposer checks,
	// as maxConcurrentBlockValidationContexts does for blocks.
	maxConcurrentSidecarProposerStateRebuilds = 4
	// maxQueuedSidecarProposerStateRebuilds bounds the reads waiting for a rebuild. Every waiting read holds gossip
	// validations, so a read beyond the queue is ignored at once instead of adding to them.
	maxQueuedSidecarProposerStateRebuilds = 4
)

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
	// admitted holds a slot for every read that rebuilds a parent state or waits to, and rebuilds one for every read
	// that is rebuilding.
	admitted chan struct{}
	rebuilds chan struct{}
	// waiting counts the callers waiting for a shared read, followers of an admitted read included. Each holds a gossip
	// validation, so beyond maxWaiting a caller is ignored at once.
	waiting    atomic.Int64
	maxWaiting int64
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
		admitted:   make(chan struct{}, maxConcurrentSidecarProposerStateRebuilds+maxQueuedSidecarProposerStateRebuilds),
		rebuilds:   make(chan struct{}, maxConcurrentSidecarProposerStateRebuilds),
		// Room for every column sidecar of two blocks.
		maxWaiting: 2 * int64(cfg.NumberOfColumns),
	}
}

// get returns the expected proposer of slot on the branch of parentRoot, as the spec computes it: from the parent
// state advanced to slot. Each caller first reads the parent state fork choice holds, in place, when it gives the
// proposer; that read neither rebuilds nor waits. Any other parent state is rebuilt, a bounded number at a time, a
// bounded number of reads wait for their turn, and a bounded number of callers wait for those reads: a caller beyond
// either bound, a read that waits longer than a slot, or one whose parent state is gone ignores the sidecar.
//
// The sidecars of a block share one rebuild. It runs under the service context, not a caller's, so one canceled
// validation does not fail the others; each caller waits for it under its own context. The rebuild runs on its own
// goroutine, outside the gossip validator's recovery, where singleflight makes a panic fatal, so it recovers one
// itself and ignores the sidecar.
func (e *expectedProposers) get(ctx context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
	key := expectedProposerKey{parentRoot: parentRoot, slot: slot}
	if proposer, ok := e.results.Get(key); ok {
		return proposer, nil
	}
	// The parent state fork choice holds neither rebuilds nor waits, so each caller reads it before the waiter limit.
	if proposer, found, err := e.readHeld(parentRoot, parentSlot, slot); err != nil {
		return 0, err
	} else if found {
		e.results.Add(key, proposer)
		return proposer, nil
	}
	if e.waiting.Add(1) > e.maxWaiting {
		e.waiting.Add(-1)
		return 0, fmt.Errorf("%w: too many validations waiting for expected proposers", ErrIgnore)
	}
	defer e.waiting.Add(-1)
	read := e.reads.DoChan(string(parentRoot[:])+strconv.FormatUint(slot, 10), func() (result any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("[dataColumnSidecarService] panic reading the expected proposer", "parentRoot", parentRoot, "slot", slot, "err", r, "stack", dbg.Stack())
				result, err = uint64(0), fmt.Errorf("%w: expected proposer read panicked: %v", ErrIgnore, r)
			}
		}()
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

// readHeld reads the proposer of slot from the parent state fork choice holds, when that state gives it in place.
func (e *expectedProposers) readHeld(parentRoot common.Hash, parentSlot, slot uint64) (uint64, bool, error) {
	epoch := slot / e.cfg.SlotsPerEpoch
	if !parentStateReadableInPlace(e.cfg, parentSlot/e.cfg.SlotsPerEpoch, epoch) {
		return 0, false, nil
	}
	var proposer uint64
	found := false
	if err := e.forkChoice.ViewCurrentStateAtBlockRoot(parentRoot, func(parentState *state.CachingBeaconState) error {
		var err error
		proposer, found, err = e.proposerInPlace(parentState, epoch, slot)
		return err
	}); err != nil {
		return 0, false, fmt.Errorf("%w: view parent state for the expected proposer: %w", ErrIgnore, err)
	}
	return proposer, found, nil
}

// proposerInPlace reads the proposer of slot from parentState without advancing it: in the state's own epoch, or from
// Fulu on in a later epoch of its lookahead.
func (e *expectedProposers) proposerInPlace(parentState *state.CachingBeaconState, epoch, slot uint64) (uint64, bool, error) {
	if state.Epoch(parentState) == epoch {
		proposer, err := parentState.GetBeaconProposerIndexForSlot(slot)
		return proposer, err == nil, err
	}
	if parentState.Version() == e.cfg.GetCurrentStateVersion(epoch) {
		proposer, ok := parentState.ProposerIndexFromLookahead(slot)
		return proposer, ok, nil
	}
	return 0, false, nil
}

// read rebuilds the parent state to find the proposer of slot, once the parent state fork choice holds did not give it.
func (e *expectedProposers) read(ctx context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
	epoch := slot / e.cfg.SlotsPerEpoch
	inPlace := parentStateReadableInPlace(e.cfg, parentSlot/e.cfg.SlotsPerEpoch, epoch)
	select {
	case e.admitted <- struct{}{}:
	default:
		return 0, fmt.Errorf("%w: too many parent state rebuilds for expected proposers in flight", ErrIgnore)
	}
	defer func() { <-e.admitted }()
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
		var proposer uint64
		found := false
		if err := e.forkChoice.ViewStateAtBlockRoot(parentRoot, func(parentState *state.CachingBeaconState) error {
			var err error
			proposer, found, err = e.proposerInPlace(parentState, epoch, slot)
			return err
		}); err != nil {
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
	proposer, err := parentState.GetBeaconProposerIndexForSlot(slot)
	if err != nil {
		return 0, fmt.Errorf("%w: expected proposer of slot %d: %w", ErrIgnore, slot, err)
	}
	return proposer, nil
}
