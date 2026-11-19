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
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
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
	// maxConcurrentSidecarProposerStateRebuilds bounds fork-choice reads, which may rebuild parent states.
	maxConcurrentSidecarProposerStateRebuilds = 4
	// maxQueuedSidecarProposerStateRebuilds bounds the reads waiting for a rebuild. Every waiting read holds gossip
	// validations, so a read beyond the queue is ignored at once instead of adding to them.
	maxQueuedSidecarProposerStateRebuilds = 4
)

type expectedProposerKey struct {
	parentRoot common.Hash
	slot       uint64
}

// expectedProposers caches successful proposer reads by parent root and slot.
// Concurrent fork-choice reads for the same pair share work.
type expectedProposers struct {
	ctx        context.Context
	cfg        *clparams.BeaconChainConfig
	forkChoice forkchoice.ForkChoiceStorage
	head       synced_data.SyncedData
	results    *lru.Cache[expectedProposerKey, uint64]
	reads      singleflight.Group
	// admitted counts active and queued fork-choice reads; rebuilds counts active reads.
	admitted chan struct{}
	rebuilds chan struct{}
	// waiting counts the callers waiting for a shared read, followers of an admitted read included. Each holds a gossip
	// validation, so beyond maxWaiting a caller is ignored at once.
	waiting    atomic.Int64
	maxWaiting int64
}

func newExpectedProposers(ctx context.Context, cfg *clparams.BeaconChainConfig, forkChoice forkchoice.ForkChoiceStorage, head synced_data.SyncedData) *expectedProposers {
	results, err := lru.New[expectedProposerKey, uint64]("sidecarExpectedProposers", 64)
	if err != nil {
		panic(err)
	}
	return &expectedProposers{
		ctx:        ctx,
		cfg:        cfg,
		forkChoice: forkChoice,
		head:       head,
		results:    results,
		admitted:   make(chan struct{}, maxConcurrentSidecarProposerStateRebuilds+maxQueuedSidecarProposerStateRebuilds),
		rebuilds:   make(chan struct{}, maxConcurrentSidecarProposerStateRebuilds),
		// Room for every column sidecar of two blocks.
		maxWaiting: 2 * int64(cfg.NumberOfColumns),
	}
}

// get returns the expected proposer on the branch of parentRoot. It uses the matching head state in place
// when possible; otherwise fork choice may reuse or reconstruct the parent state.
// Every returned error wraps ErrIgnore.
// Shared reads use the service context, so canceling a caller only stops that caller's wait.
func (e *expectedProposers) get(ctx context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
	key := expectedProposerKey{parentRoot: parentRoot, slot: slot}
	if proposer, ok := e.results.Get(key); ok {
		return proposer, nil
	}
	// Head-state reads bypass the waiter limit because they do not wait for a shared read.
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
		// singleflight runs this on a separate goroutine, outside the gossip validator's panic recovery.
		defer func() {
			if r := recover(); r != nil {
				log.Error("[expectedProposers] panic reading the expected proposer", "parentRoot", parentRoot, "slot", slot, "err", r, "stack", dbg.Stack())
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

// readHeld tries the matching head state in place without taking the fork-choice lock held during block imports.
func (e *expectedProposers) readHeld(parentRoot common.Hash, parentSlot, slot uint64) (uint64, bool, error) {
	epoch := slot / e.cfg.SlotsPerEpoch
	if e.head == nil || !parentStateReadableInPlace(e.cfg, parentSlot/e.cfg.SlotsPerEpoch, epoch) {
		return 0, false, nil
	}
	var proposer uint64
	found := false
	err := e.head.ViewHeadStateWithIdentity(func(headState *state.CachingBeaconState, headRoot common.Hash, _ uint64) error {
		if headRoot != parentRoot {
			return nil
		}
		var err error
		proposer, found, err = e.proposerInPlace(headState, epoch, slot)
		return err
	})
	if errors.Is(err, synced_data.ErrNotSynced) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("%w: view head state for the expected proposer: %w", ErrIgnore, err)
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

// read gets the proposer through fork choice, rebuilding or advancing the parent state when needed.
func (e *expectedProposers) read(ctx context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
	epoch := slot / e.cfg.SlotsPerEpoch
	inPlace := parentStateReadableInPlace(e.cfg, parentSlot/e.cfg.SlotsPerEpoch, epoch)
	select {
	case e.admitted <- struct{}{}:
	default:
		return 0, fmt.Errorf("%w: too many parent state rebuilds for expected proposers in flight", ErrIgnore)
	}
	defer func() { <-e.admitted }()
	// The slot timeout applies only while waiting for a rebuild permit.
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
