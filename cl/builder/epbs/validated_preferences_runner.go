// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type ValidatedPreferencesCoordinator interface {
	HandleValidatedPreferences(context.Context, *cltypes.SignedProposerPreferences) (*cltypes.SignedExecutionPayloadBid, error)
	PruneExpiredBeforeSlot(uint64) int
}

type SlotClock interface {
	GetCurrentSlot() uint64
}

type runnerTicker interface {
	Chan() <-chan time.Time
	Stop()
}

type systemRunnerTicker struct {
	*time.Ticker
}

func (t systemRunnerTicker) Chan() <-chan time.Time { return t.C }

func logValidatedPreferencesFailure(slot uint64, root common.Hash, err error) {
	log.Warn("Embedded builder proposer preferences attempt failed", "slot", slot, "dependentRoot", root, "err", err)
}

type preferencesKey struct {
	slot uint64
	root common.Hash
}

type preferencesAttempt struct {
	key             preferencesKey
	preferences     *cltypes.SignedProposerPreferences
	failureObserved bool
	cancel          context.CancelFunc
}

type pendingPreferences struct {
	preferences     *cltypes.SignedProposerPreferences
	failureObserved bool
}

type preferencesAttemptResult struct {
	key      preferencesKey
	bid      *cltypes.SignedExecutionPayloadBid
	err      error
	canceled bool
}

type ValidatedPreferencesRunner struct {
	coordinator    ValidatedPreferencesCoordinator
	clock          SlotClock
	maxPending     int
	retryInterval  time.Duration
	newTicker      func(time.Duration) runnerTicker
	observeFailure func(uint64, common.Hash, error)

	mu              sync.Mutex
	pending         map[preferencesKey]pendingPreferences
	blocked         map[preferencesKey]struct{}
	active          *preferencesAttempt
	retryCursor     preferencesKey
	haveRetryCursor bool
	observedSlot    uint64
	haveSlot        bool
	attemptPermit   bool
	started         bool
	stopping        bool
	wake            chan struct{}
	results         chan preferencesAttemptResult
}

var (
	errValidatedPreferencesRunnerStopped = errors.New("epbs/preferences runner: already run")
	errValidatedPreferencesAttemptNoBid  = errors.New("epbs/preferences runner: attempt returned no bid")
)

const minValidatedPreferencesRetryInterval = 100 * time.Millisecond

func NewValidatedPreferencesRunner(
	coordinator ValidatedPreferencesCoordinator,
	clock SlotClock,
	maxPending int,
	retryInterval time.Duration,
) (*ValidatedPreferencesRunner, error) {
	return newValidatedPreferencesRunner(coordinator, clock, maxPending, retryInterval, func(interval time.Duration) runnerTicker {
		return systemRunnerTicker{Ticker: time.NewTicker(interval)}
	})
}

func newValidatedPreferencesRunner(
	coordinator ValidatedPreferencesCoordinator,
	clock SlotClock,
	maxPending int,
	retryInterval time.Duration,
	newTicker func(time.Duration) runnerTicker,
) (*ValidatedPreferencesRunner, error) {
	if isNilDependency(coordinator) || isNilDependency(clock) || newTicker == nil {
		return nil, errors.New("epbs/preferences runner: missing dependency")
	}
	if maxPending <= 0 {
		return nil, errors.New("epbs/preferences runner: max pending must be positive")
	}
	if retryInterval < minValidatedPreferencesRetryInterval {
		return nil, errors.New("epbs/preferences runner: retry interval is too short")
	}
	return &ValidatedPreferencesRunner{
		coordinator:    coordinator,
		clock:          clock,
		maxPending:     maxPending,
		retryInterval:  retryInterval,
		newTicker:      newTicker,
		observeFailure: logValidatedPreferencesFailure,
		pending:        make(map[preferencesKey]pendingPreferences),
		blocked:        make(map[preferencesKey]struct{}),
		wake:           make(chan struct{}, 1),
		results:        make(chan preferencesAttemptResult, 1),
	}, nil
}

func (r *ValidatedPreferencesRunner) SubmitValidatedPreferences(preferences *cltypes.SignedProposerPreferences) {
	if r == nil || preferences == nil || preferences.Message == nil {
		return
	}
	r.mu.Lock()
	stopping := r.stopping
	r.mu.Unlock()
	if stopping {
		return
	}
	owned, ok := preferences.Clone().(*cltypes.SignedProposerPreferences)
	if !ok || owned == nil || owned.Message == nil {
		return
	}
	key := preferencesKey{slot: owned.Message.ProposalSlot, root: owned.Message.DependentRoot}
	currentSlot := r.clock.GetCurrentSlot()
	defer r.signal()

	var cancel context.CancelFunc
	r.mu.Lock()
	if r.stopping {
		r.mu.Unlock()
		return
	}
	_, cancel = r.observeSlotLocked(currentSlot)
	if key.slot < r.observedSlot {
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}
	if _, exists := r.pending[key]; exists {
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}
	if r.active != nil && r.active.key == key {
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}
	r.admitWaitingLocked(key, pendingPreferences{preferences: owned})
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *ValidatedPreferencesRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("epbs/preferences runner: nil context")
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errValidatedPreferencesRunnerStopped
	}
	r.started = true
	r.mu.Unlock()
	defer r.stopAndClear()
	if err := ctx.Err(); err != nil {
		return err
	}
	ticker := r.newTicker(r.retryInterval)
	if ticker == nil {
		return errors.New("epbs/preferences runner: ticker is nil")
	}
	defer ticker.Stop()
	r.mu.Lock()
	r.attemptPermit = true
	r.mu.Unlock()

	currentSlot, cancelAttempt := r.observeSlot(r.clock.GetCurrentSlot())
	if cancelAttempt != nil {
		cancelAttempt()
	}
	lastPrunedSlot := currentSlot
	r.coordinator.PruneExpiredBeforeSlot(currentSlot)
	r.startNext(ctx, currentSlot)
	for {
		select {
		case <-ctx.Done():
			r.beginStop()
			r.waitForActive()
			return ctx.Err()
		case <-r.wake:
			currentSlot, cancelAttempt = r.observeSlot(r.clock.GetCurrentSlot())
			if cancelAttempt != nil {
				cancelAttempt()
			}
			if currentSlot > lastPrunedSlot {
				r.coordinator.PruneExpiredBeforeSlot(currentSlot)
				lastPrunedSlot = currentSlot
			}
			r.startNext(ctx, currentSlot)
		case <-ticker.Chan():
			currentSlot, cancelAttempt = r.observeSlot(r.clock.GetCurrentSlot())
			if cancelAttempt != nil {
				cancelAttempt()
			}
			if currentSlot > lastPrunedSlot {
				r.coordinator.PruneExpiredBeforeSlot(currentSlot)
				lastPrunedSlot = currentSlot
			}
			r.replenishAttemptPermit()
			r.startNext(ctx, currentSlot)
		case result := <-r.results:
			currentSlot, cancelAttempt = r.observeSlot(r.clock.GetCurrentSlot())
			if cancelAttempt != nil {
				cancelAttempt()
			}
			if currentSlot > lastPrunedSlot {
				r.coordinator.PruneExpiredBeforeSlot(currentSlot)
				lastPrunedSlot = currentSlot
			}
			r.finish(result)
			r.startNext(ctx, currentSlot)
		}
	}
}

func (r *ValidatedPreferencesRunner) startNext(ctx context.Context, currentSlot uint64) {
	if ctx.Err() != nil {
		return
	}
	r.mu.Lock()
	if r.active != nil || !r.attemptPermit || r.stopping || ctx.Err() != nil {
		r.mu.Unlock()
		return
	}
	if r.haveSlot && r.observedSlot > currentSlot {
		currentSlot = r.observedSlot
	}
	key, pending, ok := r.nextEligibleLocked(currentSlot)
	if !ok {
		r.mu.Unlock()
		return
	}
	ownedAttempt, ok := pending.preferences.Clone().(*cltypes.SignedProposerPreferences)
	if !ok || ownedAttempt == nil || ownedAttempt.Message == nil {
		delete(r.pending, key)
		delete(r.blocked, key)
		r.mu.Unlock()
		return
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	if _, retry := r.blocked[key]; retry {
		r.retryCursor = key
		r.haveRetryCursor = true
	}
	delete(r.pending, key)
	delete(r.blocked, key)
	r.attemptPermit = false
	r.active = &preferencesAttempt{
		key: key, preferences: pending.preferences, failureObserved: pending.failureObserved, cancel: cancel,
	}
	r.mu.Unlock()

	go func() {
		bid, err := r.coordinator.HandleValidatedPreferences(attemptCtx, ownedAttempt)
		r.results <- preferencesAttemptResult{key: key, bid: bid, err: err, canceled: attemptCtx.Err() != nil}
	}()
}

func (r *ValidatedPreferencesRunner) nextEligibleLocked(currentSlot uint64) (preferencesKey, pendingPreferences, bool) {
	var earliestSlot uint64
	foundSlot := false
	for key := range r.pending {
		if !slotEligible(currentSlot, key.slot) || (foundSlot && key.slot >= earliestSlot) {
			continue
		}
		earliestSlot = key.slot
		foundSlot = true
	}
	if !foundSlot {
		return preferencesKey{}, pendingPreferences{}, false
	}
	var selected preferencesKey
	var preferences pendingPreferences
	var blockedSelected preferencesKey
	var blockedPreferences pendingPreferences
	var blockedAfterCursor preferencesKey
	var blockedAfterCursorPreferences pendingPreferences
	for key, candidate := range r.pending {
		if key.slot != earliestSlot {
			continue
		}
		if _, blocked := r.blocked[key]; blocked {
			if blockedPreferences.preferences == nil || preferencesKeyLess(key, blockedSelected) {
				blockedSelected = key
				blockedPreferences = candidate
			}
			if r.haveRetryCursor && r.retryCursor.slot == earliestSlot && preferencesKeyLess(r.retryCursor, key) &&
				(blockedAfterCursorPreferences.preferences == nil || preferencesKeyLess(key, blockedAfterCursor)) {
				blockedAfterCursor = key
				blockedAfterCursorPreferences = candidate
			}
			continue
		}
		if preferences.preferences == nil || preferencesKeyLess(key, selected) {
			selected = key
			preferences = candidate
		}
	}
	if preferences.preferences != nil {
		return selected, preferences, true
	}
	if blockedAfterCursorPreferences.preferences != nil {
		return blockedAfterCursor, blockedAfterCursorPreferences, true
	}
	return blockedSelected, blockedPreferences, blockedPreferences.preferences != nil
}

func (r *ValidatedPreferencesRunner) finish(result preferencesAttemptResult) {
	r.mu.Lock()
	if r.active == nil || r.active.key != result.key {
		r.mu.Unlock()
		return
	}
	attempt := r.active
	attempt.cancel()
	r.active = nil
	if r.stopping || result.key.slot < r.observedSlot {
		r.mu.Unlock()
		return
	}
	tracked := errors.Is(result.err, ErrAuctionAlreadyTracked)
	if tracked || result.bid != nil && result.err == nil {
		r.mu.Unlock()
		return
	}
	observeFailure := r.observeFailure
	firstFailure := !attempt.failureObserved && !result.canceled
	if firstFailure {
		attempt.failureObserved = true
	}
	if result.bid == nil {
		if r.admitRetryLocked(result.key, pendingPreferences{
			preferences: attempt.preferences, failureObserved: attempt.failureObserved,
		}) {
			r.blocked[result.key] = struct{}{}
		}
	}
	r.mu.Unlock()
	if firstFailure && observeFailure != nil {
		err := result.err
		if err == nil {
			err = errValidatedPreferencesAttemptNoBid
		}
		observeFailure(result.key.slot, result.key.root, err)
	}
}

func (r *ValidatedPreferencesRunner) observeSlot(slot uint64) (uint64, context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.observeSlotLocked(slot)
}

func (r *ValidatedPreferencesRunner) observeSlotLocked(slot uint64) (uint64, context.CancelFunc) {
	if r.haveSlot && slot < r.observedSlot {
		slot = r.observedSlot
	}
	advanced := !r.haveSlot || slot > r.observedSlot
	if advanced {
		r.observedSlot = slot
		r.haveSlot = true
		if r.haveRetryCursor && r.retryCursor.slot < slot {
			r.haveRetryCursor = false
		}
	}
	var cancel context.CancelFunc
	for key := range r.pending {
		if key.slot >= slot {
			continue
		}
		delete(r.pending, key)
		delete(r.blocked, key)
	}
	if r.active != nil && r.active.key.slot < slot {
		cancel = r.active.cancel
	}
	return slot, cancel
}

func (r *ValidatedPreferencesRunner) stopAndClear() {
	r.mu.Lock()
	r.stopping = true
	clear(r.pending)
	clear(r.blocked)
	r.haveRetryCursor = false
	r.attemptPermit = false
	r.mu.Unlock()
}

func (r *ValidatedPreferencesRunner) replenishAttemptPermit() {
	r.mu.Lock()
	r.attemptPermit = true
	r.mu.Unlock()
}

func (r *ValidatedPreferencesRunner) beginStop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopping = true
	clear(r.pending)
	clear(r.blocked)
	r.haveRetryCursor = false
	r.attemptPermit = false
	if r.active != nil {
		r.active.cancel()
	}
}

func (r *ValidatedPreferencesRunner) waitForActive() {
	for {
		r.mu.Lock()
		active := r.active != nil
		r.mu.Unlock()
		if !active {
			return
		}
		result := <-r.results
		r.mu.Lock()
		if r.active != nil && r.active.key == result.key {
			r.active.cancel()
			r.active = nil
		}
		r.mu.Unlock()
	}
}

func (r *ValidatedPreferencesRunner) admitWaitingLocked(key preferencesKey, preferences pendingPreferences) bool {
	if _, exists := r.pending[key]; exists {
		return false
	}
	if len(r.pending) < r.maxPending {
		r.pending[key] = preferences
		return true
	}
	if blocked, ok := r.farthestBlockedAtSlotLocked(key.slot); ok {
		delete(r.pending, blocked)
		delete(r.blocked, blocked)
		r.pending[key] = preferences
		return true
	}
	farthest, ok := r.farthestPendingLocked()
	if !ok || !preferencesKeyLess(key, farthest) {
		return false
	}
	delete(r.pending, farthest)
	delete(r.blocked, farthest)
	r.pending[key] = preferences
	return true
}

func (r *ValidatedPreferencesRunner) admitRetryLocked(key preferencesKey, preferences pendingPreferences) bool {
	if len(r.pending) < r.maxPending {
		r.pending[key] = preferences
		return true
	}
	farthest, ok := r.farthestPendingLocked()
	if !ok || key.slot >= farthest.slot {
		return false
	}
	delete(r.pending, farthest)
	delete(r.blocked, farthest)
	r.pending[key] = preferences
	return true
}

func (r *ValidatedPreferencesRunner) farthestBlockedAtSlotLocked(slot uint64) (preferencesKey, bool) {
	var farthest preferencesKey
	first := true
	for key := range r.blocked {
		if key.slot != slot {
			continue
		}
		if first || preferencesKeyLess(farthest, key) {
			farthest = key
			first = false
		}
	}
	return farthest, !first
}

func (r *ValidatedPreferencesRunner) farthestPendingLocked() (preferencesKey, bool) {
	var farthest preferencesKey
	first := true
	for key := range r.pending {
		if first || preferencesKeyLess(farthest, key) {
			farthest = key
			first = false
		}
	}
	return farthest, !first
}

func (r *ValidatedPreferencesRunner) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func preferencesKeyLess(left, right preferencesKey) bool {
	if left.slot != right.slot {
		return left.slot < right.slot
	}
	return bytes.Compare(left.root[:], right.root[:]) < 0
}

func slotEligible(currentSlot, targetSlot uint64) bool {
	return targetSlot == currentSlot || currentSlot != math.MaxUint64 && targetSlot == currentSlot+1
}
