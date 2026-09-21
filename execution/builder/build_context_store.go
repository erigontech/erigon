// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package builder

import (
	"context"
	"errors"
	"math"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type BuildContextStore struct {
	mu                sync.RWMutex
	next              uint64
	contexts          map[uint64][]buildContext
	active            atomic.Pointer[activeBuildSlots]
	activeContexts    atomic.Pointer[activeBuildContexts]
	current           atomic.Pointer[activeBuildContexts]
	selectable        map[uint64]uint64
	invalidated       map[uint64]struct{}
	retainedSlot      uint64
	hasRetained       bool
	admissionWindow   time.Duration
	rebindGeneration  func(uint64, common.Hash, uint64, uint64)
	pinnedGenerations func(uint64) map[uint64]struct{}
	now               func() uint64
}

type activeBuildSlots map[uint64]uint64
type activeBuildContext struct {
	slot  uint64
	token uint64
}
type activeBuildContexts map[activeBuildContext]uint64

type buildContext struct {
	token     uint64
	expiresAt uint64
	params    *Parameters
}

type PrivateOrderflowContextStatus struct {
	Accepting     bool
	ActiveSlot    uint64
	Generation    uint64
	ExpiresAt     uint64
	LastBuildSlot uint64
}

const maxRetainedBuildContextsPerSlot = 16

type BuildContextStoreOption func(*BuildContextStore)

func WithBuildContextAdmissionWindow(window time.Duration) BuildContextStoreOption {
	return func(store *BuildContextStore) {
		if window > 0 {
			store.admissionWindow = window
		}
	}
}

func WithBuildContextGenerationRebinder(rebind func(uint64, common.Hash, uint64, uint64)) BuildContextStoreOption {
	return func(store *BuildContextStore) {
		store.rebindGeneration = rebind
	}
}

// WithBuildContextPinnedGenerations preserves contexts that still own admitted private bundles.
func WithBuildContextPinnedGenerations(pinned func(uint64) map[uint64]struct{}) BuildContextStoreOption {
	return func(store *BuildContextStore) {
		store.pinnedGenerations = pinned
	}
}

func NewBuildContextStore(opts ...BuildContextStoreOption) *BuildContextStore {
	store := &BuildContextStore{
		contexts:    make(map[uint64][]buildContext),
		selectable:  make(map[uint64]uint64),
		invalidated: make(map[uint64]struct{}),
		now:         currentUnixTime,
	}
	active := make(activeBuildSlots)
	store.active.Store(&active)
	activeContexts := make(activeBuildContexts)
	store.activeContexts.Store(&activeContexts)
	current := make(activeBuildContexts)
	store.current.Store(&current)
	for _, opt := range opts {
		opt(store)
	}
	return store
}

func (s *BuildContextStore) publish(params *Parameters) (uint64, uint64) {
	if s == nil || params == nil || params.SlotNumber == nil || !params.ValidatedProposerContext {
		return 0, 0
	}
	slot := *params.SlotNumber
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	s.contexts[slot] = append(s.contexts[slot], buildContext{token: s.next, params: params.Copy()})
	s.selectable[slot] = s.next
	s.updateActiveSlots()
	return slot, s.next
}

func (s *BuildContextStore) release(slot, token uint64) {
	if s == nil || token == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	contexts := s.contexts[slot]
	for i := range contexts {
		if contexts[i].token != token {
			continue
		}
		contexts = slices.Delete(contexts, i, i+1)
		delete(s.invalidated, token)
		if s.selectable[slot] == token {
			delete(s.selectable, slot)
		}
		if len(contexts) > 0 {
			s.contexts[slot] = contexts
			s.updateActiveSlots()
			return
		}
		delete(s.contexts, slot)
		s.updateActiveSlots()
		return
	}
}

func (s *BuildContextStore) retain(params *Parameters) (uint64, uint64) {
	if s == nil || params == nil || params.SlotNumber == nil || params.Timestamp == 0 || !params.ValidatedProposerContext || params.TransientPayload || params.CustomTxnProvider != nil {
		return 0, 0
	}
	slot := *params.SlotNumber
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasRetained && slot < s.retainedSlot {
		return 0, 0
	}
	if !s.hasRetained || slot > s.retainedSlot {
		s.contexts = make(map[uint64][]buildContext)
		s.selectable = make(map[uint64]uint64)
		s.invalidated = make(map[uint64]struct{})
		s.retainedSlot = slot
		s.hasRetained = true
	}
	contexts := s.contexts[slot]
	var replacedToken uint64
	for i, context := range slices.Backward(contexts) {
		if sameRetainedBuildContext(context.params, params) {
			if _, invalidated := s.invalidated[context.token]; invalidated {
				replacedToken = context.token
				break
			}
			matched := context
			if i != len(contexts)-1 {
				contexts = append(slices.Delete(contexts, i, i+1), matched)
				s.contexts[slot] = contexts
			}
			s.selectable[slot] = matched.token
			s.updateActiveSlots()
			return slot, matched.token
		}
	}
	s.next++
	contexts = append(contexts, buildContext{
		token: s.next, expiresAt: buildContextExpiry(params.Timestamp, s.currentTime(), s.admissionWindow), params: params.Copy(),
	})
	if replacedToken != 0 && s.rebindGeneration != nil {
		s.rebindGeneration(slot, params.ParentHash, replacedToken, s.next)
	}
	contexts = s.trimContexts(slot, s.next, contexts)
	s.contexts[slot] = contexts
	s.selectable[slot] = s.next
	s.updateActiveSlots()
	return slot, s.next
}

func (s *BuildContextStore) trimContexts(slot, preserveToken uint64, contexts []buildContext) []buildContext {
	excess := len(contexts) - maxRetainedBuildContextsPerSlot
	if excess <= 0 {
		return contexts
	}
	var pinned map[uint64]struct{}
	if s.pinnedGenerations != nil {
		pinned = s.pinnedGenerations(slot)
	}
	kept := make([]buildContext, 0, len(contexts)-excess)
	for _, context := range contexts {
		_, isPinned := pinned[context.token]
		isPinned = isPinned || context.token == preserveToken
		if excess > 0 && !isPinned {
			delete(s.invalidated, context.token)
			excess--
			continue
		}
		kept = append(kept, context)
	}
	return kept
}

func buildContextExpiry(timestamp, now uint64, window time.Duration) uint64 {
	if window <= 0 {
		return timestamp
	}
	seconds := uint64(window / time.Second)
	if window%time.Second != 0 {
		seconds++
	}
	seconds++
	base := max(timestamp, now)
	if math.MaxUint64-base < seconds {
		return math.MaxUint64
	}
	return base + seconds
}

func sameRetainedBuildContext(left, right *Parameters) bool {
	if left == nil || right == nil {
		return left == right
	}
	left = left.Copy()
	right = right.Copy()
	left.PayloadId = 0
	right.PayloadId = 0
	left.privateBundleGeneration = 0
	right.privateBundleGeneration = 0
	left.CustomTxnProvider = nil
	right.CustomTxnProvider = nil
	return reflect.DeepEqual(left, right)
}

func (s *BuildContextStore) Resolve(slot uint64) (*Parameters, uint64, bool) {
	if s == nil {
		return nil, 0, false
	}
	s.mu.RLock()
	now := s.currentTime()
	contexts := s.contexts[slot]
	for _, context := range slices.Backward(contexts) {
		if buildContextActive(context, now) {
			s.mu.RUnlock()
			return context.params.Copy(), context.token, true
		}
	}
	s.mu.RUnlock()
	return nil, 0, false
}

func (s *BuildContextStore) ResolveForParent(slot uint64, parentHash common.Hash) (*Parameters, uint64, bool) {
	if s == nil {
		return nil, 0, false
	}
	s.mu.RLock()
	now := s.currentTime()
	currentToken := s.selectable[slot]
	contexts := s.contexts[slot]
	for _, context := range slices.Backward(contexts) {
		if context.token == currentToken && context.params.ParentHash == parentHash && buildContextActive(context, now) {
			s.mu.RUnlock()
			return context.params.Copy(), context.token, true
		}
	}
	s.mu.RUnlock()
	return nil, 0, false
}

func (s *BuildContextStore) IsActive(slot uint64) bool {
	if s == nil {
		return false
	}
	active := s.active.Load()
	if active == nil {
		return false
	}
	expiresAt, ok := (*active)[slot]
	if !ok {
		return false
	}
	return expiresAt == 0 || s.currentTime() < expiresAt
}

func (s *BuildContextStore) IsContextActive(slot, token uint64) bool {
	if s == nil || token == 0 {
		return false
	}
	active := s.activeContexts.Load()
	if active == nil {
		return false
	}
	expiresAt, ok := (*active)[activeBuildContext{slot: slot, token: token}]
	if !ok {
		return false
	}
	return expiresAt == 0 || s.currentTime() < expiresAt
}

// IsContextCurrent reports whether a generation is selectable for new admission.
func (s *BuildContextStore) IsContextCurrent(slot, token uint64) bool {
	if s == nil || token == 0 {
		return false
	}
	current := s.current.Load()
	if current == nil {
		return false
	}
	expiresAt, ok := (*current)[activeBuildContext{slot: slot, token: token}]
	return ok && (expiresAt == 0 || s.currentTime() < expiresAt)
}

func (s *BuildContextStore) PrivateOrderflowStatus() PrivateOrderflowContextStatus {
	if s == nil {
		return PrivateOrderflowContextStatus{}
	}
	now := s.currentTime()
	s.mu.RLock()
	defer s.mu.RUnlock()
	current := s.current.Load()
	status := PrivateOrderflowContextStatus{}
	if current != nil {
		for key, expiresAt := range *current {
			if expiresAt != 0 && now >= expiresAt {
				continue
			}
			if !status.Accepting || key.slot > status.ActiveSlot || key.slot == status.ActiveSlot && key.token > status.Generation {
				status.Accepting = true
				status.ActiveSlot = key.slot
				status.Generation = key.token
				status.ExpiresAt = expiresAt
			}
		}
	}
	if s.hasRetained {
		status.LastBuildSlot = s.retainedSlot
	}
	return status
}

func (s *BuildContextStore) updateActiveSlots() {
	active := make(activeBuildSlots, len(s.contexts))
	activeContexts := make(activeBuildContexts)
	current := make(activeBuildContexts)
	for slot, contexts := range s.contexts {
		for _, context := range contexts {
			activeContexts[activeBuildContext{slot: slot, token: context.token}] = context.expiresAt
			expiresAt, exists := active[slot]
			if !exists || expiresAt != 0 && (context.expiresAt == 0 || context.expiresAt > expiresAt) {
				active[slot] = context.expiresAt
			}
		}
		if token := s.selectable[slot]; token != 0 {
			for _, context := range contexts {
				if context.token == token {
					current[activeBuildContext{slot: slot, token: token}] = context.expiresAt
					break
				}
			}
		}
	}
	s.active.Store(&active)
	s.activeContexts.Store(&activeContexts)
	s.current.Store(&current)
}

func (s *BuildContextStore) WithActive(slot, token uint64, fn func() error) error {
	if s == nil || token == 0 || fn == nil {
		return errors.New("build context is unavailable")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.currentTime()
	for _, context := range s.contexts[slot] {
		if context.token == token && buildContextActive(context, now) {
			return fn()
		}
	}
	return errors.New("build context is no longer active")
}

// WithCurrent runs fn while the selected generation cannot be replaced or invalidated.
func (s *BuildContextStore) WithCurrent(slot, token uint64, fn func() error) error {
	if s == nil || token == 0 || fn == nil {
		return errors.New("build context is unavailable")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.selectable[slot] != token {
		return errors.New("build context is no longer current")
	}
	now := s.currentTime()
	for _, context := range s.contexts[slot] {
		if context.token == token && buildContextActive(context, now) {
			return fn()
		}
	}
	return errors.New("build context is no longer active")
}

// Invalidate clears new admission for an exact rejected build context.
func (s *BuildContextStore) Invalidate(params *Parameters) {
	if s == nil || params == nil || params.SlotNumber == nil {
		return
	}
	slot := *params.SlotNumber
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, context := range slices.Backward(s.contexts[slot]) {
		if sameRetainedBuildContext(context.params, params) {
			s.invalidated[context.token] = struct{}{}
			if s.selectable[slot] == context.token {
				delete(s.selectable, slot)
			}
			s.updateActiveSlots()
			return
		}
	}
}

func buildContextActive(context buildContext, now uint64) bool {
	return context.expiresAt == 0 || now < context.expiresAt
}

func currentUnixTime() uint64 {
	now := time.Now().Unix()
	if now < 0 {
		return 0
	}
	return uint64(now)
}

func (s *BuildContextStore) currentTime() uint64 {
	if s.now == nil {
		return currentUnixTime()
	}
	return s.now()
}

// Prepare attaches the retained private-bundle generation to accepted build parameters.
func (s *BuildContextStore) Prepare(params *Parameters) *Parameters {
	_, token := s.retain(params)
	if token == 0 {
		return params
	}
	prepared := params.Copy()
	prepared.privateBundleGeneration = token
	return prepared
}

func (s *BuildContextStore) Wrap(next BlockBuilderFunc) BlockBuilderFunc {
	return func(ctx context.Context, params *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		return next(ctx, s.Prepare(params), interrupt)
	}
}
