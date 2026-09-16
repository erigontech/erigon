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
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type BuildContextStore struct {
	mu             sync.RWMutex
	next           uint64
	contexts       map[uint64][]buildContext
	active         atomic.Pointer[activeBuildSlots]
	activeContexts atomic.Pointer[activeBuildContexts]
	retainedSlot   uint64
	hasRetained    bool
	now            func() uint64
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

const maxRetainedBuildContextsPerSlot = 16

func NewBuildContextStore() *BuildContextStore {
	store := &BuildContextStore{contexts: make(map[uint64][]buildContext), now: currentUnixTime}
	active := make(activeBuildSlots)
	store.active.Store(&active)
	activeContexts := make(activeBuildContexts)
	store.activeContexts.Store(&activeContexts)
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
		s.retainedSlot = slot
		s.hasRetained = true
	}
	contexts := s.contexts[slot]
	for i, context := range slices.Backward(contexts) {
		if sameRetainedBuildContext(context.params, params) {
			matched := context
			if i != len(contexts)-1 {
				contexts = append(slices.Delete(contexts, i, i+1), matched)
				s.contexts[slot] = contexts
			}
			return slot, matched.token
		}
	}
	s.next++
	contexts = append(contexts, buildContext{token: s.next, expiresAt: params.Timestamp, params: params.Copy()})
	if len(contexts) > maxRetainedBuildContextsPerSlot {
		contexts = slices.Clone(contexts[len(contexts)-maxRetainedBuildContextsPerSlot:])
	}
	s.contexts[slot] = contexts
	s.updateActiveSlots()
	return slot, s.next
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
	contexts := s.contexts[slot]
	for _, context := range slices.Backward(contexts) {
		if context.params.ParentHash == parentHash && buildContextActive(context, now) {
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

func (s *BuildContextStore) updateActiveSlots() {
	active := make(activeBuildSlots, len(s.contexts))
	activeContexts := make(activeBuildContexts)
	for slot, contexts := range s.contexts {
		for _, context := range contexts {
			activeContexts[activeBuildContext{slot: slot, token: context.token}] = context.expiresAt
			expiresAt, exists := active[slot]
			if !exists || expiresAt != 0 && (context.expiresAt == 0 || context.expiresAt > expiresAt) {
				active[slot] = context.expiresAt
			}
		}
	}
	s.active.Store(&active)
	s.activeContexts.Store(&activeContexts)
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
