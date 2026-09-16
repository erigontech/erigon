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
	"slices"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/execution/types"
)

type BuildContextStore struct {
	mu             sync.RWMutex
	next           uint64
	contexts       map[uint64][]buildContext
	active         atomic.Pointer[activeBuildSlots]
	activeContexts atomic.Pointer[activeBuildContexts]
}

type activeBuildSlots map[uint64]struct{}
type activeBuildContext struct {
	slot  uint64
	token uint64
}
type activeBuildContexts map[activeBuildContext]struct{}

type buildContext struct {
	token  uint64
	params *Parameters
}

func NewBuildContextStore() *BuildContextStore {
	store := &BuildContextStore{contexts: make(map[uint64][]buildContext)}
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

func (s *BuildContextStore) Resolve(slot uint64) (*Parameters, uint64, bool) {
	if s == nil {
		return nil, 0, false
	}
	s.mu.RLock()
	contexts := s.contexts[slot]
	if len(contexts) == 0 {
		s.mu.RUnlock()
		return nil, 0, false
	}
	context := contexts[len(contexts)-1]
	s.mu.RUnlock()
	return context.params.Copy(), context.token, true
}

func (s *BuildContextStore) IsActive(slot uint64) bool {
	if s == nil {
		return false
	}
	active := s.active.Load()
	if active == nil {
		return false
	}
	_, ok := (*active)[slot]
	return ok
}

func (s *BuildContextStore) IsContextActive(slot, token uint64) bool {
	if s == nil || token == 0 {
		return false
	}
	active := s.activeContexts.Load()
	if active == nil {
		return false
	}
	_, ok := (*active)[activeBuildContext{slot: slot, token: token}]
	return ok
}

func (s *BuildContextStore) updateActiveSlots() {
	active := make(activeBuildSlots, len(s.contexts))
	activeContexts := make(activeBuildContexts)
	for slot, contexts := range s.contexts {
		if len(contexts) > 0 {
			active[slot] = struct{}{}
		}
		for _, context := range contexts {
			activeContexts[activeBuildContext{slot: slot, token: context.token}] = struct{}{}
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
	for _, context := range s.contexts[slot] {
		if context.token == token {
			return fn()
		}
	}
	return errors.New("build context is no longer active")
}

func (s *BuildContextStore) Wrap(next BlockBuilderFunc) BlockBuilderFunc {
	return func(ctx context.Context, params *Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		slot, token := s.publish(params)
		defer s.release(slot, token)
		if token == 0 {
			return next(ctx, params, interrupt)
		}
		buildParams := params.Copy()
		buildParams.privateBundleGeneration = token
		return next(ctx, buildParams, interrupt)
	}
}
