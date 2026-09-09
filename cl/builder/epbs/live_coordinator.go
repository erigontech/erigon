// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/cltypes"
)

type SlotInputResolver interface {
	Resolve(context.Context, *cltypes.SignedProposerPreferences) (SlotInput, error)
}

type SlotInputFreshness interface {
	ValidateCurrent(context.Context, SlotInput) error
}

type LiveCoordinator struct {
	coordinator *Coordinator
	resolver    SlotInputResolver
	freshness   SlotInputFreshness
}

func NewLiveCoordinator(coordinator *Coordinator, resolver SlotInputResolver, freshness SlotInputFreshness) *LiveCoordinator {
	return &LiveCoordinator{coordinator: coordinator, resolver: resolver, freshness: freshness}
}

func (c *LiveCoordinator) PruneExpiredBeforeSlot(slot uint64) int {
	if c == nil || c.coordinator == nil {
		return 0
	}
	return c.coordinator.PruneExpiredBeforeSlot(slot)
}

func (c *LiveCoordinator) HandleValidatedPreferences(
	ctx context.Context,
	preferences *cltypes.SignedProposerPreferences,
) (*cltypes.SignedExecutionPayloadBid, error) {
	if ctx == nil {
		return nil, errors.New("epbs/live coordinator: nil context")
	}
	if c == nil || c.coordinator == nil || isNilDependency(c.resolver) || isNilDependency(c.freshness) {
		return nil, errors.New("epbs/live coordinator: missing dependency")
	}
	if preferences == nil || preferences.Message == nil {
		return nil, errors.New("epbs/live coordinator: missing validated proposer preferences")
	}
	input, err := c.resolver.Resolve(ctx, preferences)
	if err != nil {
		return nil, fmt.Errorf("epbs/live coordinator: resolve slot input: %w", err)
	}
	if err := validateSlotInputFreshness(ctx, input, c.freshness); err != nil {
		return nil, err
	}
	return c.coordinator.runSlotGuarded(ctx, input, c.freshness)
}
