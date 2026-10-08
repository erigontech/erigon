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

package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestAwaitGloasPayloadSourceReturnsOnceDecided(t *testing.T) {
	calls := 0
	resolve := func() (executionPayloadSource, error) {
		calls++
		if calls < 3 {
			return executionPayloadSource{gloasPath: gloasPayloadPathPending}, nil
		}
		return executionPayloadSource{gloasPath: gloasPayloadPathFull}, nil
	}
	pending := executionPayloadSource{gloasPath: gloasPayloadPathPending}
	src := awaitGloasPayloadSource(context.Background(), time.Now().Add(time.Second), time.Millisecond, pending, resolve)
	require.Equal(t, gloasPayloadPathFull, src.gloasPath)
	require.Equal(t, 3, calls)
}

func TestAwaitGloasPayloadSourceGivesUpAtDeadline(t *testing.T) {
	resolve := func() (executionPayloadSource, error) {
		return executionPayloadSource{gloasPath: gloasPayloadPathPending}, nil
	}
	pending := executionPayloadSource{gloasPath: gloasPayloadPathPending}
	start := time.Now()
	// A poll interval far longer than the deadline: the cutoff itself must end the wait.
	src := awaitGloasPayloadSource(context.Background(), start.Add(20*time.Millisecond), time.Second, pending, resolve)
	require.Equal(t, gloasPayloadPathPending, src.gloasPath)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestAwaitGloasPayloadSourceKeepsLastSourceOnResolveError(t *testing.T) {
	last := executionPayloadSource{gloasPath: gloasPayloadPathPending, head: common.Hash{1}}
	resolve := func() (executionPayloadSource, error) {
		return executionPayloadSource{}, errors.New("head changed")
	}
	src := awaitGloasPayloadSource(context.Background(), time.Now().Add(time.Second), time.Millisecond, last, resolve)
	require.Equal(t, last, src)
}

func TestGloasPendingParentDeadline(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	slot := 12 * time.Second
	require.Equal(t, slotStart.Add(1500*time.Millisecond), gloasPendingParentDeadline(slotStart, slotStart, slot))
	require.Equal(t, slotStart.Add(1000*time.Millisecond), gloasPendingParentDeadline(slotStart.Add(-500*time.Millisecond), slotStart, slot))
	require.Equal(t, slotStart.Add(2*time.Second), gloasPendingParentDeadline(slotStart.Add(time.Second), slotStart, slot))
	// Past the cutoff the wait still allows one retry.
	require.Equal(t, slotStart.Add(3500*time.Millisecond), gloasPendingParentDeadline(slotStart.Add(3*time.Second), slotStart, slot))

	shortSlot := 6 * time.Second
	require.Equal(t, slotStart.Add(750*time.Millisecond), gloasPendingParentDeadline(slotStart, slotStart, shortSlot))
	require.Equal(t, slotStart.Add(time.Second), gloasPendingParentDeadline(slotStart.Add(500*time.Millisecond), slotStart, shortSlot))
}

func TestAwaitGloasPayloadSourceDoesNotWaitForABlockedResolve(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	resolve := func() (executionPayloadSource, error) {
		<-release
		return executionPayloadSource{gloasPath: gloasPayloadPathFull}, nil
	}
	last := executionPayloadSource{gloasPath: gloasPayloadPathAwaitingEnvelope}
	start := time.Now()
	src := awaitGloasPayloadSource(context.Background(), start.Add(20*time.Millisecond), time.Millisecond, last, resolve)
	require.Equal(t, last, src)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

// Preparation treats an EMPTY head with a parked envelope as EMPTY and primes that fallback;
// only production waits on it.
func TestAwaitingEnvelopePathIsUndecidedOnlyForProduction(t *testing.T) {
	require.True(t, gloasPayloadPathAwaitingEnvelope.undecided())
	require.True(t, gloasPayloadPathPending.undecided())
	require.False(t, gloasPayloadPathEmpty.undecided())
	require.False(t, gloasPathRequiresForkChoiceUpdate(gloasPayloadPathAwaitingEnvelope))
}
