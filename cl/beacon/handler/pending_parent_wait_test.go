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
	src := awaitGloasPayloadSource(context.Background(), time.Now().Add(20*time.Millisecond), time.Millisecond, pending, resolve)
	require.Equal(t, gloasPayloadPathPending, src.gloasPath)
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
	require.Equal(t, slotStart.Add(3*time.Second), gloasPendingParentDeadline(slotStart.Add(3*time.Second), slotStart, slot))

	shortSlot := 6 * time.Second
	require.Equal(t, slotStart.Add(750*time.Millisecond), gloasPendingParentDeadline(slotStart, slotStart, shortSlot))
	require.Equal(t, slotStart.Add(time.Second), gloasPendingParentDeadline(slotStart.Add(500*time.Millisecond), slotStart, shortSlot))
}
