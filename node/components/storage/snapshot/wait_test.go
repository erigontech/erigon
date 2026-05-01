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

package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/views"
)

func TestWaitForReady_LocalReturnsImmediately(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, Local: true})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := inv.WaitForReady(ctx, "a.kv", false)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 20*time.Millisecond)
}

func TestWaitForReady_NotDeclaredReturnsErrNotFound(t *testing.T) {
	inv := NewInventory()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := inv.WaitForReady(ctx, "missing.kv", false)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNotFound))
}

func TestWaitForReady_PendingResolvesOnMarkLocal(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, Local: false})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Schedule MarkLocal at t=80ms via a one-off goroutine.
	go func() {
		time.Sleep(80 * time.Millisecond)
		inv.MarkLocal("a.kv")
	}()

	start := time.Now()
	err := inv.WaitForReady(ctx, "a.kv", false)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, 70*time.Millisecond,
		"must wait until MarkLocal fires; got %v", elapsed)
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestWaitForReady_CtxExpiresReturnsErrPending(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, Local: false})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := inv.WaitForReady(ctx, "a.kv", false)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, views.ErrPending))
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
}

func TestWaitForReady_RequireAdvertisableGatesOnPromotion(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, Local: true, Advertisable: false})

	// Without the gate: returns immediately (Local=true is enough).
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, inv.WaitForReady(ctx, "a.kv", false))

	// With the gate: pending until MarkAdvertisable.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel2()
	err := inv.WaitForReady(ctx2, "a.kv", true)
	require.True(t, errors.Is(err, views.ErrPending),
		"requireAdvertisable=true must wait for the flag")

	// Promote, expect resolution.
	go func() {
		time.Sleep(40 * time.Millisecond)
		inv.MarkAdvertisable("a.kv")
	}()
	ctx3, cancel3 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel3()
	require.NoError(t, inv.WaitForReady(ctx3, "a.kv", true))
}

func TestWaitForReady_UndeclaredDuringWaitContinuesUntilCtx(t *testing.T) {
	inv := NewInventory()
	inv.AddFile(&FileEntry{Name: "a.kv", Domain: DomainAccounts, Local: false})

	go func() {
		time.Sleep(30 * time.Millisecond)
		inv.RemoveFile("a.kv")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := inv.WaitForReady(ctx, "a.kv", false)
	elapsed := time.Since(start)

	// Per the contract, mid-wait removal does NOT short-circuit to ErrNotFound;
	// we wait until ctx decides.
	require.True(t, errors.Is(err, views.ErrPending),
		"undeclared mid-wait must continue until ctx; got %v", err)
	require.GreaterOrEqual(t, elapsed, 70*time.Millisecond)
}
