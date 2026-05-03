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

package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestQuarantine_StopsRetryingAfterThreshold(t *testing.T) {
	inv := snapshot.NewInventory()
	inv.AddFile(&snapshot.FileEntry{
		Name: "stuck.kv", Domain: snapshot.DomainAccounts, Local: true,
	})

	wantErr := errors.New("simulated persistent failure")
	var calls atomic.Int32
	d := &Driver{
		Inv:                 inv,
		QuarantineThreshold: 3,
		OnIndexing: func(_ context.Context, _ *snapshot.FileEntry) error {
			calls.Add(1)
			return wantErr
		},
	}

	// 5 sweeps. Threshold=3, so after the third sweep the file is
	// quarantined and the handler should not be called again.
	for i := 0; i < 5; i++ {
		d.Sweep(context.Background(), nil)
	}

	require.Equal(t, int32(3), calls.Load(),
		"handler must be called exactly threshold (3) times, then quarantine kicks in")
}

func TestQuarantine_ChangeSetClears(t *testing.T) {
	inv := snapshot.NewInventory()
	inv.AddFile(&snapshot.FileEntry{
		Name: "stuck.kv", Domain: snapshot.DomainAccounts, Local: true,
	})

	wantErr := errors.New("simulated failure")
	var calls atomic.Int32
	d := &Driver{
		Inv:                 inv,
		QuarantineThreshold: 2,
		OnIndexing: func(_ context.Context, _ *snapshot.FileEntry) error {
			calls.Add(1)
			return wantErr
		},
	}

	// Trip the quarantine.
	d.Sweep(context.Background(), nil)
	d.Sweep(context.Background(), nil)
	require.Equal(t, int32(2), calls.Load())

	// Quarantined — next sweep is a no-op.
	d.Sweep(context.Background(), nil)
	require.Equal(t, int32(2), calls.Load(),
		"sweep after quarantine must skip the file")

	// Simulate a ChangeSet for the file → clears quarantine.
	d.clearFailure("stuck.kv")

	// Sweep again — handler runs, failure counter starts fresh.
	d.Sweep(context.Background(), nil)
	require.Equal(t, int32(3), calls.Load(),
		"clearing quarantine must restore dispatch")
}

func TestQuarantine_SuccessfulDispatchClearsCounter(t *testing.T) {
	inv := snapshot.NewInventory()
	inv.AddFile(&snapshot.FileEntry{
		Name: "ok.kv", Domain: snapshot.DomainAccounts, Local: true,
	})

	var calls atomic.Int32
	var failNext atomic.Bool
	failNext.Store(true) // first call fails, subsequent succeed
	d := &Driver{
		Inv:                 inv,
		QuarantineThreshold: 3,
		OnIndexing: func(_ context.Context, _ *snapshot.FileEntry) error {
			calls.Add(1)
			if failNext.Swap(false) {
				return errors.New("transient")
			}
			return nil
		},
	}

	d.Sweep(context.Background(), nil) // fails (counter=1)
	d.Sweep(context.Background(), nil) // succeeds (counter cleared)
	d.Sweep(context.Background(), nil) // succeeds (no counter to clear)

	require.Equal(t, int32(3), calls.Load(),
		"successful dispatch must clear the counter so transient failures don't accumulate")

	// Verify counter is actually clear by tripping a fresh quarantine.
	failNext.Store(true)
	failOnceErr := func(_ context.Context, _ *snapshot.FileEntry) error {
		calls.Add(1)
		return errors.New("failing again")
	}
	d.OnIndexing = failOnceErr
	for i := 0; i < 3; i++ {
		d.Sweep(context.Background(), nil)
	}
	require.Equal(t, int32(6), calls.Load(),
		"counter was clear; quarantine threshold (3) must apply fresh")
}
