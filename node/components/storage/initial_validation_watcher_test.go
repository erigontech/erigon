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

package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/lifecycle"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TestWatchInitialValidation pins the settle-watcher's gate: it holds
// flow.InitialValidationComplete until after the initial download set
// completes AND every Local file has settled to Advertisable.
func TestWatchInitialValidation(t *testing.T) {
	old := initialValidationPollInterval
	initialValidationPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { initialValidationPollInterval = old })

	bus := event.NewEventBus(nil)
	inv := snapshot.NewInventory()
	advertisable := "v1-000000-000500-headers.seg"
	settling := "v1-000500-001000-headers.seg"
	for _, n := range []string{advertisable, settling} {
		e := &snapshot.FileEntry{Name: n, Local: true}
		snapshot.PopulateFromName(e)
		require.NoError(t, inv.AddFile(e))
	}
	inv.AdvanceTo(advertisable, snapshot.LifecycleAdvertisable)
	// `settling` stays at LifecycleDownloaded — not yet validated.

	p := &Provider{
		eventBus:        bus,
		Inventory:       inv,
		LifecycleDriver: &lifecycle.Driver{Inv: inv},
		logger:          log.Root(),
	}

	var done int32
	require.NoError(t, bus.Subscribe(func(flow.InitialValidationComplete) {
		atomic.AddInt32(&done, 1)
	}))

	downloadsComplete := make(chan struct{})
	go p.watchInitialValidation(context.Background(), downloadsComplete, snapshotsync.RevalidationRedownload)

	// Not fired before the download set completes.
	time.Sleep(40 * time.Millisecond)
	require.Zero(t, atomic.LoadInt32(&done))

	close(downloadsComplete)

	// Still not fired: `settling` has not reached Advertisable.
	time.Sleep(60 * time.Millisecond)
	require.Zero(t, atomic.LoadInt32(&done), "must not fire while a file is still settling")

	// Settle the last file → the watcher fires InitialValidationComplete.
	inv.AdvanceTo(settling, snapshot.LifecycleAdvertisable)
	require.Eventually(t, func() bool { return atomic.LoadInt32(&done) == 1 },
		2*time.Second, 10*time.Millisecond, "InitialValidationComplete after all files settle")
}
