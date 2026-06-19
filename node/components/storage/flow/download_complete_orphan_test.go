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

package flow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// These tests pin the "external-download recovery" path added to fix
// the soak v17 iter-4 1k-stub Inventory orphan bug. Setup:
//
//   - SyncSnapshots.ReconcilePreverifiedAgainstDisk requests every
//     preverified item for download regardless of orchestrator
//     coverage dedup.
//   - The orchestrator's requestGapsFor subsumes 1k stubs whose range
//     is covered by a wider merged sibling already scheduled or local.
//     Those subsumed entries are NOT added to peerFiles.
//   - When the downloader finishes a subsumed 1k stub, onDownloadComplete
//     fires with a FileName the orchestrator has no peerFiles entry for.
//
// Pre-fix the orchestrator logged "DownloadComplete for unknown file"
// and dropped the event — the file landed on disk but Inventory never
// tracked it. Provider.unwindSnapshotsPastBlock's trim path then
// missed the file, and the defense-in-depth verifier refused the
// next Mode-B unwind at depths past where the stubs live.

// TestDownloadComplete_UnknownFile_RegistersInInventory pins the fix:
// when DownloadComplete arrives for a file with no peerFiles entry,
// the orchestrator must STILL register the file in Inventory (via
// Storage.RecordFile) so the trim path catches it. The phase-1 gate
// state stays untouched — these files were never part of the
// InitialStateReady accounting.
func TestDownloadComplete_UnknownFile_RegistersInInventory(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	// No PeerManifestReceived published — the 1k stub was never
	// announced to the orchestrator. The downloader independently
	// requested it via ReconcilePreverifiedAgainstDisk; this models
	// the "DownloadComplete fires for an unknown file" race.
	orphan := "v1.1-002990-002991-headers.seg"
	bus.Publish(DownloadComplete{
		FileName: orphan,
		Size:     642396,
	})

	waitUntil(t, func() bool {
		return len(storage.snapshotRecorded()) == 1
	}, 2*time.Second, "external-download RecordFile call")

	got := storage.snapshotRecorded()
	require.Len(t, got, 1)
	require.Equal(t, orphan, got[0].Name)
	require.True(t, got[0].Local,
		"externally-downloaded file MUST land in Inventory with Local=true so the trim path catches it")
	require.Equal(t, snapshot.TrustVerified, got[0].Trust,
		"a file that landed on disk via the downloader is bytes-on-disk = TrustVerified by construction (the downloader's torrent-hash gate already validated it)")
	// PopulateFromName must have parsed the block range so the trim
	// path's range-based queries work; without this the entry's
	// FromBlock/ToBlock stay zero and Provider.collectFilesPastBlock
	// can't classify it as "past toBlock".
	require.Equal(t, uint64(2_990_000), got[0].FromBlock,
		"PopulateFromName must parse FromBlock so collectFilesPastBlock can classify the entry")
	require.Equal(t, uint64(2_991_000), got[0].ToBlock,
		"PopulateFromName must parse ToBlock so collectFilesPastBlock can classify the entry")
}

// TestDownloadComplete_UnknownFile_DoesNotAffectPhase1 pins that the
// fix doesn't accidentally pull subsumed downloads into the
// InitialStateReady gate. PendingCount must remain zero (the entry
// was never in peerFiles → never tracked as pending) and the gate
// state is undisturbed.
func TestDownloadComplete_UnknownFile_DoesNotAffectPhase1(t *testing.T) {
	bus := newBusForTest()
	storage := &recordingStorage{inv: snapshot.NewInventory()}
	o := NewWithStorage(bus, storage, logger())

	require.NoError(t, o.Start(context.Background()))
	t.Cleanup(func() { _ = o.Close() })

	require.Equal(t, 0, o.PendingCount(), "no peerFiles → no pending")

	bus.Publish(DownloadComplete{
		FileName: "v1.1-002991-002992-bodies.seg",
		Size:     321198,
	})

	waitUntil(t, func() bool {
		return len(storage.snapshotRecorded()) == 1
	}, 2*time.Second, "external-download RecordFile call")

	require.Equal(t, 0, o.PendingCount(),
		"recording an external download must NOT increment pending counts; this file was never part of the orchestrator's gating set")
}
