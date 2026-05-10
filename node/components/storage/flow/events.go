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

// Package flow defines the event types and orchestrator for the snapshot
// distribution lifecycle. Events are plain structs dispatched on the framework
// event bus by reflected argument type.
//
// Event families:
//   - Inventory: InventoryLoaded, InventoryChanged, TrustPromoted
//   - Peer exchange: PeerManifestReceived, PeerDeparted, ManifestPublished
//   - Download / seed: DownloadRequested, DownloadComplete, DownloadFailed, FileSeeded
//   - Merge cycle: MergeStarted, MergeComplete, RetirementStarted, RetirementDone
//   - Execution: BlocksFlushed, SnapshotsReady
//
// See cocoon/pocs-and-proposals/app-components/snapshot-flow-design.md for the
// ordering and timing invariants each event family is expected to satisfy.
package flow

import (
	"sync"

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// InitialStateReadyChannel returns a channel that closes the first time
// InitialStateReady is published on bus. Subscribes via SubscribeOnce
// so the handler self-removes after firing.
//
// Used by production wiring (backend.go) to bridge the bus event into
// the staged-sync OtterSync gate (SnapshotsCfg.SetInitialStateReady) —
// staged sync waits on the channel; when storage signals
// minimal-set-ready the channel closes and stages 2-6 unblock.
//
// If the subscription itself fails (programmer error in the bus
// implementation), the returned channel is closed immediately so the
// caller doesn't block forever.
func InitialStateReadyChannel(bus event.BusSubscriber) <-chan struct{} {
	ch := make(chan struct{})
	var once sync.Once
	closeOnce := func() { once.Do(func() { close(ch) }) }
	if err := bus.SubscribeOnce(func(InitialStateReady) {
		closeOnce()
	}); err != nil {
		closeOnce()
	}
	return ch
}

// --- Inventory events ---

// InventoryLoaded fires once per Storage lifecycle, after the initial scan.
// Late subscribers must read the current state via inventory query methods;
// the event is not replayed.
type InventoryLoaded struct {
	Inventory *snapshot.Inventory
}

// InventoryChanged signals that inventory state has mutated. Subscribers that
// miss events due to lag converge by re-reading the inventory on the next
// relevant event.
type InventoryChanged struct {
	Added   []*snapshot.FileEntry
	Removed []string // file names
	// Generation is monotonically increasing per Inventory instance. Subscribers
	// detect missed events by comparing consecutive generations.
	Generation uint64
}

// TrustPromoted fires when a file moves up the trust ladder.
type TrustPromoted struct {
	FileName string
	OldTrust snapshot.TrustLevel
	NewTrust snapshot.TrustLevel
}

// --- Peer-exchange events ---

// PeerManifestReceived carries the full manifest state a peer currently
// advertises. Supersedes any prior manifest from the same peer.
//
// Domains carries every kind a domain advertises (kv, history, idx);
// inspect FileEntry.Kind to filter. Blocks holds top-level block .seg.
// Caplin, Meta, Salt are flat slices keyed by their respective Kind.
type PeerManifestReceived struct {
	PeerID  string
	Domains map[snapshot.Domain][]*snapshot.FileEntry
	Blocks  []*snapshot.FileEntry
	Caplin  []*snapshot.FileEntry
	Meta    []*snapshot.FileEntry
	Salt    []*snapshot.FileEntry
}

// PeerDeparted fires when a peer leaves; its manifest is no longer authoritative.
type PeerDeparted struct {
	PeerID string
}

// ManifestPublished fires after the local node updates its chain.toml / ENR
// advertisement.
type ManifestPublished struct {
	Generation uint64
}

// --- Download / seed events ---

// DownloadRequested is published by the orchestrator when it decides to fetch
// a file from a peer. Does not guarantee a subsequent DownloadComplete — the
// orchestrator reconciles via Inventory state.
type DownloadRequested struct {
	FileName  string
	InfoHash  [20]byte
	FromPeers []string
	Domain    snapshot.Domain    // empty for block snapshots
	Range     snapshot.StepRange // zero value for block snapshots
}

// DownloadComplete fires when a file has been fully downloaded and content
// hash verified against the torrent info-hash.
type DownloadComplete struct {
	FileName  string
	InfoHash  [20]byte
	LocalPath string
	Size      int64
}

// DownloadFailed fires when a download gives up or times out.
type DownloadFailed struct {
	FileName string
	Reason   string
}

// FileSeeded fires when a file has been registered with the local torrent
// client and is serving peers.
type FileSeeded struct {
	FileName string
	InfoHash [20]byte
}

// --- Merge-cycle events ---

// MergeStarted fires when the aggregator begins merging small step files into
// a larger merged file.
type MergeStarted struct {
	Domain snapshot.Domain
	Range  snapshot.StepRange
}

// MergeComplete fires when the merge finishes and the inventory has been
// atomically updated. Replaced files are logically gone from the inventory
// but may still be on disk briefly until cleanup.
type MergeComplete struct {
	Domain   snapshot.Domain
	Merged   *snapshot.FileEntry
	Replaced []string // file names
}

// RetirementStarted fires when block retirement (freezing executed blocks
// into snapshots) begins.
type RetirementStarted struct {
	FromBlock uint64
	ToBlock   uint64
}

// RetirementDone fires when block retirement finishes; NewFiles have been
// added to the inventory.
type RetirementDone struct {
	FromBlock uint64
	ToBlock   uint64
	NewFiles  []*snapshot.FileEntry
}

// --- Execution-side events ---

// BlocksFlushed carries the latest block number committed to the DB. Ordered:
// consecutive events from the same source carry monotonically non-decreasing
// LatestBlock. Out-of-order events are rejected by the orchestrator.
type BlocksFlushed struct {
	LatestBlock uint64
	FromBlock   uint64 // first block in the range flushed since the previous event
}

// SnapshotsReady is level-triggered: the ranges [FromBlock, ToBlock) are
// covered at the time of publication. Safe to receive multiple times.
type SnapshotsReady struct {
	FromBlock uint64
	ToBlock   uint64
}

// InitialStateReady signals that the state-domain phase of a peered sync
// has finished — every state file the orchestrator gap-filled for is now
// in the local inventory at TrustVerified. Execution can begin replaying
// forward from this point while the block-file phase continues to
// download in parallel.
//
// Fires at most once per orchestrator lifetime. If a peer's manifest
// requires no state-file downloads (the local inventory already
// satisfies the state coverage), the event still fires immediately so
// downstream consumers don't need a separate "is state already
// covered?" check.
type InitialStateReady struct {
	// StateDomains lists the domains the phase-1 gap-fill covered.
	// Consumers that care about which subset of state was filled can
	// read this; consumers that only care about "is the gate open?"
	// can ignore it.
	StateDomains []snapshot.Domain
}
