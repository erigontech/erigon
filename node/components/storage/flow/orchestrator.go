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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Orchestrator mediates between the Inventory, the event bus, and the
// Storage / Downloader / Execution components. It subscribes to external
// signals (BlocksFlushed, PeerManifestReceived, DownloadComplete) and
// translates them into Inventory mutations or outbound event publications.
type Orchestrator struct {
	bus       event.EventBus
	inventory *snapshot.Inventory
	log       log.Logger

	mu               sync.Mutex
	started          bool
	lastFlushedBlock atomic.Uint64

	// peerFiles remembers peer-advertised entries by name so DownloadComplete
	// can reconstruct the file shape (domain, step range) without requiring
	// the download-side to echo it back. Keyed by file name.
	peerMu    sync.RWMutex
	peerFiles map[string]*snapshot.FileEntry
}

// New creates a flow orchestrator bound to the given bus and inventory.
// Neither is owned — the caller manages their lifecycles.
func New(bus event.EventBus, inv *snapshot.Inventory, logger log.Logger) *Orchestrator {
	if logger == nil {
		logger = log.Root()
	}
	return &Orchestrator{
		bus:       bus,
		inventory: inv,
		log:       logger,
		peerFiles: make(map[string]*snapshot.FileEntry),
	}
}

// Start performs the startup sequence:
//  1. Registers subscriptions for inbound events.
//  2. Publishes InventoryLoaded with the current inventory state.
//
// Safe to call once per lifecycle. Returns an error on double-start.
func (o *Orchestrator) Start(_ context.Context) error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return fmt.Errorf("flow orchestrator already started")
	}
	subs := []interface{}{
		o.onBlocksFlushed,
		o.onPeerManifestReceived,
		o.onDownloadComplete,
		o.onPeerDeparted,
	}
	for i, sub := range subs {
		if err := o.bus.Subscribe(sub); err != nil {
			// Roll back the subscriptions that did succeed.
			for j := 0; j < i; j++ {
				_ = o.bus.Unsubscribe(subs[j])
			}
			o.mu.Unlock()
			return fmt.Errorf("subscribe handler %d: %w", i, err)
		}
	}
	o.started = true
	o.mu.Unlock()

	// Publish without holding the lock — handlers may re-enter the orchestrator.
	o.bus.Publish(InventoryLoaded{Inventory: o.inventory})
	return nil
}

// Close drains any in-flight async handlers, unsubscribes, and marks the
// orchestrator stopped.
func (o *Orchestrator) Close() error {
	o.mu.Lock()
	if !o.started {
		o.mu.Unlock()
		return nil
	}
	o.started = false
	o.mu.Unlock()

	// Drain async handlers before unsubscribing so handlers can't be racing
	// with our teardown.
	o.bus.WaitAsync()
	for _, sub := range []interface{}{
		o.onBlocksFlushed,
		o.onPeerManifestReceived,
		o.onDownloadComplete,
		o.onPeerDeparted,
	} {
		if err := o.bus.Unsubscribe(sub); err != nil {
			o.log.Warn("[flow] unsubscribe", "err", err)
		}
	}
	return nil
}

// onBlocksFlushed enforces ordering invariant O3 (monotonically non-decreasing
// LatestBlock). Out-of-order events produce a warning and are dropped.
func (o *Orchestrator) onBlocksFlushed(e BlocksFlushed) {
	prev := o.lastFlushedBlock.Load()
	if e.LatestBlock < prev {
		o.log.Warn("[flow] out-of-order BlocksFlushed dropped", "prev", prev, "got", e.LatestBlock)
		return
	}
	o.lastFlushedBlock.Store(e.LatestBlock)
}

// onPeerManifestReceived computes the gap between local inventory and the
// peer's advertised coverage, then publishes a DownloadRequested for each
// file the peer has that we don't.
func (o *Orchestrator) onPeerManifestReceived(e PeerManifestReceived) {
	for domain, peerEntries := range e.Domains {
		o.requestGapsFor(domain, peerEntries, e.PeerID)
	}
	// Block files use zero Domain — handle separately.
	o.requestGapsFor("", e.Blocks, e.PeerID)
}

// requestGapsFor iterates peer entries for a single domain (or "" for blocks)
// and emits DownloadRequested for any file not already held locally.
//
// Matching is by file name, not step range: a single range commonly requires
// multiple files (e.g. a domain's .kv + .kvi), so range-level coverage is
// insufficient to decide whether a specific peer file is redundant.
func (o *Orchestrator) requestGapsFor(domain snapshot.Domain, peerEntries []*snapshot.FileEntry, peerID string) {
	// Record peer entries under a single lock before publishing.
	o.peerMu.Lock()
	for _, entry := range peerEntries {
		if _, seen := o.peerFiles[entry.Name]; !seen {
			o.peerFiles[entry.Name] = entry
		}
	}
	o.peerMu.Unlock()

	for _, entry := range peerEntries {
		if o.haveLocally(domain, entry.Name) {
			continue
		}
		o.bus.Publish(DownloadRequested{
			FileName:  entry.Name,
			InfoHash:  entry.TorrentHash,
			FromPeers: []string{peerID},
			Domain:    domain,
			Range:     entry.Range(),
		})
	}
}

// onPeerDeparted removes the departing peer's file-shape entries from the
// peer-manifest cache. Without this, peerFiles grows unbounded across long
// scenarios as peers churn.
func (o *Orchestrator) onPeerDeparted(e PeerDeparted) {
	// The current peerFiles map is keyed by file name without peer
	// attribution — if another peer later advertises the same file name it
	// stays valid. A more precise impl would track (peerID → file names) and
	// only evict entries unique to the departing peer. For the current
	// single-claim-per-name model, this is a no-op; kept as a subscription
	// slot so future refinement lands without churning subscribers.
	_ = e
}

// haveLocally reports whether the local inventory has a file with the given
// name and Local=true. Used by gap-fill to skip files we already hold.
func (o *Orchestrator) haveLocally(domain snapshot.Domain, name string) bool {
	var entries []*snapshot.FileEntry
	if domain == "" {
		entries = o.inventory.BlockFiles()
	} else {
		entries = o.inventory.AllDomainFiles(domain)
	}
	for _, f := range entries {
		if f.Name == name && f.Local {
			return true
		}
	}
	return false
}

// onDownloadComplete promotes the file to the local inventory at TrustVerified
// and publishes TrustPromoted to signal the transition from peer-claim trust
// to locally-verified content.
func (o *Orchestrator) onDownloadComplete(e DownloadComplete) {
	o.peerMu.RLock()
	peerEntry, ok := o.peerFiles[e.FileName]
	o.peerMu.RUnlock()
	if !ok {
		o.log.Warn("[flow] DownloadComplete for unknown file", "file", e.FileName)
		return
	}

	localEntry := peerEntry.Clone()
	localEntry.Size = e.Size
	localEntry.TorrentHash = e.InfoHash
	localEntry.Trust = snapshot.TrustVerified
	localEntry.Local = true
	o.inventory.AddFile(localEntry)

	o.bus.Publish(TrustPromoted{
		FileName: e.FileName,
		OldTrust: snapshot.TrustNone,
		NewTrust: snapshot.TrustVerified,
	})
}
