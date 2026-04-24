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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// Storage is the narrow interface the Orchestrator needs from the storage
// layer. Production wires the real storage.Provider; tests wire MockStorage.
//
//   - Inventory exposes the current in-memory inventory for coverage and
//     membership queries. Callers must treat the returned value as read-only
//     with respect to orchestrator semantics — writes go through RecordFile
//     so storage can synchronise side effects (disk move, inventory bump,
//     notifications).
//   - RecordFile registers a newly downloaded file as locally present. The
//     implementation is responsible for making the file discoverable via
//     Inventory() on return.
type Storage interface {
	Inventory() *snapshot.Inventory
	RecordFile(*snapshot.FileEntry) error
}

// inventoryStorage adapts a bare *snapshot.Inventory to the Storage
// interface so the legacy New(bus, inv, logger) constructor keeps working
// without rippling through every caller.
type inventoryStorage struct{ inv *snapshot.Inventory }

func (s *inventoryStorage) Inventory() *snapshot.Inventory { return s.inv }

func (s *inventoryStorage) RecordFile(e *snapshot.FileEntry) error {
	s.inv.AddFile(e)
	return nil
}

// Orchestrator mediates between the Storage, the event bus, and the
// Downloader / Execution components. It subscribes to external signals
// (BlocksFlushed, PeerManifestReceived, DownloadComplete) and translates
// them into storage mutations or outbound event publications.
type Orchestrator struct {
	bus     event.EventBus
	storage Storage
	log     log.Logger

	mu               sync.Mutex
	started          bool
	lastFlushedBlock atomic.Uint64

	// peerFiles remembers peer-advertised entries by name so DownloadComplete
	// can reconstruct the file shape (domain, step range) without requiring
	// the download-side to echo it back. Keyed by file name.
	//
	// pending tracks in-flight downloads so coverage queries include them —
	// this is how merge-divergence handling avoids redundantly downloading
	// unmerged equivalents of a range already being served by a merged file.
	peerMu    sync.RWMutex
	peerFiles map[string]*snapshot.FileEntry
	pending   map[string]*snapshot.FileEntry

	// Handlers are materialised once so Subscribe and Unsubscribe see the
	// same reflect.Value.Pointer(). Re-referencing a method value
	// (o.onXxx) allocates a fresh closure whose pointer the event bus
	// cannot reliably match against the one it recorded at Subscribe time.
	hBlocksFlushed        func(BlocksFlushed)
	hPeerManifestReceived func(PeerManifestReceived)
	hDownloadComplete     func(DownloadComplete)
	hDownloadFailed       func(DownloadFailed)
	hPeerDeparted         func(PeerDeparted)
}

// New creates a flow orchestrator bound to the given bus and inventory.
// Neither is owned — the caller manages their lifecycles.
//
// Uses an adapter so the inventory is treated as the backing Storage; use
// NewWithStorage to plug in a richer storage implementation (e.g. MockStorage
// or the production storage.Provider).
func New(bus event.EventBus, inv *snapshot.Inventory, logger log.Logger) *Orchestrator {
	return NewWithStorage(bus, &inventoryStorage{inv: inv}, logger)
}

// NewWithStorage creates a flow orchestrator bound to the given bus and
// Storage. The orchestrator routes inventory reads through Storage.Inventory
// and newly downloaded files through Storage.RecordFile, so callers can
// intercept the "file landed" moment (e.g. to fire side effects or fail tests
// on unexpected files).
func NewWithStorage(bus event.EventBus, storage Storage, logger log.Logger) *Orchestrator {
	if logger == nil {
		logger = log.Root()
	}
	o := &Orchestrator{
		bus:       bus,
		storage:   storage,
		log:       logger,
		peerFiles: make(map[string]*snapshot.FileEntry),
		pending:   make(map[string]*snapshot.FileEntry),
	}
	o.hBlocksFlushed = o.onBlocksFlushed
	o.hPeerManifestReceived = o.onPeerManifestReceived
	o.hDownloadComplete = o.onDownloadComplete
	o.hDownloadFailed = o.onDownloadFailed
	o.hPeerDeparted = o.onPeerDeparted
	return o
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
		o.hBlocksFlushed,
		o.hPeerManifestReceived,
		o.hDownloadComplete,
		o.hDownloadFailed,
		o.hPeerDeparted,
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
	o.bus.Publish(InventoryLoaded{Inventory: o.storage.Inventory()})
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
		o.hBlocksFlushed,
		o.hPeerManifestReceived,
		o.hDownloadComplete,
		o.hDownloadFailed,
		o.hPeerDeparted,
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
// and emits DownloadRequested for any file whose (domain, role, range) is not
// already served by a local or pending download.
//
// Matching is role-scoped: different file roles under the same range (.kv
// vs .kvi) are non-interchangeable, so coverage is checked per role. Within
// a role, a larger merged file subsumes smaller unmerged files — this is how
// merge-divergent peer manifests are rationalised without duplicate work.
func (o *Orchestrator) requestGapsFor(domain snapshot.Domain, peerEntries []*snapshot.FileEntry, peerID string) {
	o.peerMu.Lock()
	for _, entry := range peerEntries {
		if _, seen := o.peerFiles[entry.Name]; !seen {
			o.peerFiles[entry.Name] = entry
		}
	}
	o.peerMu.Unlock()

	// First pass: decide which entries to request and mark them pending under
	// a single lock, so subsequent coverage checks in the same manifest see
	// their own earlier selections.
	toRequest := make([]*snapshot.FileEntry, 0, len(peerEntries))
	o.peerMu.Lock()
	for _, entry := range peerEntries {
		if o.haveLocally(domain, entry.Name) {
			continue
		}
		role := fileRole(entry.Name)
		if o.coverageForRoleLocked(domain, role).IsComplete(entry.FromStep, entry.ToStep) {
			continue
		}
		o.pending[entry.Name] = entry
		toRequest = append(toRequest, entry)
	}
	o.peerMu.Unlock()

	for _, entry := range toRequest {
		o.bus.Publish(DownloadRequested{
			FileName:  entry.Name,
			InfoHash:  entry.TorrentHash,
			FromPeers: []string{peerID},
			Domain:    domain,
			Range:     entry.Range(),
		})
	}
}

// onDownloadFailed removes the failed download from the pending map so a
// retry (from this or another peer) isn't silently suppressed by the
// role-coverage check.
func (o *Orchestrator) onDownloadFailed(e DownloadFailed) {
	o.peerMu.Lock()
	delete(o.pending, e.FileName)
	o.peerMu.Unlock()
	o.log.Warn("[flow] download failed", "file", e.FileName, "reason", e.Reason)
}

// fileRole extracts a role token that distinguishes non-interchangeable
// files at the same (domain, range). Rules, by filename shape:
//
//   - Domain files (e.g. "v1.0-accounts.0-256.kv"): the extension is
//     sufficient — "kv" and "kvi" are the only roles, and the range
//     precedes the extension as "<from>-<to>.<ext>".
//   - Block files (e.g. "v1.0-000000-000500-headers.seg"): the range and
//     the role both sit in dash-separated segments; "headers.seg" and
//     "bodies.seg" share extension but are distinct roles. So if the
//     segment immediately before the extension is alphabetic, it is part
//     of the role.
func fileRole(name string) string {
	extIdx := strings.LastIndexByte(name, '.')
	if extIdx < 0 {
		return name
	}
	ext := name[extIdx+1:]
	base := name[:extIdx]

	dashIdx := strings.LastIndexByte(base, '-')
	if dashIdx < 0 {
		return ext
	}
	lastSeg := base[dashIdx+1:]
	if isAlphabetic(lastSeg) {
		return lastSeg + "." + ext
	}
	return ext
}

func isAlphabetic(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			return false
		}
	}
	return true
}

// coverageForRoleLocked is the body of coverageForRole. The caller must hold
// peerMu (read or write) for the duration of the call. Used on the
// request-gaps hot path where peerMu is already held to batch pending
// inserts.
func (o *Orchestrator) coverageForRoleLocked(domain snapshot.Domain, role string) snapshot.StepRanges {
	inv := o.storage.Inventory()
	var entries []*snapshot.FileEntry
	if domain == "" {
		entries = inv.BlockFiles()
	} else {
		entries = inv.AllDomainFiles(domain)
	}
	var ranges snapshot.StepRanges
	for _, f := range entries {
		if f.Local && fileRole(f.Name) == role {
			ranges = append(ranges, f.Range())
		}
	}
	for _, p := range o.pending {
		if p.Domain == domain && fileRole(p.Name) == role {
			ranges = append(ranges, p.Range())
		}
	}
	return ranges.Normalize()
}

// PendingCount returns the number of in-flight downloads the orchestrator is
// currently tracking. Exposed for soak-style tests that need to observe
// whether the state bounded correctly across long scenarios.
func (o *Orchestrator) PendingCount() int {
	o.peerMu.RLock()
	defer o.peerMu.RUnlock()
	return len(o.pending)
}

// PeerFilesCount returns the number of distinct peer-advertised file names
// cached. Same observability role as PendingCount.
func (o *Orchestrator) PeerFilesCount() int {
	o.peerMu.RLock()
	defer o.peerMu.RUnlock()
	return len(o.peerFiles)
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
	inv := o.storage.Inventory()
	var entries []*snapshot.FileEntry
	if domain == "" {
		entries = inv.BlockFiles()
	} else {
		entries = inv.AllDomainFiles(domain)
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
	if err := o.storage.RecordFile(localEntry); err != nil {
		o.log.Warn("[flow] storage.RecordFile failed", "file", e.FileName, "err", err)
		// Leave pending as-is; a retry or a subsequent DownloadComplete
		// will re-attempt. Dropping pending here would let gap-fill re-
		// request the file immediately, amplifying a transient storage
		// error into redundant downloads.
		return
	}

	o.peerMu.Lock()
	delete(o.pending, e.FileName)
	o.peerMu.Unlock()

	o.bus.Publish(TrustPromoted{
		FileName: e.FileName,
		OldTrust: snapshot.TrustNone,
		NewTrust: snapshot.TrustVerified,
	})
}
