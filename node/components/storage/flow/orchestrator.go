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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
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
//
// Optional fields (chain, snapDir) wire the validation phase. When
// chain is non-empty, RecordFile runs every validator before adding
// the file to the inventory. snapDir is the on-disk root the
// validator's ContentSource resolves against — only used when a
// validator in the chain actually reads bytes.
type inventoryStorage struct {
	inv     *snapshot.Inventory
	chain   validation.Chain
	snapDir string
}

func (s *inventoryStorage) Inventory() *snapshot.Inventory { return s.inv }

func (s *inventoryStorage) RecordFile(e *snapshot.FileEntry) error {
	if len(s.chain) > 0 {
		var content validation.ContentSource
		if s.snapDir != "" && e != nil && e.Name != "" {
			content = validation.FileContent{Path: filepath.Join(s.snapDir, e.Name)}
		}
		if err := s.chain.Validate(e, content); err != nil {
			return fmt.Errorf("validation: %w", err)
		}
	}
	s.inv.AddFile(e)
	return nil
}

// NewInventoryStorage returns a Storage that wraps inv with an
// optional validation chain. snapDir is where on-disk validators
// resolve ContentSource paths — pass "" if no chain validator reads
// bytes (stage-1 metadata-only validators don't).
//
// An empty chain produces a passthrough adapter equivalent to the
// pre-validation behaviour. NewWithStorage(bus, NewInventoryStorage(
// inv, nil, ""), logger) is identical to New(bus, inv, logger).
func NewInventoryStorage(inv *snapshot.Inventory, chain validation.Chain, snapDir string) Storage {
	return &inventoryStorage{inv: inv, chain: chain, snapDir: snapDir}
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
	// Each entry tracks the set of peer IDs currently advertising it. On
	// PeerDeparted we remove the peer from each entry's set; an entry whose
	// set becomes empty AND is not in `pending` is dropped. This is what
	// future UCAN peer-selection consumes (which trusted peers offer file
	// X) and what bounds the cache after long churn.
	//
	// pending tracks in-flight downloads so coverage queries include them —
	// this is how merge-divergence handling avoids redundantly downloading
	// unmerged equivalents of a range already being served by a merged file.
	peerMu    sync.RWMutex
	peerFiles map[string]*peerFileClaim
	pending   map[string]*snapshot.FileEntry

	// Phased scheduling: state-domain files are requested first; block
	// files are held in blocksQueued until the last state download
	// completes, at which point InitialStateReady fires and the queued
	// block requests are published. Execution can then replay blocks
	// forward while subsequent block transfers continue in parallel.
	//
	// statePending counts the state-domain downloads still in flight;
	// when it reaches zero after having been positive (or when we
	// initially observe a manifest with no state gap to fill),
	// InitialStateReady fires exactly once per orchestrator lifetime.
	//
	// stateDomainsSeen records which domains contributed to phase 1 so
	// the InitialStateReady payload can list them.
	statePending     int
	stateReadyFired  bool
	stateDomainsSeen map[snapshot.Domain]struct{}
	blocksQueued     []queuedBlock

	// Handlers are materialised once so Subscribe and Unsubscribe see the
	// same reflect.Value.Pointer(). Re-referencing a method value
	// (o.onXxx) allocates a fresh closure whose pointer the event bus
	// cannot reliably match against the one it recorded at Subscribe time.
	hBlocksFlushed        func(BlocksFlushed)
	hPeerManifestReceived func(PeerManifestReceived)
	hDownloadComplete     func(DownloadComplete)
	hDownloadFailed       func(DownloadFailed)
	hPeerDeparted         func(PeerDeparted)

	// trust gates DownloadRequested publication. Nil means trust-
	// everyone (default). Set via SetTrust before Start; mid-flight
	// reconfiguration is not supported.
	trust TrustFilter
}

// SetTrust attaches a TrustFilter that gates which peers the
// orchestrator will route DownloadRequested events to. Must be called
// before Start. Pass nil to clear (trust-everyone). A non-nil filter
// is consulted at the entry to every gap-fill request path; manifests
// from untrusted peers update peerFiles attribution but do not
// trigger downloads.
func (o *Orchestrator) SetTrust(t TrustFilter) error {
	if o == nil {
		return fmt.Errorf("flow.SetTrust: nil orchestrator")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return fmt.Errorf("flow.SetTrust: orchestrator already started")
	}
	o.trust = t
	return nil
}

// queuedBlock is a held-back block-file DownloadRequested that will be
// published after InitialStateReady fires.
type queuedBlock struct {
	entry  *snapshot.FileEntry
	peerID string
}

// TrustFilter gates which peers the orchestrator will route
// DownloadRequested events to. Production wires manifest_exchange's
// trust state (post-UCAN-verification); tests can use a stub. A nil
// filter on Orchestrator means trust-everyone — preserves the
// pre-UCAN behaviour.
//
// Trusted is consulted at gap-fill time. If a peer that earlier
// passed UCAN verification has since fallen out of trust (UCAN
// expired, or ReverifyOnReconnect cleared the cache on disconnect),
// the orchestrator skips publishing DownloadRequested even though
// the peer's prior manifest is still cached in peerFiles.
type TrustFilter interface {
	Trusted(peerID string) bool
}

// peerFileClaim is the orchestrator's per-file-name record of which
// peers currently advertise that file. The file shape (entry) is
// captured from the first advertiser; subsequent advertisers add to
// peers but never overwrite the shape — every honest advertiser of the
// same file name agrees on its (domain, range, infohash). When peers
// disagree, validation lives at a different layer (per
// feature-pluggable-validation-phase).
type peerFileClaim struct {
	entry *snapshot.FileEntry
	peers map[string]struct{}
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
		bus:              bus,
		storage:          storage,
		log:              logger,
		peerFiles:        make(map[string]*peerFileClaim),
		pending:          make(map[string]*snapshot.FileEntry),
		stateDomainsSeen: make(map[snapshot.Domain]struct{}),
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
//
// Phased scheduling: state-domain gaps publish immediately and increment
// statePending; block-file gaps are held in blocksQueued until the state
// phase completes (InitialStateReady fires). If the manifest produces no
// state gap at all — either we already have all the state, or the peer
// only advertises blocks — InitialStateReady fires immediately at the
// end of this call so the queue drains.
func (o *Orchestrator) onPeerManifestReceived(e PeerManifestReceived) {
	for domain, peerEntries := range e.Domains {
		o.requestGapsFor(domain, peerEntries, e.PeerID)
	}
	// Block files use zero Domain — handle separately.
	o.requestGapsFor("", e.Blocks, e.PeerID)

	// Non-ranged categories (caplin, meta, salt). Identified by
	// name+infohash; no coverage check. Same phase gate as blocks: held
	// behind InitialStateReady so state-domain bandwidth isn't contended.
	o.requestSimpleGaps(e.Caplin, e.PeerID)
	o.requestSimpleGaps(e.Meta, e.PeerID)
	o.requestSimpleGaps(e.Salt, e.PeerID)

	// If the state phase has nothing pending and hasn't fired yet, open
	// phase 2 now. Handles the all-local and blocks-only cases.
	o.peerMu.Lock()
	shouldFire := !o.stateReadyFired && o.statePending == 0
	o.peerMu.Unlock()
	if shouldFire {
		o.fireInitialStateReady()
	}
}

// requestSimpleGaps emits DownloadRequested for non-ranged peer entries
// (caplin .seg, meta, salt) that aren't already local. Bypasses
// coverage logic since these files don't carry step semantics. Honours
// the phase-2 gate the same way blocks do.
func (o *Orchestrator) requestSimpleGaps(peerEntries []*snapshot.FileEntry, peerID string) {
	if len(peerEntries) == 0 {
		return
	}

	// Always record peer-attribution (informational); skip downstream
	// only if the trust filter rejects the peer.
	o.peerMu.Lock()
	for _, entry := range peerEntries {
		o.recordPeerClaimLocked(entry, peerID)
	}
	o.peerMu.Unlock()

	if o.trust != nil && !o.trust.Trusted(peerID) {
		return
	}

	toRequest := make([]*snapshot.FileEntry, 0, len(peerEntries))
	o.peerMu.Lock()
	for _, entry := range peerEntries {
		if o.haveLocally("", entry.Name) {
			continue
		}
		if _, pending := o.pending[entry.Name]; pending {
			continue
		}
		o.pending[entry.Name] = entry
		toRequest = append(toRequest, entry)
	}
	holdForPhase2 := !o.stateReadyFired
	if holdForPhase2 {
		for _, entry := range toRequest {
			o.blocksQueued = append(o.blocksQueued, queuedBlock{entry: entry, peerID: peerID})
		}
	}
	o.peerMu.Unlock()

	if holdForPhase2 {
		return
	}

	for _, entry := range toRequest {
		o.bus.Publish(DownloadRequested{
			FileName:  entry.Name,
			InfoHash:  entry.TorrentHash,
			FromPeers: []string{peerID},
		})
	}
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
		o.recordPeerClaimLocked(entry, peerID)
	}
	o.peerMu.Unlock()

	if o.trust != nil && !o.trust.Trusted(peerID) {
		return
	}

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

	// Schedule later files first. For domain files, "later" means higher
	// ToStep (more recent state); for block files, FromStep / ToStep
	// carry block numbers, so the same ordering works.
	sort.SliceStable(toRequest, func(i, j int) bool {
		if toRequest[i].ToStep != toRequest[j].ToStep {
			return toRequest[i].ToStep > toRequest[j].ToStep
		}
		if toRequest[i].FromStep != toRequest[j].FromStep {
			return toRequest[i].FromStep > toRequest[j].FromStep
		}
		return toRequest[i].Name > toRequest[j].Name
	})

	// Phase routing. State-domain gaps publish now and count toward
	// statePending. Block-file gaps publish now only if phase 2 is
	// already open; otherwise they queue for release when the state
	// phase completes.
	isState := domain != ""
	o.peerMu.Lock()
	holdForPhase2 := !isState && !o.stateReadyFired
	if isState && len(toRequest) > 0 {
		o.statePending += len(toRequest)
		o.stateDomainsSeen[domain] = struct{}{}
	}
	if holdForPhase2 {
		for _, entry := range toRequest {
			o.blocksQueued = append(o.blocksQueued, queuedBlock{entry: entry, peerID: peerID})
		}
	}
	o.peerMu.Unlock()

	if holdForPhase2 {
		return
	}

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

// fireInitialStateReady publishes InitialStateReady exactly once and
// drains any block-file DownloadRequested events queued while the state
// phase was in flight. Safe to call concurrently — the state-ready flag
// serialises.
func (o *Orchestrator) fireInitialStateReady() {
	o.peerMu.Lock()
	if o.stateReadyFired {
		o.peerMu.Unlock()
		return
	}
	o.stateReadyFired = true
	domains := make([]snapshot.Domain, 0, len(o.stateDomainsSeen))
	for d := range o.stateDomainsSeen {
		domains = append(domains, d)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
	queue := o.blocksQueued
	o.blocksQueued = nil
	o.peerMu.Unlock()

	o.bus.Publish(InitialStateReady{StateDomains: domains})
	for _, qb := range queue {
		// Defensive re-check: a peer that was trusted at queue time
		// may have fallen out of trust by drain time (UCAN expired,
		// peer disconnected with ReverifyOnReconnect=true). Drop the
		// queued request and clear pending so a future trusted
		// advertiser of the same file is free to request it.
		if o.trust != nil && !o.trust.Trusted(qb.peerID) {
			o.peerMu.Lock()
			delete(o.pending, qb.entry.Name)
			o.peerMu.Unlock()
			continue
		}
		o.bus.Publish(DownloadRequested{
			FileName:  qb.entry.Name,
			InfoHash:  qb.entry.TorrentHash,
			FromPeers: []string{qb.peerID},
			// Blocks carry zero Domain.
			Range: qb.entry.Range(),
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

// recordPeerClaimLocked adds peerID's claim for entry. Caller must hold
// o.peerMu. The first claim wires up the file shape; subsequent claims
// only add to the peer set (entries are immutable here — the orchestrator
// trusts that two honest advertisers of the same name agree on the
// shape; mismatches are a validation-phase concern).
func (o *Orchestrator) recordPeerClaimLocked(entry *snapshot.FileEntry, peerID string) {
	claim, ok := o.peerFiles[entry.Name]
	if !ok {
		claim = &peerFileClaim{entry: entry, peers: map[string]struct{}{}}
		o.peerFiles[entry.Name] = claim
	}
	if peerID != "" {
		claim.peers[peerID] = struct{}{}
	}
}

// onPeerDeparted removes the departing peer from each peerFiles claim's
// advertiser set. A claim whose advertiser set drops to empty AND that
// has no in-flight download is evicted; entries with an in-flight
// download are kept so onDownloadComplete can still resolve the file
// shape when the bytes arrive.
func (o *Orchestrator) onPeerDeparted(e PeerDeparted) {
	if e.PeerID == "" {
		return
	}
	o.peerMu.Lock()
	defer o.peerMu.Unlock()
	for name, claim := range o.peerFiles {
		if _, had := claim.peers[e.PeerID]; !had {
			continue
		}
		delete(claim.peers, e.PeerID)
		if len(claim.peers) > 0 {
			continue
		}
		if _, inflight := o.pending[name]; inflight {
			continue
		}
		delete(o.peerFiles, name)
	}
}

// PeersOffering returns the set of peer IDs currently advertising the
// named file. Used by future UCAN peer-selection — given a trusted-peer
// filter, the orchestrator can pick a fetch source from the
// intersection of advertisers and trusted peers. Returns nil if no peer
// has advertised the file.
func (o *Orchestrator) PeersOffering(name string) []string {
	o.peerMu.RLock()
	defer o.peerMu.RUnlock()
	claim, ok := o.peerFiles[name]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(claim.peers))
	for id := range claim.peers {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// haveLocally reports whether the local inventory has a file with the given
// name and Local=true. Used by gap-fill to skip files we already hold.
//
// When domain is empty the caller may be looking for a block, caplin,
// meta, or salt file — they all live outside the per-domain map. Scan
// every flat slice rather than make the caller pre-classify by Kind.
func (o *Orchestrator) haveLocally(domain snapshot.Domain, name string) bool {
	inv := o.storage.Inventory()
	var entries []*snapshot.FileEntry
	if domain == "" {
		entries = append(entries, inv.BlockFiles()...)
		entries = append(entries, inv.CaplinFiles()...)
		entries = append(entries, inv.MetaFiles()...)
		entries = append(entries, inv.SaltFiles()...)
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
	claim, ok := o.peerFiles[e.FileName]
	o.peerMu.RUnlock()
	if !ok {
		o.log.Warn("[flow] DownloadComplete for unknown file", "file", e.FileName)
		return
	}
	peerEntry := claim.entry

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
	// Evict orphan claims: if every advertiser departed while this
	// download was in flight, the claim was retained on PeerDeparted
	// (because pending was set). Now that pending is clear and there
	// are no peers, drop it.
	if len(claim.peers) == 0 {
		delete(o.peerFiles, e.FileName)
	}
	// Decrement state-phase pending count for state-domain completions.
	// If this drains the state phase, fire InitialStateReady + release
	// the block queue (done after unlock via fireInitialStateReady).
	var shouldFireStateReady bool
	if peerEntry.Domain != "" {
		if o.statePending > 0 {
			o.statePending--
		}
		if o.statePending == 0 && !o.stateReadyFired {
			shouldFireStateReady = true
		}
	}
	o.peerMu.Unlock()

	o.bus.Publish(TrustPromoted{
		FileName: e.FileName,
		OldTrust: snapshot.TrustNone,
		NewTrust: snapshot.TrustVerified,
	})

	if shouldFireStateReady {
		o.fireInitialStateReady()
	}
}
