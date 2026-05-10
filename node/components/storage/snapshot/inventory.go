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
	"fmt"
	"sort"
	"sync"
	"time"
)

// Domain identifies a state domain (accounts, storage, code, commitment, etc).
type Domain string

const (
	DomainAccounts   Domain = "accounts"
	DomainStorage    Domain = "storage"
	DomainCode       Domain = "code"
	DomainCommitment Domain = "commitment"
	DomainReceipt    Domain = "receipt"
)

// FileKind identifies what role a file plays in the snapshot archive. Empty
// kind means a domain primary (.kv) when Domain is set, or a block primary
// (.seg) when Domain is empty — preserves back-compat with callers that
// don't set Kind.
type FileKind string

const (
	KindKV      FileKind = ""        // domain primary (.kv) or block primary (.seg)
	KindHistory FileKind = "history" // domain history primary (.v)
	KindIdx     FileKind = "idx"     // domain inverted-index primary (.ef)
	KindCaplin  FileKind = "caplin"  // caplin beacon archive (.seg)
	KindMeta    FileKind = "meta"    // chain config (erigondb.toml)
	KindSalt    FileKind = "salt"    // hash-derivation salt (salt-*.txt)
)

// FileEntry represents a single snapshot file (block, domain primary,
// domain history/idx, caplin, meta, or salt) tracked by the inventory.
type FileEntry struct {
	// Domain this file belongs to. Empty for block, caplin, meta, salt files.
	Domain Domain

	// Kind tags the file's role. See FileKind for the categorisation. The
	// zero value (empty string) means a primary file — .kv when Domain is
	// set, .seg when Domain is empty.
	Kind FileKind

	// Step range [FromStep, ToStep) covered by this file, in step
	// units. Populated for state files (kv, kvi, v, ef, efi) directly
	// from the name. Zero for block files, caplin, meta, salt — block
	// files use FromBlock/ToBlock instead, and only acquire a step
	// range once a commitment-derived (step, block) binding maps
	// their block range into step units.
	FromStep uint64
	ToStep   uint64

	// Block range [FromBlock, ToBlock) covered by this file, in block
	// units. Populated for block files (headers.seg, bodies.seg,
	// transactions.seg + their indexes) directly from the name. Zero
	// for state, caplin, meta, salt files.
	//
	// Block range is the secondary indexing axis for block files —
	// distinct from FromStep/ToStep which only carry step units.
	// Caplin files will gain a similar FromSlot/ToSlot pair when the
	// caplin storage path is wired through the lifecycle.
	FromBlock uint64
	ToBlock   uint64

	// File name (relative to snapshots directory; may include subdirs).
	Name string

	// Size in bytes on disk. Zero if the file is not local (peer-advertised only).
	Size int64

	// TorrentHash is the 20-byte BitTorrent info-hash. Zero if not yet computed.
	TorrentHash [20]byte

	// Trust indicates how this file's integrity has been established.
	Trust TrustLevel

	// Local is true if the file exists on this node's disk and is available for
	// reading by the aggregator/blockReader. A file can have Trust=TrustVerified
	// but Local=false (e.g. a preverified entry not yet downloaded).
	Local bool

	// Seeding is true if the file is currently being seeded via BitTorrent.
	Seeding bool

	// Advertisable is true once the producer-side validation chain has
	// approved this file for inclusion in the published V2 manifest.
	// The field is set but not yet read by GenerateV2 — the
	// chain.v2.<seq>.toml output still derives from Local + canonicity
	// today. Once the producer-side gate is fully hooked into the
	// retire/build pipeline, GenerateV2 will tighten its filter to
	// require Advertisable=true and the manifest's "advertised content
	// == validated content" invariant becomes a structural guarantee
	// rather than a procedural one.
	//
	// Per docs/plans/20260501-storage-lifecycle-spec.md, Advertisable
	// becomes the single Ready gate for both internal reads and
	// external publication. Local stays as a derived "on disk"
	// accessor; downloader/seeding still need to know what bytes have
	// been fetched, regardless of whether they're indexed/validated yet.
	Advertisable bool

	// State is the file's position in the import lifecycle. Driven by
	// the storage component's lifecycle driver (per
	// docs/plans/20260501-storage-lifecycle-spec.md). Zero value is
	// LifecycleDeclared — a freshly-AddFile'd entry without explicit
	// state defaults there. AddFile derives State from existing Local +
	// Advertisable for back-compat with callers that still set those
	// flags directly.
	State LifecycleState

	// Dependencies names files that must also be Local for this entry
	// to advance to LifecycleIndexed. Set explicitly by the caller of
	// AddFile based on the file's kind:
	//   - .kv (domain primary)  → ["<prefix>.kvi"]
	//   - .seg (block primary)  → ["<prefix>.idx"]
	//   - .v   (history primary)→ ["<prefix>.ef", "<prefix>.efi"]
	//   - .ef  (idx primary)    → ["<prefix>.efi"]
	//   - meta, salt, caplin    → empty
	// Empty Dependencies means no dependent files are required —
	// the entry can advance directly to LifecycleIndexed once Local.
	Dependencies []string

	// Anchors is the cryptographic step-header for this file: state-trie
	// root + consensus coordinates (block, txNum) + a flag for mid-block
	// commitments. Populated by validators (commitment domain) and
	// emitted into the V2 manifest's DomainFileEntry. IsZero() reports
	// whether no root has been recorded.
	Anchors Anchors
}

// Anchors is the cryptographic step-header for a file: a state-trie
// root tied to consensus block coordinates. Carried on FileEntry,
// emitted into the V2 manifest, used by the consumer-side coverage
// check. See docs/plans/20260510-partial-block-validation-model.md.
type Anchors struct {
	// Root is the recorded state-trie root for the snapshot at this
	// file's end. The 32-byte root is stored raw; the chain.toml
	// manifest renders it hex-encoded.
	Root [32]byte

	// AtBlock is the block whose execution produced Root. AtTxNum is
	// the txnum at which Root was recorded. Together they position the
	// proof in the chain timeline so consumers can cross-reference
	// against the block header's stateRoot.
	AtBlock uint64
	AtTxNum uint64

	// IsPartialBlock is true when AtTxNum < the block's max txnum, i.e.
	// the file's last recorded txn is inside AtBlock, not at its
	// boundary. Consumers MUST NOT treat partial-block files as a
	// valid snapshot-tip state.
	IsPartialBlock bool
}

// IsZero reports whether no anchor has been recorded.
func (a Anchors) IsZero() bool { return a == Anchors{} }

// Range returns the StepRange for this file entry.
func (f *FileEntry) Range() StepRange {
	return StepRange{f.FromStep, f.ToStep}
}

// Clone returns a shallow copy of the FileEntry. All fields are value types,
// so a shallow copy is a full copy.
func (f *FileEntry) Clone() *FileEntry {
	c := *f
	return &c
}

// Inventory tracks all known snapshot files (local and remote) for a node,
// organized by domain. It is the state model that storage uses to decide:
//   - What to download (gaps in local coverage vs peer offerings)
//   - What to seed (local files)
//   - What to advertise in chain.toml (all files, tagged with trust)
//   - What to accept from peers (trust level filtering)
//
// Thread-safe for concurrent reads and writes.
type Inventory struct {
	mu      sync.RWMutex
	domains map[Domain][]*FileEntry // kv + history + idx, keyed by domain
	blocks  []*FileEntry            // top-level block snapshots (.seg)
	caplin  []*FileEntry            // caplin beacon archive (.seg)
	meta    []*FileEntry            // erigondb.toml etc
	salt    []*FileEntry            // salt-*.txt

	// Held-view discipline: held views increment refcount on every
	// captured entry; eviction (RemoveFile, ReplaceWithMerge of
	// constituents) defers via pendingDeletes when refcount > 0. See
	// inventory_view.go for the View / Close / Subscribe surface.
	refcount       map[string]int
	pendingDeletes map[string]*FileEntry
	subs           []chan ChangeSet

	// Per-file lifecycle timestamps — feeds the future bandwidth-aware
	// download orchestrator. See timings.go for the shape and the
	// derived StepTimings view.
	timings map[string]*FileTimings

	// stepBlockBoundaries maps a commitment step's ToStep to the
	// canonical block number whose end-state is recorded at that
	// boundary. Populated by Stage-2 commitment-domain batch
	// validation. See step_block_binding.go.
	stepBlockBoundaries map[uint64]uint64

	// nowFn lets tests inject deterministic timestamps. Production
	// leaves this nil and time.Now() is used. Set via WithClock.
	nowFn func() time.Time
}

// NewInventory creates an empty inventory.
func NewInventory() *Inventory {
	return &Inventory{
		domains:        make(map[Domain][]*FileEntry),
		refcount:       make(map[string]int),
		pendingDeletes: make(map[string]*FileEntry),
	}
}

// AddFile adds a file entry to the inventory. If an entry with the same name
// already exists, it is replaced (useful for trust promotion or re-scan).
//
// Tier-0 metadata gate: ValidateMetadata is called before insertion;
// entries with empty Name, inverted/empty step ranges, or Kind that
// disagrees with the name's pattern are rejected and NOT added. The
// returned error reports the reason. Callers that don't care about
// the rejection cause may ignore the return value (Go's error-return
// is opt-in to inspect); the inventory will simply not contain the
// rejected entry.
//
// Routing:
//   - Kind=meta/salt/caplin: stored in their respective flat slice.
//   - Kind=kv (default), history, or idx: stored under entry.Domain when
//     non-empty, or in the blocks slice when Domain is empty.
//
// State derivation: if the caller did not set entry.State explicitly
// (i.e. State == LifecycleDeclared, the zero value), it is derived
// from the Local + Advertisable flags. This preserves back-compat with
// callers that still populate flags directly. Callers that own the
// lifecycle should set State explicitly and let applyStateToFlags
// keep the flags consistent.
func (inv *Inventory) AddFile(entry *FileEntry) error {
	if err := ValidateMetadata(entry); err != nil {
		return fmt.Errorf("AddFile: %w", err)
	}
	// Auto-fill Kind from name pattern when caller didn't set one
	// explicitly. Resolves the KindKV-zero-value ambiguity (see the
	// note on ValidateMetadata).
	if entry.Kind == "" {
		if inferred, ok := InferKind(entry.Name); ok {
			entry.Kind = inferred
		}
	}
	if entry.State == LifecycleDeclared && (entry.Local || entry.Advertisable) {
		entry.State = deriveStateFromFlags(entry.Local, entry.Advertisable)
	}
	inv.mu.Lock()
	now := inv.now()
	inv.recordEnqueueLocked(entry.Name, now)
	// If the entry arrives at a non-Declared state (typical for
	// bootstrap and disk-discovery paths), stamp the corresponding
	// timing slots so the orchestrator's timeline has a starting
	// reference. Records all states up to and including the entry's
	// current state — a file that lands at Advertisable on bootstrap
	// gets all four timestamps set to "now".
	if entry.State >= LifecycleDownloaded {
		inv.recordTimingTransitionLocked(entry.Name, LifecycleDownloaded, now)
	}
	if entry.State >= LifecycleIndexed {
		inv.recordTimingTransitionLocked(entry.Name, LifecycleIndexed, now)
	}
	if entry.State >= LifecycleAdvertisable {
		inv.recordTimingTransitionLocked(entry.Name, LifecycleAdvertisable, now)
	}
	switch entry.Kind {
	case KindCaplin:
		inv.caplin = replaceOrAppend(inv.caplin, entry)
	case KindMeta:
		inv.meta = replaceOrAppend(inv.meta, entry)
	case KindSalt:
		inv.salt = replaceOrAppend(inv.salt, entry)
	default:
		if entry.Domain == "" {
			inv.blocks = replaceOrAppend(inv.blocks, entry)
		} else {
			inv.domains[entry.Domain] = replaceOrAppend(inv.domains[entry.Domain], entry)
		}
	}
	inv.mu.Unlock()
	inv.notify(ChangeSet{Files: []string{entry.Name}})
	return nil
}

// RemoveFile removes a file entry by name. Scans every category since the
// caller doesn't supply a kind hint.
//
// If a held view still references this file (refcount > 0), the entry
// goes into pendingDeletes — held views continue to see their captured
// clone, and the pending-delete clears when the last referencing view
// closes. Subscribers receive a ChangeSet on completion.
func (inv *Inventory) RemoveFile(name string) {
	inv.mu.Lock()
	if inv.refcount[name] > 0 {
		if e := inv.findByNameLocked(name); e != nil {
			inv.pendingDeletes[name] = e.Clone()
		}
	} else {
		// No held view referencing this file → clean timings
		// immediately. Held-view case is handled in view.Close
		// when refcount drops to zero.
		delete(inv.timings, name)
	}
	inv.blocks = removeByName(inv.blocks, name)
	inv.caplin = removeByName(inv.caplin, name)
	inv.meta = removeByName(inv.meta, name)
	inv.salt = removeByName(inv.salt, name)
	for domain, entries := range inv.domains {
		inv.domains[domain] = removeByName(entries, name)
	}
	inv.mu.Unlock()
	inv.notify(ChangeSet{Files: []string{name}})
}

// ReplaceWithMerge atomically replaces multiple small files with a single merged file.
// Returns false if the merged file doesn't cover all replaced ranges.
//
// Constituents referenced by held views (refcount > 0) move into
// pendingDeletes for deferred eviction; held views continue to read
// their captured clones until close. Subscribers receive a single
// ChangeSet covering both the merged file and the replaced names.
func (inv *Inventory) ReplaceWithMerge(merged *FileEntry, replaced []string) bool {
	inv.mu.Lock()

	entries := inv.domains[merged.Domain]

	// Verify the merged file covers all replaced ranges.
	for _, name := range replaced {
		for _, e := range entries {
			if e.Name == name && !merged.Range().Covers(e.Range()) {
				inv.mu.Unlock()
				return false
			}
		}
	}

	// Held-view defer: snapshot constituents about to be evicted.
	// Files with no held view drop their timings immediately; the
	// held-view case is handled in view.Close when refcount drops.
	for _, name := range replaced {
		if inv.refcount[name] > 0 {
			if e := findByName(entries, name); e != nil {
				inv.pendingDeletes[name] = e.Clone()
			}
		} else {
			delete(inv.timings, name)
		}
	}

	// Remove replaced files.
	for _, name := range replaced {
		entries = removeByName(entries, name)
	}

	// Add the merged file.
	entries = replaceOrAppend(entries, merged)
	inv.domains[merged.Domain] = entries
	inv.mu.Unlock()

	files := append([]string{merged.Name}, replaced...)
	inv.notify(ChangeSet{Files: files})
	return true
}

// Coverage returns the step ranges covered by local files for a domain.
func (inv *Inventory) Coverage(domain Domain) StepRanges {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	return inv.coverageLocked(domain, true)
}

// FullCoverage returns the step ranges covered by ALL known files (local + remote)
// for a domain, regardless of trust level.
func (inv *Inventory) FullCoverage(domain Domain) StepRanges {
	inv.mu.RLock()
	defer inv.mu.RUnlock()
	return inv.coverageLocked(domain, false)
}

// CoverageAtTrust returns step ranges covered by files at or above the given trust level.
func (inv *Inventory) CoverageAtTrust(domain Domain, minTrust TrustLevel) StepRanges {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	var ranges StepRanges
	for _, e := range inv.domains[domain] {
		if e.Trust.Satisfies(minTrust) {
			ranges = append(ranges, e.Range())
		}
	}
	return ranges.Normalize()
}

func (inv *Inventory) coverageLocked(domain Domain, localOnly bool) StepRanges {
	var ranges StepRanges
	for _, e := range inv.domains[domain] {
		if localOnly && !e.Local {
			continue
		}
		ranges = append(ranges, e.Range())
	}
	return ranges.Normalize()
}

// GapsAgainst returns ranges that a remote inventory covers but we don't (locally).
// Used to build download requests.
func (inv *Inventory) GapsAgainst(domain Domain, remote StepRanges) StepRanges {
	local := inv.Coverage(domain)
	return local.GapsAgainst(remote)
}

// FilesForRange returns all files for a domain that overlap with [from, to).
func (inv *Inventory) FilesForRange(domain Domain, from, to uint64) []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	target := StepRange{from, to}
	var result []*FileEntry
	for _, e := range inv.domains[domain] {
		if e.Range().Overlaps(target) {
			result = append(result, e)
		}
	}
	return result
}

// LocalFiles returns all files that exist on disk for a domain.
// The returned slice is a copy, but entries are shared pointers.
// Callers MUST NOT mutate entries — use SetTorrentHash for hash updates.
func (inv *Inventory) LocalFiles(domain Domain) []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	var result []*FileEntry
	for _, e := range inv.domains[domain] {
		if e.Local {
			result = append(result, e)
		}
	}
	return result
}

// AllDomainFiles returns all files for a domain regardless of local/remote status.
// The returned slice is a copy, but entries are shared pointers.
// Callers MUST NOT mutate entries — use SetTorrentHash for hash updates.
func (inv *Inventory) AllDomainFiles(domain Domain) []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	result := make([]*FileEntry, len(inv.domains[domain]))
	copy(result, inv.domains[domain])
	return result
}

// BlockFiles returns all top-level block snapshot files (.seg).
func (inv *Inventory) BlockFiles() []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	result := make([]*FileEntry, len(inv.blocks))
	copy(result, inv.blocks)
	return result
}

// CaplinFiles returns all caplin beacon-archive files.
func (inv *Inventory) CaplinFiles() []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	result := make([]*FileEntry, len(inv.caplin))
	copy(result, inv.caplin)
	return result
}

// MetaFiles returns all chain-config metadata files (erigondb.toml etc).
func (inv *Inventory) MetaFiles() []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	result := make([]*FileEntry, len(inv.meta))
	copy(result, inv.meta)
	return result
}

// SaltFiles returns all hash-derivation salt files.
func (inv *Inventory) SaltFiles() []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	result := make([]*FileEntry, len(inv.salt))
	copy(result, inv.salt)
	return result
}

// CoverageOfKind returns the local step ranges covered by files of a
// specific kind for a domain. Use for kind-aware gap detection — a
// domain at [0, 128) with a .kv but no .v has full Coverage but a gap
// in CoverageOfKind(domain, KindHistory).
func (inv *Inventory) CoverageOfKind(domain Domain, kind FileKind) StepRanges {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	var ranges StepRanges
	for _, e := range inv.domains[domain] {
		if e.Kind != kind {
			continue
		}
		if !e.Local {
			continue
		}
		ranges = append(ranges, e.Range())
	}
	return ranges.Normalize()
}

// Domains returns the list of domains that have any files.
func (inv *Inventory) Domains() []Domain {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	domains := make([]Domain, 0, len(inv.domains))
	for d, entries := range inv.domains {
		if len(entries) > 0 {
			domains = append(domains, d)
		}
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
	return domains
}

// MarkAdvertisable flips a file's Advertisable flag to true. Returns
// true if the flag was actually changed (false if the file was
// already advertisable, or no file with that name is in the
// inventory).
//
// Callers run the producer-side validation chain BEFORE invoking
// this — the inventory itself doesn't enforce the gate, it just
// records the verdict. validation.Producer is the recommended
// caller; tests may flip the flag directly.
func (inv *Inventory) MarkAdvertisable(name string) bool {
	inv.mu.Lock()
	changed := false
	for _, slice := range [][]*FileEntry{inv.blocks, inv.caplin, inv.meta, inv.salt} {
		if e := findByName(slice, name); e != nil {
			if !e.Advertisable {
				applyStateToFlags(e, LifecycleAdvertisable)
				changed = true
			}
			inv.mu.Unlock()
			if changed {
				inv.notify(ChangeSet{Files: []string{name}})
			}
			return changed
		}
	}
	for _, entries := range inv.domains {
		if e := findByName(entries, name); e != nil {
			if !e.Advertisable {
				applyStateToFlags(e, LifecycleAdvertisable)
				changed = true
			}
			inv.mu.Unlock()
			if changed {
				inv.notify(ChangeSet{Files: []string{name}})
			}
			return changed
		}
	}
	inv.mu.Unlock()
	return false
}

// SetAnchors records the step-header cryptographic anchors on a file
// entry. Returns true when the entry was found and any field changed.
// Notifies subscribers on change.
func (inv *Inventory) SetAnchors(name string, a Anchors) bool {
	apply := func(e *FileEntry) bool {
		if e.Anchors == a {
			return false
		}
		e.Anchors = a
		return true
	}
	inv.mu.Lock()
	for _, slice := range [][]*FileEntry{inv.blocks, inv.caplin, inv.meta, inv.salt} {
		if e := findByName(slice, name); e != nil {
			changed := apply(e)
			inv.mu.Unlock()
			if changed {
				inv.notify(ChangeSet{Files: []string{name}})
			}
			return changed
		}
	}
	for _, entries := range inv.domains {
		if e := findByName(entries, name); e != nil {
			changed := apply(e)
			inv.mu.Unlock()
			if changed {
				inv.notify(ChangeSet{Files: []string{name}})
			}
			return changed
		}
	}
	inv.mu.Unlock()
	return false
}

// PromoteTrust promotes a file's trust level if the new level is higher.
// Returns true if the trust was actually changed. Notifies subscribers
// on change.
func (inv *Inventory) PromoteTrust(name string, newTrust TrustLevel) bool {
	inv.mu.Lock()
	changed := false
	for _, slice := range [][]*FileEntry{inv.blocks, inv.caplin, inv.meta, inv.salt} {
		if e := findByName(slice, name); e != nil {
			if newTrust > e.Trust {
				e.Trust = newTrust
				changed = true
			}
			inv.mu.Unlock()
			if changed {
				inv.notify(ChangeSet{Files: []string{name}})
			}
			return changed
		}
	}
	for _, entries := range inv.domains {
		if e := findByName(entries, name); e != nil {
			if newTrust > e.Trust {
				e.Trust = newTrust
				changed = true
			}
			inv.mu.Unlock()
			if changed {
				inv.notify(ChangeSet{Files: []string{name}})
			}
			return changed
		}
	}
	inv.mu.Unlock()
	return false
}

// helpers

func replaceOrAppend(entries []*FileEntry, entry *FileEntry) []*FileEntry {
	for i, e := range entries {
		if e.Name == entry.Name {
			entries[i] = entry
			return entries
		}
	}
	return append(entries, entry)
}

func removeByName(entries []*FileEntry, name string) []*FileEntry {
	for i, e := range entries {
		if e.Name == name {
			entries[i] = entries[len(entries)-1]
			entries[len(entries)-1] = nil
			return entries[:len(entries)-1]
		}
	}
	return entries
}

func findByName(entries []*FileEntry, name string) *FileEntry {
	for _, e := range entries {
		if e.Name == name {
			return e
		}
	}
	return nil
}
