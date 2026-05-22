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
	"sort"
	"sync"
)

// Domain identifies a state domain (accounts, storage, code, commitment, etc).
type Domain string

const (
	DomainAccounts   Domain = "accounts"
	DomainStorage    Domain = "storage"
	DomainCode       Domain = "code"
	DomainCommitment Domain = "commitment"
)

// FileEntry represents a single snapshot file (block or domain) tracked by the inventory.
type FileEntry struct {
	// Domain this file belongs to. Empty for block snapshots.
	Domain Domain

	// Step range [FromStep, ToStep) covered by this file. Zero for block snapshots.
	FromStep uint64
	ToStep   uint64

	// File name (relative to snapshots directory).
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
}

// Range returns the StepRange for this file entry.
func (f *FileEntry) Range() StepRange {
	return StepRange{f.FromStep, f.ToStep}
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
	domains map[Domain][]*FileEntry
	blocks  []*FileEntry // block snapshots (headers, bodies, etc.)
}

// NewInventory creates an empty inventory.
func NewInventory() *Inventory {
	return &Inventory{
		domains: make(map[Domain][]*FileEntry),
	}
}

// AddFile adds a file entry to the inventory. If an entry with the same name
// already exists, it is replaced (useful for trust promotion or re-scan).
func (inv *Inventory) AddFile(entry *FileEntry) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	if entry.Domain == "" {
		inv.blocks = replaceOrAppend(inv.blocks, entry)
	} else {
		inv.domains[entry.Domain] = replaceOrAppend(inv.domains[entry.Domain], entry)
	}
}

// RemoveFile removes a file entry by name.
func (inv *Inventory) RemoveFile(name string) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	inv.blocks = removeByName(inv.blocks, name)
	for domain, entries := range inv.domains {
		inv.domains[domain] = removeByName(entries, name)
	}
}

// ReplaceWithMerge atomically replaces multiple small files with a single merged file.
// Returns false if the merged file doesn't cover all replaced ranges.
func (inv *Inventory) ReplaceWithMerge(merged *FileEntry, replaced []string) bool {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	entries := inv.domains[merged.Domain]

	// Verify the merged file covers all replaced ranges.
	for _, name := range replaced {
		for _, e := range entries {
			if e.Name == name && !merged.Range().Covers(e.Range()) {
				return false
			}
		}
	}

	// Remove replaced files.
	for _, name := range replaced {
		entries = removeByName(entries, name)
	}

	// Add the merged file.
	entries = replaceOrAppend(entries, merged)
	inv.domains[merged.Domain] = entries
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

// BlockFiles returns all block snapshot files.
func (inv *Inventory) BlockFiles() []*FileEntry {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	result := make([]*FileEntry, len(inv.blocks))
	copy(result, inv.blocks)
	return result
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

// PromoteTrust promotes a file's trust level if the new level is higher.
// Returns true if the trust was actually changed.
func (inv *Inventory) PromoteTrust(name string, newTrust TrustLevel) bool {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	if e := findByName(inv.blocks, name); e != nil {
		if newTrust > e.Trust {
			e.Trust = newTrust
			return true
		}
		return false
	}

	for _, entries := range inv.domains {
		if e := findByName(entries, name); e != nil {
			if newTrust > e.Trust {
				e.Trust = newTrust
				return true
			}
			return false
		}
	}
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
