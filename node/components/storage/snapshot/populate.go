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

import "sync"

// VisibleFileProvider is implemented by types that expose the current set of
// visible domain files. The provider pins the files for the duration of its
// lifetime (like a read transaction) — they can't change until it's closed.
//
// The storage component wires this to the aggregator's read transaction or
// a similar mechanism that provides a consistent snapshot of visible files.
type VisibleFileProvider interface {
	// VisibleDomainFiles returns the current set of visible domain files.
	VisibleDomainFiles() []DomainFileInfo

	// Close releases the pinned file references.
	Close()
}

// VisibleFileOpener creates a new VisibleFileProvider. Called each time the
// inventory needs to refresh its view of the visible files.
type VisibleFileOpener func() (VisibleFileProvider, error)

// DomainFileInfo describes a single visible domain file.
type DomainFileInfo struct {
	Domain   Domain
	FromStep uint64
	ToStep   uint64
	Name     string // relative filename
	Size     int64
}

// LiveInventory is a long-lived inventory that holds a pinned view of visible
// files and refreshes when notified of changes. It subscribes to file-change
// events and re-opens its view to get the updated file set.
//
// Thread-safe: the snapshot can be read concurrently while a refresh is in progress.
type LiveInventory struct {
	mu       sync.RWMutex
	current  *Inventory
	provider VisibleFileProvider // currently pinned view
	opener   VisibleFileOpener
}

// NewLiveInventory creates a live inventory that refreshes from the given opener.
// Call Refresh() after creation to populate the initial state.
func NewLiveInventory(opener VisibleFileOpener) *LiveInventory {
	return &LiveInventory{
		current: NewInventory(),
		opener:  opener,
	}
}

// Refresh closes the current pinned view, opens a new one, and rebuilds the
// inventory from the new visible files. This is called when file-change events
// are received (OnFilesChange from the aggregator, merge completion, etc).
func (li *LiveInventory) Refresh() error {
	newProvider, err := li.opener()
	if err != nil {
		return err
	}

	files := newProvider.VisibleDomainFiles()

	inv := NewInventory()
	for _, f := range files {
		inv.AddFile(&FileEntry{
			Domain:   f.Domain,
			FromStep: f.FromStep,
			ToStep:   f.ToStep,
			Name:     f.Name,
			Size:     f.Size,
			Local:    true,
			Trust:    TrustVerified,
		})
	}

	li.mu.Lock()
	oldProvider := li.provider
	// Preserve torrent hashes from old inventory.
	if li.current != nil {
		for _, domain := range li.current.Domains() {
			for _, oldEntry := range li.current.AllDomainFiles(domain) {
				if oldEntry.TorrentHash != [20]byte{} {
					for _, newEntry := range inv.AllDomainFiles(domain) {
						if newEntry.Name == oldEntry.Name {
							newEntry.TorrentHash = oldEntry.TorrentHash
							newEntry.Seeding = oldEntry.Seeding
							break
						}
					}
				}
			}
		}
	}
	li.current = inv
	li.provider = newProvider
	li.mu.Unlock()

	// Close old provider AFTER releasing the lock — this releases the old file references.
	if oldProvider != nil {
		oldProvider.Close()
	}

	return nil
}

// Snapshot returns the current inventory. The returned value is safe to read
// concurrently — it won't be modified (a new Inventory is created on each Refresh).
// Snapshot returns the current pinned inventory. The returned Inventory is
// shared — callers MUST NOT modify its entries directly. SetTorrentHash is
// the only sanctioned mutation path (it updates entries in place because
// torrent hashing is async and the inventory is rebuilt on each refresh).
func (li *LiveInventory) Snapshot() *Inventory {
	li.mu.RLock()
	defer li.mu.RUnlock()
	return li.current
}

// Close releases the currently pinned file view.
func (li *LiveInventory) Close() {
	li.mu.Lock()
	defer li.mu.Unlock()
	if li.provider != nil {
		li.provider.Close()
		li.provider = nil
	}
}

// SetTorrentHash sets the torrent hash for a file by name in the current inventory.
func (li *LiveInventory) SetTorrentHash(name string, hash [20]byte) {
	li.mu.RLock()
	defer li.mu.RUnlock()
	SetTorrentHash(li.current, name, hash)
}

// SetTorrentHash sets the torrent hash for a file by name. Tries each
// inventory bucket in turn (blocks → domains → caplin → meta → salt) so
// every entry kind gets its hash stamped. Missing a bucket here is
// gap-K: populateInventoryTorrentHashes still scans the .torrent files
// fine, but the kind whose bucket is skipped stays at zero-hash,
// GenerateV2 drops it from the manifest, and consumers never download
// it — for salt that surfaces downstream as "salt not found on ReloadSalt".
func SetTorrentHash(inv *Inventory, name string, hash [20]byte) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	stamp := func(e *FileEntry) {
		e.TorrentHash = hash
		e.Seeding = true
	}
	if e := findByName(inv.blocks, name); e != nil {
		stamp(e)
		return
	}
	for _, entries := range inv.domains {
		if e := findByName(entries, name); e != nil {
			stamp(e)
			return
		}
	}
	if e := findByName(inv.caplin, name); e != nil {
		stamp(e)
		return
	}
	if e := findByName(inv.meta, name); e != nil {
		stamp(e)
		return
	}
	if e := findByName(inv.salt, name); e != nil {
		stamp(e)
		return
	}
}

// ReplaceContent records that an inventory entry's bytes on disk have
// changed — the canonical-adoption cutover swap renamed a new file
// over the live one. It atomically updates the torrent hash, marks
// the entry seedable, and resets LifecycleState to Downloaded so the
// lifecycle driver re-emits the Indexed → Advertisable transitions
// downstream consumers subscribe to. ChangeSet fires once per call
// (after the lock releases).
//
// Returns true if an entry was found and updated. The signal-on-
// change semantics let cutover callers react to "this file's bytes
// just changed" without polling every entry. Per the canonical-layer
// revision spec §5.4 step (3).
func ReplaceContent(inv *Inventory, name string, hash [20]byte) bool {
	inv.mu.Lock()
	found := false
	apply := func(e *FileEntry) {
		e.TorrentHash = hash
		e.Seeding = true
		// Reset to Downloaded so the lifecycle driver re-promotes the
		// entry through Indexed → Advertisable, re-firing ChangeSet
		// events that downstream consumers (manifest_exchange caches,
		// segment caches) subscribe to.
		applyStateToFlags(e, LifecycleDownloaded)
		inv.recordTimingTransitionLocked(name, LifecycleDownloaded, inv.now())
		found = true
	}
	if e := findByName(inv.blocks, name); e != nil {
		apply(e)
	} else {
		for _, entries := range inv.domains {
			if e := findByName(entries, name); e != nil {
				apply(e)
				break
			}
		}
		if !found {
			if e := findByName(inv.caplin, name); e != nil {
				apply(e)
			} else if e := findByName(inv.meta, name); e != nil {
				apply(e)
			} else if e := findByName(inv.salt, name); e != nil {
				apply(e)
			}
		}
	}
	inv.mu.Unlock()
	if found {
		inv.notify(ChangeSet{Files: []string{name}})
	}
	return found
}
