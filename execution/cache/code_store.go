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

package cache

import (
	"bytes"
	"sync/atomic"

	"github.com/maypok86/otter/v2"

	"github.com/erigontech/erigon/db/kv"
)

// CodeStore is a two-tier code cache keyed by keccak(code): an in-memory otter
// tier over a persistent MDBX TblCodeCache backing, both holding DECOMPRESSED
// code so a hit skips the CodeDomain btree+decompress cost. Content-addressed,
// so entries are immutable and callers must key by the authoritative account
// codehash — a wrong codehash can only miss, never return wrong bytes.
type CodeStore struct {
	mem *otter.Cache[[32]byte, []byte]

	// tableSizeBytes is an in-memory approximation (not rescanned at startup);
	// Evict prunes the MDBX tier in cursor order, safe since RoTx reads preclude
	// LRU and entries re-derive from CodeDomain.
	tableCapBytes  uint64
	tableSizeBytes atomic.Int64
	tableSeeded    atomic.Bool

	memHits   atomic.Uint64
	tableHits atomic.Uint64
	misses    atomic.Uint64
}

// Stats returns and resets the (memHits, tableHits, misses) counters.
func (s *CodeStore) Stats() (memHits, tableHits, misses uint64) {
	if s == nil {
		return 0, 0, 0
	}
	return s.memHits.Swap(0), s.tableHits.Swap(0), s.misses.Swap(0)
}

const (
	DefaultCodeStoreMemBytes   = 256 * 1024 * 1024
	DefaultCodeStoreTableBytes = 1024 * 1024 * 1024
)

func NewCodeStore(memCapBytes, tableCapBytes uint64) *CodeStore {
	mem := otter.Must(&otter.Options[[32]byte, []byte]{
		MaximumWeight: memCapBytes,
		Weigher:       func(_ [32]byte, code []byte) uint32 { return uint32(len(code)) },
	})
	return &CodeStore{mem: mem, tableCapBytes: tableCapBytes}
}

// GetByHash returns decompressed code for codehash, checking the in-memory tier
// then the MDBX backing (populating the in-memory tier on a backing hit). A miss
// means the caller must fall through to the authoritative CodeDomain read.
func (s *CodeStore) GetByHash(tx kv.Getter, codeHash []byte) ([]byte, bool) {
	if s == nil || len(codeHash) != 32 {
		return nil, false
	}
	var key [32]byte
	copy(key[:], codeHash)
	if code, ok := s.mem.GetIfPresent(key); ok {
		s.memHits.Add(1)
		return code, true
	}
	code, err := tx.GetOne(kv.TblCodeCache, codeHash)
	if err != nil || len(code) == 0 {
		s.misses.Add(1)
		return nil, false
	}
	// GetOne returns mmap-backed memory that must not outlive the tx; the otter
	// tier is process-lifetime, so copy before caching/returning (kv contract).
	code = bytes.Clone(code)
	s.mem.Set(key, code)
	s.tableHits.Add(1)
	return code, true
}

// PutByHash records decompressed code in both tiers. The MDBX write needs an
// RwTx, so this runs on the code write path (deploy/commit), not on reads.
func (s *CodeStore) PutByHash(tx kv.RwTx, codeHash, code []byte) error {
	if s == nil || len(codeHash) != 32 || len(code) == 0 {
		return nil
	}
	var key [32]byte
	copy(key[:], codeHash)
	s.mem.Set(key, code)
	has, err := tx.Has(kv.TblCodeCache, codeHash)
	if err != nil {
		return err
	}
	if err := tx.Put(kv.TblCodeCache, codeHash, code); err != nil {
		return err
	}
	if !has {
		s.tableSizeBytes.Add(int64(len(codeHash) + len(code)))
	}
	return nil
}

// Evict prunes the MDBX backing to ~90% of tableCapBytes in cursor (codehash)
// order when over capacity. Safe: evicted entries are re-derivable from
// CodeDomain on miss. Call on a write tx (e.g., at commit), never on reads.
func (s *CodeStore) Evict(tx kv.RwTx) error {
	if s == nil || s.tableCapBytes == 0 {
		return nil
	}
	// tableSizeBytes starts at 0 each process start; seed it once from the
	// persistent table (in the same key+value byte units the eviction loop
	// decrements) so a backing that already exceeds the cap gets pruned rather
	// than growing unbounded across restarts.
	if s.tableSeeded.CompareAndSwap(false, true) {
		total, err := sumTableBytes(tx)
		if err != nil {
			return err
		}
		s.tableSizeBytes.Store(total)
	}
	if uint64(s.tableSizeBytes.Load()) <= s.tableCapBytes {
		return nil
	}
	c, err := tx.RwCursor(kv.TblCodeCache)
	if err != nil {
		return err
	}
	defer c.Close()
	target := int64(s.tableCapBytes / 10 * 9)
	for k, v, err := c.First(); k != nil && s.tableSizeBytes.Load() > target; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
		s.tableSizeBytes.Add(-int64(len(k) + len(v)))
	}
	return nil
}

// sumTableBytes returns the total key+value byte size of TblCodeCache, in the
// same units Evict tracks and decrements.
func sumTableBytes(tx kv.RwTx) (int64, error) {
	c, err := tx.Cursor(kv.TblCodeCache)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	var total int64
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return 0, err
		}
		total += int64(len(k) + len(v))
	}
	return total, nil
}
