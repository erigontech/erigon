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

package v3

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common/empty"
)

var errVerifyRecords = errors.New("commitment v3: record verification failed")

type RecordHasher struct {
	n       node
	path    []byte
	addr    [32]byte
	scratch []byte
	child   []byte
	key     []byte
}

func NewRecordHasher() *RecordHasher {
	return &RecordHasher{n: node{slots: make([]childSlot, 0, 16)}}
}

func (h *RecordHasher) Hash(key, value []byte, expect func(key, hash []byte) error) ([32]byte, error) {
	tag, addrHash, err := h.parseKey(key)
	if err != nil {
		return [32]byte{}, err
	}
	depth := len(h.path)
	if err := Validate(value, depth); err != nil {
		return [32]byte{}, fmt.Errorf("%w: %x: %w", errVerifyRecords, key, err)
	}
	record := Record{data: value, depth: depth}
	n := &h.n
	resetNode(n, h.path, tag)
	h.scratch = h.scratch[:0]
	if record.isLeafRoot() {
		hashedKey, leafValue := value[1:33], value[34:]
		full := (*[32]byte)(hashedKey)
		appendLeaf(n, int(full[0]>>4), appendHashSuffix(&h.scratch, full, 1), leafValue)
	} else {
		l := record.layout()
		if value[0]&hdrHasSelfExt != 0 {
			selfExt := record.SelfExt()
			start := len(h.scratch)
			h.scratch = appendUnpacked(h.scratch, selfExt[1:], int(selfExt[0]))
			n.path = h.scratch[start:]
		}
		for nib := range 16 {
			bit := uint16(1) << nib
			switch {
			case l.child&bit == 0:
			case l.leaf&bit != 0:
				suffix, leafValue := record.leafAt(l, nib)
				appendLeaf(n, nib, suffix, leafValue)
			default:
				var ext []byte
				if encoded := record.extAt(l, nib); len(encoded) != 0 {
					start := len(h.scratch)
					h.scratch = appendUnpacked(h.scratch, encoded[1:], int(encoded[0]))
					ext = h.scratch[start:]
				}
				appendStoredChild(n, nib, record.slotAt(l, nib), ext)
			}
		}
	}

	rootExtension := depth == 0 && len(n.path) != 0
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.hashMask&bit != 0 {
			childPath := n.path
			if !rootExtension {
				h.child = append(append(h.child[:0], n.path...), byte(nib))
				h.child = append(h.child, n.childExtAt(nib)...)
				childPath = h.child
			}
			h.key = nodeKey(tag, addrHash, childPath, h.key[:0])
			if err := expect(h.key, n.childHashAt(nib)); err != nil {
				return [32]byte{}, err
			}
		}
		if tag != planeAccount || n.leafMask&bit == 0 {
			continue
		}
		suffix, leafValue := n.leafAt(nib)
		_, _, _, storageRoot, err := decodeAccountLeaf(leafValue)
		if err != nil {
			return [32]byte{}, fmt.Errorf("%w: %x leaf %x: %w", errVerifyRecords, key, nib, err)
		}
		if isEmptyStorageRoot(storageRoot) {
			continue
		}
		var full [64]byte
		copy(full[:], n.path)
		full[len(n.path)] = byte(nib)
		unpackPath(suffix, 63-len(n.path), full[len(n.path)+1:len(n.path)+1:64])
		var accountHash [32]byte
		packPath(full[:], accountHash[:0])
		h.key = StorageNodeKey(accountHash, nil, h.key[:0])
		if err := expect(h.key, storageRoot); err != nil {
			return [32]byte{}, err
		}
	}
	hash, err := fold(n, depth)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: %x: %w", errVerifyRecords, key, err)
	}
	return hash, nil
}

func (h *RecordHasher) parseKey(key []byte) (tag byte, addrHash []byte, err error) {
	if len(key) < 2 {
		return 0, nil, fmt.Errorf("%w: key %x", errVerifyRecords, key)
	}
	tag = key[0]
	start := 1
	switch tag {
	case tagAccountNode:
	case tagStorageNode:
		if len(key) < 34 {
			return 0, nil, fmt.Errorf("%w: key %x", errVerifyRecords, key)
		}
		copy(h.addr[:], key[1:33])
		addrHash, start = h.addr[:], 33
	default:
		return 0, nil, fmt.Errorf("%w: key %x has tag %02x", errVerifyRecords, key, tag)
	}
	count := int(key[len(key)-1])
	if count > 63 || len(key)-start-1 != packedLen(count) {
		return 0, nil, fmt.Errorf("%w: key %x: path length %d does not match", errVerifyRecords, key, count)
	}
	h.path = unpackPath(key[start:len(key)-1], count, h.path[:0])
	return tag, addrHash, nil
}

type pendingHash struct {
	hash     [32]byte
	expected bool
}

type storageRootExpectation struct {
	addrHash, root [32]byte
}

type RecordMatcher struct {
	pending       map[string]pendingHash
	roots         []storageRootExpectation
	rootsPos      int
	storagePlane  bool
	accountRoot   [32]byte
	records       uint64
	orphanStorage uint64
	trie          [32]byte
	inTrie, live  bool
}

func NewRecordMatcher() *RecordMatcher {
	return &RecordMatcher{pending: map[string]pendingHash{}, accountRoot: empty.RootHash}
}

func isStorageRootKey(key []byte) bool {
	return len(key) == 34 && key[0] == tagStorageNode && key[33] == 0
}

func (m *RecordMatcher) Expect(key, hash []byte) error {
	if isStorageRootKey(key) {
		if m.storagePlane {
			return fmt.Errorf("%w: storage root %x expected after the account plane", errVerifyRecords, key)
		}
		var e storageRootExpectation
		copy(e.addrHash[:], key[1:33])
		copy(e.root[:], hash)
		m.roots = append(m.roots, e)
		return nil
	}
	if key[0] == tagStorageNode {
		if err := m.enterTrie(key[1:33]); err != nil || !m.live {
			return err
		}
	}
	return m.match(key, (*[32]byte)(hash), true)
}

func (m *RecordMatcher) enterTrie(addrHash []byte) error {
	if m.inTrie && bytes.Equal(m.trie[:], addrHash) {
		return nil
	}
	if !m.storagePlane {
		m.storagePlane = true
		slices.SortFunc(m.roots, func(a, b storageRootExpectation) int { return bytes.Compare(a.addrHash[:], b.addrHash[:]) })
	}
	if m.rootsPos < len(m.roots) && bytes.Compare(m.roots[m.rootsPos].addrHash[:], addrHash) < 0 {
		return fmt.Errorf("%w: account %x has storage root %x but no storage trie", errVerifyRecords, m.roots[m.rootsPos].addrHash, m.roots[m.rootsPos].root)
	}
	copy(m.trie[:], addrHash)
	m.inTrie = true
	m.live = m.rootsPos < len(m.roots) && bytes.Equal(m.roots[m.rootsPos].addrHash[:], addrHash)
	return nil
}

func (m *RecordMatcher) Record(key []byte, hash [32]byte) error {
	m.records++
	switch {
	case len(key) == 2 && key[0] == tagAccountNode && key[1] == 0:
		m.accountRoot = hash
		return nil
	case key[0] == tagStorageNode:
		if err := m.enterTrie(key[1:33]); err != nil {
			return err
		}
		if !m.live {
			m.orphanStorage++
			return nil
		}
		if !isStorageRootKey(key) {
			return m.match(key, &hash, false)
		}
		if m.roots[m.rootsPos].root != hash {
			return fmt.Errorf("%w: account %x stores storage root %x, storage trie folds to %x", errVerifyRecords, key[1:33], m.roots[m.rootsPos].root, hash)
		}
		m.rootsPos++
		return nil
	}
	return m.match(key, &hash, false)
}

func (m *RecordMatcher) match(key []byte, hash *[32]byte, expected bool) error {
	p, ok := m.pending[string(key)]
	if !ok {
		m.pending[string(key)] = pendingHash{hash: *hash, expected: expected}
		return nil
	}
	if p.expected == expected {
		return fmt.Errorf("%w: %x is recorded or expected twice", errVerifyRecords, key)
	}
	if p.hash != *hash {
		if expected {
			return fmt.Errorf("%w: %x folds to %x, its parent stores %x", errVerifyRecords, key, p.hash, *hash)
		}
		return fmt.Errorf("%w: %x folds to %x, its parent stores %x", errVerifyRecords, key, *hash, p.hash)
	}
	delete(m.pending, string(key))
	return nil
}

func (m *RecordMatcher) Finish() (root [32]byte, records, orphans uint64, err error) {
	if m.rootsPos < len(m.roots) {
		e := m.roots[m.rootsPos]
		return root, m.records, 0, fmt.Errorf("%w: account %x has storage root %x but no storage trie", errVerifyRecords, e.addrHash, e.root)
	}
	orphans = m.orphanStorage
	for key, p := range m.pending {
		if p.expected {
			return root, m.records, orphans, fmt.Errorf("%w: %x is referenced but missing", errVerifyRecords, []byte(key))
		}
		orphans++
	}
	return m.accountRoot, m.records, orphans, nil
}

func appendUnpacked(dst, packed []byte, count int) []byte {
	dst = slices.Grow(dst, count)
	unpackPath(packed, count, dst[len(dst):])
	return dst[:len(dst)+count]
}
