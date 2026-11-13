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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
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
		n.setLeafShared(int(full[0]>>4), appendHashSuffix(&h.scratch, full, 1), leafValue)
	} else {
		l := record.layout()
		if value[0]&hdrHasSelfExt != 0 {
			h.scratch = decodeExtensionInto(record.SelfExt(), h.scratch[:0])
			n.path = h.scratch
		}
		n.record, n.layout = record, l
		n.childMask, n.leafMask, n.hashMask = l.child, l.leaf, l.tree()
	}

	rootExtension := depth == 0 && len(n.path) != 0
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.hashMask&bit != 0 {
			childPath := n.path
			if !rootExtension {
				h.child = append(append(h.child[:0], n.path...), byte(nib))
				ext := n.childExtAtInto(nib, h.child[len(h.child):cap(h.child)])
				h.child = append(h.child, ext...)
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
		h.key = StorageNodeKey(hashAddressPath(full[:]), nil, h.key[:0])
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
	tmpdir        string
	roots         *etl.Collector
	rootsFile     *os.File
	rootsReader   *bufio.Reader
	root          storageRootExpectation
	hasRoot       bool
	storagePlane  bool
	accountRoot   [32]byte
	records       uint64
	orphanStorage uint64
	trie          [32]byte
	inTrie, live  bool
}

func NewRecordMatcher(tmpdir string) *RecordMatcher {
	return newRecordMatcher(tmpdir, etl.BufferOptimalSize/8)
}

func newRecordMatcher(tmpdir string, buf datasize.ByteSize) *RecordMatcher {
	roots := etl.NewCollector("commitment v3 verify", tmpdir, etl.NewSortableBuffer(buf), log.Root()).LogLvl(log.LvlTrace)
	return &RecordMatcher{pending: map[string]pendingHash{}, tmpdir: tmpdir, roots: roots, accountRoot: empty.RootHash}
}

func (m *RecordMatcher) Close() {
	m.roots.Close()
	if m.rootsFile == nil {
		return
	}
	name := m.rootsFile.Name()
	m.rootsFile.Close()
	m.rootsFile = nil
	_ = dir.RemoveFile(name)
}

func isStorageRootKey(key []byte) bool {
	return len(key) == 34 && key[0] == tagStorageNode && key[33] == 0
}

func (m *RecordMatcher) Expect(key, hash []byte) error {
	if isStorageRootKey(key) {
		if m.storagePlane {
			return fmt.Errorf("%w: storage root %x expected after the account plane", errVerifyRecords, key)
		}
		var root [32]byte
		copy(root[:], hash)
		return m.roots.Collect(key[1:33], root[:])
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
	if err := m.startStoragePlane(); err != nil {
		return err
	}
	if m.hasRoot && bytes.Compare(m.root.addrHash[:], addrHash) < 0 {
		return fmt.Errorf("%w: account %x has storage root %x but no storage trie", errVerifyRecords, m.root.addrHash, m.root.root)
	}
	copy(m.trie[:], addrHash)
	m.inTrie = true
	m.live = m.hasRoot && bytes.Equal(m.root.addrHash[:], addrHash)
	return nil
}

func (m *RecordMatcher) startStoragePlane() error {
	if m.storagePlane {
		return nil
	}
	m.storagePlane = true
	f, err := os.CreateTemp(m.tmpdir, "commitment-v3-storage-roots-")
	if err != nil {
		return err
	}
	m.rootsFile = f
	w := bufio.NewWriter(f)
	if err := m.roots.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		if _, err := w.Write(k); err != nil {
			return err
		}
		_, err := w.Write(v)
		return err
	}, etl.TransformArgs{}); err != nil {
		return err
	}
	m.roots.Close()
	if err := w.Flush(); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	m.rootsReader = bufio.NewReader(f)
	return m.nextRoot()
}

func (m *RecordMatcher) nextRoot() error {
	_, err := io.ReadFull(m.rootsReader, m.root.addrHash[:])
	if errors.Is(err, io.EOF) {
		m.hasRoot = false
		return nil
	}
	if err == nil {
		_, err = io.ReadFull(m.rootsReader, m.root.root[:])
	}
	m.hasRoot = err == nil
	return err
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
		if m.root.root != hash {
			return fmt.Errorf("%w: account %x stores storage root %x, storage trie folds to %x", errVerifyRecords, key[1:33], m.root.root, hash)
		}
		return m.nextRoot()
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
	if err := m.startStoragePlane(); err != nil {
		return root, m.records, 0, err
	}
	if m.hasRoot {
		return root, m.records, 0, fmt.Errorf("%w: account %x has storage root %x but no storage trie", errVerifyRecords, m.root.addrHash, m.root.root)
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
