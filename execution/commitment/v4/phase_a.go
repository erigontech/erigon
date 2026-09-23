// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for
// more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v4

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errPhaseAKey     = errors.New("commitment v4: invalid phase A key")
	errPhaseAUpdate  = errors.New("commitment v4: invalid phase A update")
	errPhaseAStorage = errors.New("commitment v4: invalid storage task")
	errPhaseAOrder   = errors.New("commitment v4: phase A input is not sorted by hashed key")
)

type storageEntry struct {
	path   []byte
	update *commitment.Update
}

type storageTask struct {
	addrHash [32]byte
	entries  []storageEntry
	wipe     bool
}

type accountEntry struct {
	hashedKey    []byte
	plainKey     []byte
	update       *commitment.Update
	storageDirty bool
}

type keyArena struct {
	buf []byte
}

const keyArenaChunk = 64 * 1024

func (a *keyArena) clone(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	if cap(a.buf)-len(a.buf) < len(src) {
		a.buf = make([]byte, 0, max(keyArenaChunk, len(src)))
	}
	start := len(a.buf)
	a.buf = append(a.buf, src...)
	return a.buf[start:len(a.buf):len(a.buf)]
}

type partitioner struct {
	storage      []storageTask
	accounts     []accountEntry
	keys         keyArena
	prev         []byte
	storageIndex int
	seen         int
}

func newPartitioner() *partitioner {
	return &partitioner{storageIndex: -1}
}

func (p *partitioner) add(hashedKey, plainKey []byte, update *commitment.Update) error {
	p.seen++
	if len(hashedKey) != 64 && len(hashedKey) != 128 {
		return nil
	}
	if p.prev != nil && bytes.Compare(hashedKey, p.prev) < 0 {
		return fmt.Errorf("%w: %x after %x", errPhaseAOrder, hashedKey, p.prev)
	}
	p.prev = append(p.prev[:0], hashedKey...)

	accountHash := hashedKey[:64]
	if len(p.accounts) == 0 || !bytes.Equal(p.accounts[len(p.accounts)-1].hashedKey, accountHash) {
		p.accounts = append(p.accounts, accountEntry{
			hashedKey: p.keys.clone(accountHash),
			plainKey:  p.keys.clone(prefix(plainKey, 20)),
		})
		p.storageIndex = -1
	}
	current := &p.accounts[len(p.accounts)-1]

	if len(hashedKey) == 64 {
		current.update = cloneUpdate(update)
		return nil
	}

	if p.storageIndex < 0 {
		p.storageIndex = len(p.storage)
		p.storage = append(p.storage, storageTask{addrHash: hashAddressPath(current.hashedKey)})
	}
	current.storageDirty = true
	p.storage[p.storageIndex].entries = append(p.storage[p.storageIndex].entries, storageEntry{
		path:   p.keys.clone(hashedKey[64:]),
		update: cloneUpdate(update),
	})
	return nil
}

func (p *partitioner) done() (storage []storageTask, accounts []accountEntry) {
	storage, accounts = p.storage, p.accounts
	for i := range accounts {
		if accounts[i].storageDirty || accounts[i].update == nil || !accounts[i].update.Deleted() {
			continue
		}
		storage = append(storage, storageTask{addrHash: hashAddressPath(accounts[i].hashedKey), wipe: true})
	}
	return storage, accounts
}

func hashAddressPath(path []byte) [32]byte {
	var addrHash [32]byte
	packPath(path, addrHash[:0])
	return addrHash
}

func prefix(src []byte, n int) []byte {
	if len(src) < n {
		return src
	}
	return src[:n]
}

func cloneUpdate(update *commitment.Update) *commitment.Update {
	if update == nil {
		return nil
	}
	return update.Copy()
}

func runStorageTask(ctx commitment.PatriciaContext, task storageTask) ([32]byte, error) {
	if ctx == nil {
		return [32]byte{}, errors.New("commitment v4: nil phase A context")
	}
	if task.wipe {
		if err := wipeStorageRecords(ctx, task.addrHash); err != nil {
			return [32]byte{}, err
		}
		return empty.RootHash, nil
	}
	if len(task.entries) == 0 {
		return empty.RootHash, nil
	}

	root, err := unfold(ctx, nil, planeStorage, task.addrHash[:])
	if err != nil {
		return [32]byte{}, err
	}
	if root == nil {
		root = fork(nil)
		root.loaded = true
	}
	root.plane = planeStorage
	markStorageRoot(root)
	before := new(keySet)
	g := storageGraph(task.addrHash[:], before)
	g.reachableRecordKeys(root, before)
	if err := g.materializeRootExtension(ctx, root); err != nil {
		return [32]byte{}, err
	}
	markStorageRoot(root)

	for _, entry := range task.entries {
		if len(entry.path) != 64 {
			return [32]byte{}, errPhaseAUpdate
		}
		for _, nib := range entry.path {
			if nib > 0x0f {
				return [32]byte{}, errPhaseAUpdate
			}
		}
		if entry.update == nil || entry.update.Flags == 0 {
			continue
		}
		if len(root.path) == 0 || bytes.HasPrefix(entry.path, root.path) {
			if err := g.ensurePath(ctx, root, entry.path); err != nil {
				return [32]byte{}, err
			}
		}
		if entry.update.Deleted() {
			if err := removeRoot(root, entry.path); err != nil {
				if errors.Is(err, ErrRemoveNotFound) {
					continue
				}
				return [32]byte{}, err
			}
			continue
		}
		if entry.update.Flags&commitment.StorageUpdate == 0 || entry.update.StorageLen < 0 {
			return [32]byte{}, errPhaseAUpdate
		}
		value := append([]byte(nil), entry.update.Storage[:entry.update.StorageLen]...)
		if err := insertRoot(root, entry.path, value); err != nil {
			return [32]byte{}, err
		}
	}

	markStorageRoot(root)
	if err := g.persistGraph(ctx, root); err != nil {
		return [32]byte{}, err
	}
	return fold(root, 0)
}

func markStorageRoot(root *node) {
	if root == nil {
		return
	}
	root.storageRoot = true
	for nib := range 16 {
		if root.childMask&(uint16(1)<<nib) == 0 || root.leafMask&(uint16(1)<<nib) != 0 {
			continue
		}
		markStorageRoot(root.child(nib))
	}
}

func putStorageRecord(ctx commitment.PatriciaContext, key, data []byte) error {
	delta, err := readRecordDelta(ctx, key, data)
	if err != nil {
		return err
	}
	return applyDelta(delta, ctx.PutBranch)
}
