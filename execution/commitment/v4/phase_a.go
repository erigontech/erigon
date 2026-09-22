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
	"math/bits"
	"slices"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errPhaseAKey     = errors.New("commitment v4: invalid phase A key")
	errPhaseAUpdate  = errors.New("commitment v4: invalid phase A update")
	errPhaseAStorage = errors.New("commitment v4: invalid storage task")
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

type partitioner struct {
	storage          []storageTask
	accounts         []accountEntry
	storageByAddress map[string]int
	accountByHash    map[string]int
	wiped            map[[32]byte]struct{}
	seen             int
}

func newPartitioner() *partitioner {
	return &partitioner{
		storageByAddress: make(map[string]int),
		accountByHash:    make(map[string]int),
		wiped:            make(map[[32]byte]struct{}),
	}
}

func (p *partitioner) add(hashedKey, plainKey []byte, update *commitment.Update) error {
	p.seen++
	if len(hashedKey) != 64 && len(hashedKey) != 128 {
		return nil
	}
	accountHash := append([]byte(nil), hashedKey[:64]...)
	accountKey := string(accountHash)
	accountIndex, ok := p.accountByHash[accountKey]
	if !ok {
		accountIndex = len(p.accounts)
		p.accountByHash[accountKey] = accountIndex
		p.accounts = append(p.accounts, accountEntry{
			hashedKey: accountHash,
			plainKey:  clonePrefix(plainKey, 20),
		})
	}

	if len(hashedKey) == 64 {
		p.accounts[accountIndex].update = cloneUpdate(update)
		addrHash := hashAddressPath(accountHash)
		if update != nil && update.Deleted() {
			p.wiped[addrHash] = struct{}{}
		} else {
			delete(p.wiped, addrHash)
		}
		return nil
	}

	addrHash := hashAddressPath(accountHash)
	delete(p.wiped, addrHash)
	storageKey := string(addrHash[:])
	storageIndex, ok := p.storageByAddress[storageKey]
	if !ok {
		storageIndex = len(p.storage)
		p.storageByAddress[storageKey] = storageIndex
		p.storage = append(p.storage, storageTask{addrHash: addrHash})
	}
	p.accounts[accountIndex].storageDirty = true
	p.storage[storageIndex].entries = append(p.storage[storageIndex].entries, storageEntry{
		path:   append([]byte(nil), hashedKey[64:]...),
		update: cloneUpdate(update),
	})
	return nil
}

func (p *partitioner) done() (storage []storageTask, accounts []accountEntry) {
	storage, accounts = p.storage, p.accounts
	for addrHash := range p.wiped {
		storage = append(storage, storageTask{addrHash: addrHash, wipe: true})
	}
	slices.SortStableFunc(accounts, func(a, b accountEntry) int {
		return bytes.Compare(a.hashedKey, b.hashedKey)
	})
	slices.SortStableFunc(storage, func(a, b storageTask) int {
		return bytes.Compare(a.addrHash[:], b.addrHash[:])
	})
	return storage, accounts
}

func hashAddressPath(path []byte) [32]byte {
	var addrHash [32]byte
	copy(addrHash[:], packPath(path, nil))
	return addrHash
}

func clonePrefix(src []byte, n int) []byte {
	if len(src) < n {
		return append([]byte(nil), src...)
	}
	return append([]byte(nil), src[:n]...)
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

	g := storageGraph(task.addrHash[:])
	root, err := unfold(ctx, nil, planeStorage, task.addrHash[:], g.scratch)
	if err != nil {
		return [32]byte{}, err
	}
	if root == nil {
		root = fork(nil)
	}
	root.plane = planeStorage
	markStorageRoot(root)
	before := g.reachableRecordKeys(root)

	for _, entry := range task.entries {
		if len(entry.path) != 64 {
			return [32]byte{}, errPhaseAUpdate
		}
		if entry.update == nil || entry.update.Flags == 0 {
			continue
		}
		if len(root.path) != 0 && !bytes.HasPrefix(entry.path, root.path) && bits.OnesCount16(root.childMask) == 1 && root.leafMask == 0 {
			nib := bits.TrailingZeros16(root.childMask)
			if root.children[nib] == nil && len(root.childHash[nib]) == 32 {
				child, err := unfold(ctx, root.path, planeStorage, task.addrHash[:], g.scratch)
				if err != nil {
					return [32]byte{}, err
				}
				if child == nil {
					return [32]byte{}, errPhaseAStorage
				}
				child.plane = planeStorage
				root.setChild(nib, child)
			}
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
	if err := g.persistGraph(ctx, root, before); err != nil {
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
		markStorageRoot(root.children[nib])
	}
}

func putStorageRecord(ctx commitment.PatriciaContext, key, data []byte) error {
	delta, err := readRecordDelta(ctx, key, data)
	if err != nil {
		return err
	}
	return applyDelta(delta, ctx.PutBranch)
}
