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
	"sort"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errPhaseAKey     = errors.New("commitment v4: invalid phase A key")
	errPhaseAUpdate  = errors.New("commitment v4: invalid phase A update")
	errPhaseAStorage = errors.New("commitment v4: invalid storage task")
)

type phaseAInput struct {
	hashedKey []byte
	plainKey  []byte
	update    *commitment.Update
}

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

func partition(stream []phaseAInput) (storage []storageTask, accounts []accountEntry) {
	storageByAddress := make(map[string]int)
	accountByHash := make(map[string]int)
	wiped := make(map[[32]byte]struct{})
	for _, item := range stream {
		if len(item.hashedKey) != 64 && len(item.hashedKey) != 128 {
			continue
		}
		accountHash := append([]byte(nil), item.hashedKey[:64]...)
		accountKey := string(accountHash)
		accountIndex, ok := accountByHash[accountKey]
		if !ok {
			accountIndex = len(accounts)
			accountByHash[accountKey] = accountIndex
			accounts = append(accounts, accountEntry{
				hashedKey: accountHash,
				plainKey:  clonePrefix(item.plainKey, 20),
			})
		}

		if len(item.hashedKey) == 64 {
			accounts[accountIndex].update = cloneUpdate(item.update)
			addrHash := hashAddressPath(accountHash)
			if item.update != nil && item.update.Deleted() {
				wiped[addrHash] = struct{}{}
			} else {
				delete(wiped, addrHash)
			}
			continue
		}

		addrHash := hashAddressPath(item.hashedKey[:64])
		delete(wiped, addrHash)
		storageKey := string(addrHash[:])
		storageIndex, ok := storageByAddress[storageKey]
		if !ok {
			storageIndex = len(storage)
			storageByAddress[storageKey] = storageIndex
			storage = append(storage, storageTask{addrHash: addrHash})
		}
		accounts[accountIndex].storageDirty = true
		storage[storageIndex].entries = append(storage[storageIndex].entries, storageEntry{
			path:   append([]byte(nil), item.hashedKey[64:]...),
			update: cloneUpdate(item.update),
		})
	}

	sort.SliceStable(accounts, func(i, j int) bool {
		return bytes.Compare(accounts[i].hashedKey, accounts[j].hashedKey) < 0
	})
	sort.SliceStable(storage, func(i, j int) bool {
		return bytes.Compare(storage[i].addrHash[:], storage[j].addrHash[:]) < 0
	})
	for addrHash := range wiped {
		storage = append(storage, storageTask{addrHash: addrHash, wipe: true})
	}
	sort.SliceStable(storage, func(i, j int) bool {
		return bytes.Compare(storage[i].addrHash[:], storage[j].addrHash[:]) < 0
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

	root, err := unfold(ctx, nil, planeStorage, task.addrHash[:])
	if err != nil {
		return [32]byte{}, err
	}
	if root == nil {
		root = fork(nil)
	}
	root.plane = planeStorage
	before := reachableRecordKeys(root, task.addrHash)

	for _, entry := range task.entries {
		if entry.update == nil || len(entry.path) != 64 {
			return [32]byte{}, errPhaseAUpdate
		}
		if err := ensureStoragePath(ctx, root, entry.path, task.addrHash); err != nil {
			return [32]byte{}, err
		}
		if entry.update.Deleted() {
			if err := removeRoot(root, entry.path); err != nil {
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

	if err := persistStorageGraph(ctx, root, task.addrHash, before); err != nil {
		return [32]byte{}, err
	}
	return fold(root, 0)
}

func ensureStoragePath(ctx commitment.PatriciaContext, n *node, path []byte, addrHash [32]byte) error {
	if n == nil || len(path) != 64 || !bytes.HasPrefix(path, n.path) {
		return errPhaseAKey
	}
	if len(n.path) >= 64 {
		return nil
	}
	nib := int(path[len(n.path)])
	bit := uint16(1) << nib
	if len(n.path) != 0 && rootBitsCount(n.childMask) == 1 && n.leafMask == 0 {
		nib = trailingNibble(n.childMask)
		if child := n.children[nib]; child != nil && bytes.Equal(child.path, n.path) {
			return ensureStoragePath(ctx, child, path, addrHash)
		}
		if len(n.childHash[nib]) != 32 {
			return errPhaseAStorage
		}
		child, err := unfold(ctx, n.path, planeStorage, addrHash[:])
		if err != nil {
			return err
		}
		if child == nil {
			return fmt.Errorf("%w: missing child at depth %d", errPhaseAStorage, len(n.path))
		}
		child.plane = planeStorage
		n.setChild(nib, child)
		return ensureStoragePath(ctx, child, path, addrHash)
	}
	if n.childMask&bit == 0 || n.leafMask&bit != 0 {
		return nil
	}
	if child := n.children[nib]; child != nil {
		return ensureStoragePath(ctx, child, path, addrHash)
	}
	if len(n.childHash[nib]) != 32 {
		return errPhaseAStorage
	}
	childPath := append(append([]byte(nil), n.path...), byte(nib))
	childPath = append(childPath, n.childExt[nib]...)
	if !bytes.HasPrefix(path, childPath) {
		return nil
	}
	child, err := unfold(ctx, childPath, planeStorage, addrHash[:])
	if err != nil {
		return err
	}
	if child == nil {
		return fmt.Errorf("%w: missing child at depth %d", errPhaseAStorage, len(childPath))
	}
	child.plane = planeStorage
	n.setChild(nib, child)
	return ensureStoragePath(ctx, child, path, addrHash)
}

func reachableRecordKeys(root *node, addrHash [32]byte) map[string][]byte {
	keys := make(map[string][]byte)
	var visit func(*node, bool)
	visit = func(n *node, isRoot bool) {
		if n == nil {
			return
		}
		path := n.path
		if isRoot {
			path = nil
		}
		key := StorageNodeKey(addrHash, path, nil)
		keys[string(key)] = key
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			if child := n.children[nib]; child != nil {
				visit(child, false)
				continue
			}
			if len(n.childHash[nib]) == 32 {
				childPath := append([]byte(nil), n.path...)
				if len(n.path) == 0 {
					childPath = append(childPath, byte(nib))
				}
				childPath = append(childPath, n.childExt[nib]...)
				childKey := StorageNodeKey(addrHash, childPath, nil)
				keys[string(childKey)] = childKey
			}
		}
	}
	visit(root, true)
	return keys
}

func persistStorageGraph(ctx commitment.PatriciaContext, root *node, addrHash [32]byte, before map[string][]byte) error {
	if root == nil {
		return errPhaseAStorage
	}
	deltas := make([]recordDelta, 0, len(before)+1)
	var materialize func(*node) ([32]byte, error)
	materialize = func(n *node) ([32]byte, error) {
		if n == nil {
			return [32]byte{}, errPhaseAStorage
		}
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			child := n.children[nib]
			if child == nil {
				if len(n.childHash[nib]) != 32 {
					return [32]byte{}, errPhaseAStorage
				}
				continue
			}
			childHash, err := materialize(child)
			if err != nil {
				return [32]byte{}, err
			}
			n.childHash[nib] = appendCopy(n.childHash[nib], childHash[:])
			var ext []byte
			if !(n == root && len(n.path) != 0 && bytes.Equal(child.path, n.path)) {
				ext = child.path[len(n.path)+1:]
			}
			n.childExt[nib] = appendCopy(n.childExt[nib], ext)
			n.children[nib] = nil
		}
		path := n.path
		depth := len(path)
		if n == root {
			path = nil
			depth = 0
		}
		key := StorageNodeKey(addrHash, path, nil)
		hash, delta, err := foldAndEncodeRecord(ctx, n, depth, key)
		if err != nil {
			return [32]byte{}, err
		}
		deltas = append(deltas, delta)
		return hash, nil
	}
	if _, err := materialize(root); err != nil {
		return err
	}
	after := reachableRecordKeys(root, addrHash)
	for _, delta := range deltas {
		after[string(delta.key)] = delta.key
	}
	var err error
	deltas, err = appendRemovedDeltas(ctx, deltas, before, after)
	if err != nil {
		return err
	}
	if err := applyDeltas(deltas, ctx.PutBranch); err != nil {
		return err
	}
	return nil
}

func putStorageRecord(ctx commitment.PatriciaContext, key, data []byte) error {
	delta, err := readRecordDelta(ctx, key, data)
	if err != nil {
		return err
	}
	return applyDeltas([]recordDelta{delta}, ctx.PutBranch)
}
