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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details.
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
	errPhaseBKey    = errors.New("commitment v4: invalid phase B key")
	errPhaseBRecord = errors.New("commitment v4: invalid account record")
)

func runAccountTrie(ctx commitment.PatriciaContext, entries []accountEntry, roots map[[32]byte][32]byte) ([32]byte, error) {
	if ctx == nil {
		return [32]byte{}, errors.New("commitment v4: nil phase B context")
	}

	root, err := unfold(ctx, nil, planeAccount, nil)
	if err != nil {
		return [32]byte{}, err
	}
	if root == nil {
		root = fork(nil)
	}
	root.plane = planeAccount
	before := reachableAccountRecordKeys(root)

	for _, entry := range entries {
		if len(entry.hashedKey) != 64 {
			return [32]byte{}, errPhaseBKey
		}
		for _, nib := range entry.hashedKey {
			if nib > 0x0f {
				return [32]byte{}, errPhaseBKey
			}
		}

		oldValue, found := accountLeafAt(root, entry.hashedKey)
		if !found && storedAccountPath(root, entry.hashedKey) {
			if err := ensureAccountPath(ctx, root, entry.hashedKey); err != nil {
				return [32]byte{}, fmt.Errorf("%w: account path %x: %w", errPhaseBRecord, entry.hashedKey, err)
			}
			oldValue, found = accountLeafAt(root, entry.hashedKey)
		}
		if entry.update != nil && entry.update.Deleted() {
			if !found {
				continue
			}
			if err := removeRoot(root, entry.hashedKey); err != nil {
				return [32]byte{}, err
			}
			continue
		}
		if !entry.storageDirty && (entry.update == nil || entry.update.Flags == 0) {
			continue
		}
		if !found && entry.update == nil && entry.storageDirty {
			addrHash := hashAddressPath(entry.hashedKey)
			storageRoot, ok := roots[addrHash]
			if !ok || storageRoot == empty.RootHash {
				continue
			}
		}
		if !found && entry.update != nil && entry.update.Flags == 0 {
			continue
		}

		var storageRoot []byte
		switch {
		case entry.storageDirty:
			addrHash := hashAddressPath(entry.hashedKey)
			storageRoot = empty.RootHash[:]
			if rootHash, ok := roots[addrHash]; ok {
				storageRoot = rootHash[:]
			}
		case found:
			_, _, _, existingRoot, decodeErr := decodeAccountLeaf(oldValue)
			if decodeErr != nil {
				return [32]byte{}, fmt.Errorf("%w: %w", errPhaseBRecord, decodeErr)
			}
			storageRoot = existingRoot
		default:
			storageRoot = empty.RootHash[:]
		}

		update, err := accountUpdate(oldValue, found, entry.update)
		if err != nil {
			return [32]byte{}, err
		}
		value := encodeAccountLeaf(update, storageRoot, nil)
		if len(root.path) != 0 && !bytes.HasPrefix(entry.hashedKey, root.path) && rootBitsCount(root.childMask) == 1 && root.leafMask == 0 {
			if err := materializeAccountRootChild(ctx, root); err != nil {
				return [32]byte{}, err
			}
		}
		var insertErr error
		if len(root.path) == 0 {
			insertErr = insert(root, entry.hashedKey, packPath(entry.hashedKey[1:], nil), value)
		} else {
			insertErr = insertRoot(root, entry.hashedKey, value)
		}
		if insertErr != nil {
			return [32]byte{}, insertErr
		}
	}

	if err := persistAccountGraph(ctx, root, before); err != nil {
		return [32]byte{}, err
	}
	return fold(root, 0)
}

func materializeAccountRootChild(ctx commitment.PatriciaContext, root *node) error {
	nib := trailingNibble(root.childMask)
	child := root.children[nib]
	if child == nil {
		return nil
	}
	hash, err := fold(child, len(child.path))
	if err != nil {
		return err
	}
	if err := persistDetachedAccountSubtree(ctx, child); err != nil {
		return err
	}
	var ext []byte
	if !bytes.Equal(child.path, root.path) {
		if len(child.path) <= len(root.path)+1 {
			return errPhaseBRecord
		}
		ext = child.path[len(root.path)+1:]
	}
	root.setStoredChild(nib, hash[:], ext)
	return nil
}

func persistDetachedAccountSubtree(ctx commitment.PatriciaContext, root *node) error {
	if root == nil {
		return errPhaseBRecord
	}
	var visit func(*node) error
	visit = func(n *node) error {
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			child := n.children[nib]
			if child == nil {
				if len(n.childHash[nib]) != 32 {
					return errPhaseBRecord
				}
				continue
			}
			if err := visit(child); err != nil {
				return err
			}
			hash, err := fold(child, len(child.path))
			if err != nil {
				return err
			}
			n.childHash[nib] = appendCopy(n.childHash[nib], hash[:])
			ext := child.path[len(n.path)+1:]
			n.childExt[nib] = appendCopy(n.childExt[nib], ext)
		}
		_, delta, err := foldAndEncodeRecord(ctx, n, len(n.path), AccountNodeKey(n.path, nil))
		if err != nil {
			return err
		}
		return applyDeltas([]recordDelta{delta}, ctx.PutBranch)
	}
	return visit(root)
}

func accountUpdate(value []byte, found bool, update *commitment.Update) (*commitment.Update, error) {
	result := &commitment.Update{CodeHash: empty.CodeHash}
	if found {
		nonce, balance, codeHash, _, err := decodeAccountLeaf(value)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errPhaseBRecord, err)
		}
		result.Nonce = nonce
		result.Balance.Set(&balance)
		copy(result.CodeHash[:], codeHash)
	}
	if update == nil {
		return result, nil
	}
	if update.Flags&commitment.BalanceUpdate != 0 {
		result.Balance.Set(&update.Balance)
	}
	if update.Flags&commitment.NonceUpdate != 0 {
		result.Nonce = update.Nonce
	}
	if update.Flags&commitment.CodeUpdate != 0 {
		result.CodeHash = update.CodeHash
	}
	return result, nil
}

func ensureAccountPath(ctx commitment.PatriciaContext, n *node, path []byte) error {
	if n == nil || len(path) != 64 || !bytes.HasPrefix(path, n.path) {
		if n == nil {
			return fmt.Errorf("%w: nil node", errPhaseBKey)
		}
		return fmt.Errorf("%w: node path %x", errPhaseBKey, n.path)
	}
	if len(n.path) >= 64 {
		return nil
	}
	nib := int(path[len(n.path)])
	bit := uint16(1) << nib
	if len(n.path) != 0 && rootBitsCount(n.childMask) == 1 && n.leafMask == 0 {
		nib = trailingNibble(n.childMask)
		if child := n.children[nib]; child != nil && bytes.Equal(child.path, n.path) {
			return ensureAccountPath(ctx, child, path)
		}
		if len(n.childHash[nib]) != 32 {
			return errPhaseBRecord
		}
		child, err := unfold(ctx, n.path, planeAccount, nil)
		if err != nil {
			return err
		}
		if child == nil {
			return fmt.Errorf("%w: missing child at depth %d", errPhaseBRecord, len(n.path))
		}
		child.plane = planeAccount
		n.setChild(nib, child)
		return ensureAccountPath(ctx, child, path)
	}
	if n.childMask&bit == 0 || n.leafMask&bit != 0 {
		return nil
	}
	if child := n.children[nib]; child != nil {
		return ensureAccountPath(ctx, child, path)
	}
	if len(n.childHash[nib]) != 32 {
		return errPhaseBRecord
	}
	childPath := append(append([]byte(nil), n.path...), byte(nib))
	childPath = append(childPath, n.childExt[nib]...)
	if !bytes.HasPrefix(path, childPath) {
		return nil
	}
	child, err := unfold(ctx, childPath, planeAccount, nil)
	if err != nil {
		return err
	}
	if child == nil {
		return fmt.Errorf("%w: missing child at depth %d", errPhaseBRecord, len(childPath))
	}
	child.plane = planeAccount
	n.setChild(nib, child)
	return ensureAccountPath(ctx, child, path)
}

func accountLeafAt(n *node, path []byte) ([]byte, bool) {
	if n == nil || len(path) != 64 || !bytes.HasPrefix(path, n.path) || len(n.path) >= len(path) {
		return nil, false
	}
	nib := int(path[len(n.path)])
	bit := uint16(1) << nib
	if n.childMask&bit == 0 {
		return nil, false
	}
	if n.leafMask&bit != 0 {
		suffixCount := len(path) - len(n.path) - 1
		if len(n.leafSuffix[nib]) != packedLen(suffixCount) || !bytes.Equal(unpackPath(n.leafSuffix[nib], suffixCount, nil), path[len(n.path)+1:]) {
			return nil, false
		}
		return append([]byte(nil), n.leafValue[nib]...), true
	}
	return accountLeafAt(n.children[nib], path)
}

func storedAccountPath(n *node, path []byte) bool {
	if n == nil || len(path) != 64 || !bytes.HasPrefix(path, n.path) || len(n.path) >= len(path) {
		return false
	}
	nib := int(path[len(n.path)])
	bit := uint16(1) << nib
	if n.childMask&bit == 0 || n.leafMask&bit != 0 {
		return false
	}
	if child := n.children[nib]; child != nil {
		return storedAccountPath(child, path)
	}
	if len(n.childHash[nib]) != 32 {
		return false
	}
	childPath := append(append([]byte(nil), n.path...), byte(nib))
	childPath = append(childPath, n.childExt[nib]...)
	return bytes.HasPrefix(path, childPath)
}

func reachableAccountRecordKeys(root *node) map[string][]byte {
	keys := make(map[string][]byte)
	var visit func(*node, bool)
	visit = func(n *node, isRoot bool) {
		if n == nil {
			return
		}
		var key []byte
		if isRoot {
			key = AccountRootKey()
		} else {
			key = AccountNodeKey(n.path, nil)
		}
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
				childPath := append(append([]byte(nil), n.path...), byte(nib))
				childPath = append(childPath, n.childExt[nib]...)
				childKey := AccountNodeKey(childPath, nil)
				keys[string(childKey)] = childKey
			}
		}
	}
	visit(root, true)
	return keys
}

func persistAccountGraph(ctx commitment.PatriciaContext, root *node, before map[string][]byte) error {
	if root == nil {
		return errPhaseBRecord
	}
	deltas := make([]recordDelta, 0, len(before)+1)
	var materialize func(*node) ([32]byte, error)
	materialize = func(n *node) ([32]byte, error) {
		if n == nil {
			return [32]byte{}, errPhaseBRecord
		}
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			child := n.children[nib]
			if child == nil {
				if len(n.childHash[nib]) != 32 {
					return [32]byte{}, errPhaseBRecord
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
		key := AccountNodeKey(path, nil)
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
	after := reachableAccountRecordKeys(root)
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
