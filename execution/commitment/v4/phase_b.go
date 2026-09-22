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
	"math/bits"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

var (
	errPhaseBKey    = errors.New("commitment v4: invalid phase B key")
	errPhaseBRecord = errors.New("commitment v4: invalid account record")
)

func materializeAccountRootChild(ctx commitment.PatriciaContext, root *node) error {
	nib := bits.TrailingZeros16(root.childMask)
	child := root.child(nib)
	if child == nil {
		return nil
	}
	hash, err := persistDetachedAccountSubtree(ctx, child)
	if err != nil {
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

func persistDetachedAccountSubtree(ctx commitment.PatriciaContext, root *node) ([32]byte, error) {
	if root == nil {
		return [32]byte{}, errPhaseBRecord
	}
	var visit func(*node) ([32]byte, error)
	visit = func(n *node) ([32]byte, error) {
		for nib := range 16 {
			bit := uint16(1) << nib
			if n.childMask&bit == 0 || n.leafMask&bit != 0 {
				continue
			}
			child := n.child(nib)
			if child == nil {
				if len(n.childHashAt(nib)) != 32 {
					return [32]byte{}, errPhaseBRecord
				}
				continue
			}
			childHash, err := visit(child)
			if err != nil {
				return [32]byte{}, err
			}
			n.setChildHashExt(nib, childHash[:], child.path[len(n.path)+1:])
		}
		hash, delta, err := foldAndEncodeRecord(ctx, n, len(n.path), AccountNodeKey(n.path, nil))
		if err != nil {
			return [32]byte{}, err
		}
		return hash, applyDelta(delta, ctx.PutBranch)
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
		if !packedMatches(n.leafSuffixAt(nib), path[len(n.path)+1:]) {
			return nil, false
		}
		return append([]byte(nil), n.leafValueAt(nib)...), true
	}
	return accountLeafAt(n.child(nib), path)
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
	if child := n.child(nib); child != nil {
		return storedAccountPath(child, path)
	}
	if len(n.childHashAt(nib)) != 32 {
		return false
	}
	childPath := append(append([]byte(nil), n.path...), byte(nib))
	childPath = append(childPath, n.childExtAt(nib)...)
	return bytes.HasPrefix(path, childPath)
}
