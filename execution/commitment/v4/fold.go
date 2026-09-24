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

package v4

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var (
	errFoldNode  = errors.New("commitment v4: invalid fold node")
	errFoldPlane = errors.New("commitment v4: invalid fold plane")
)

func fold(n *node, depth int) ([32]byte, error) {
	if n == nil || depth < 0 || depth > 63 {
		return [32]byte{}, errFoldNode
	}
	if n.childMask == 0 {
		return empty.RootHash, nil
	}
	if n.plane != planeAccount && n.plane != planeStorage {
		return [32]byte{}, fmt.Errorf("%w: 0x%02x", errFoldPlane, n.plane)
	}
	if len(n.path) > 63 || depth != 0 && depth != len(n.path) {
		return [32]byte{}, fmt.Errorf("%w: depth %d path %d", errFoldNode, depth, len(n.path))
	}

	if depth == 0 && len(n.path) == 0 && bits.OnesCount16(n.childMask) == 1 && n.leafMask == n.childMask {
		nib := bits.TrailingZeros16(n.childMask)
		var rootRef [32]byte
		ref, err := foldLeaf(n, nib, 0, true, rootRef[:0])
		if err != nil {
			return [32]byte{}, err
		}
		if len(ref) != 32 {
			return keccak.Sum256(ref), nil
		}
		return [32]byte(ref), nil
	}
	if depth == 0 && len(n.path) != 0 && bits.OnesCount16(n.childMask) == 1 && n.leafMask == 0 {
		nib := bits.TrailingZeros16(n.childMask)
		var childHash []byte
		if child := n.child(nib); child != nil {
			hash, err := fold(child, len(child.path))
			if err != nil {
				return [32]byte{}, err
			}
			childHash = hash[:]
		} else {
			childHash = n.childHashAt(nib)
		}
		if len(childHash) != 32 {
			return [32]byte{}, fmt.Errorf("%w: root child %d hash", errFoldNode, nib)
		}
		return extensionRef(n.path, childHash), nil
	}

	var refs [16][]byte
	var refStore [16][32]byte
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.childMask&bit == 0 {
			continue
		}
		var ref []byte
		var err error
		if n.leafMask&bit != 0 {
			ref, err = foldLeaf(n, nib, len(n.path), false, refStore[nib][:0])
		} else {
			ref, err = foldBranchChild(n, nib, len(n.path))
		}
		if err != nil {
			return [32]byte{}, err
		}
		refs[nib] = ref
	}

	branchHash := branchRef(&refs)
	if depth == 0 && len(n.path) != 0 {
		return extensionRef(n.path, branchHash[:]), nil
	}
	return branchHash, nil
}

func foldLeaf(n *node, nib, depth int, includeNib bool, out []byte) ([]byte, error) {
	suffixCount := 64 - depth - 1
	suffix, payload := n.leafAt(nib)
	if len(suffix) != packedLen(suffixCount) {
		return nil, fmt.Errorf("%w: leaf %d suffix", errFoldNode, nib)
	}
	var keyScratch [65]byte
	key := keyScratch[:0]
	if includeNib {
		key = append(key, byte(nib))
	}
	start := len(key)
	key = key[:start+suffixCount]
	unpackPath(suffix, suffixCount, key[start:start+suffixCount:start+suffixCount])
	key = append(key, nibbles.Terminator)
	var compactScratch [34]byte
	compact := nibbles.HexToCompactInto(compactScratch[:0], key)
	var encScratch [leafRefScratch]byte
	if n.plane == planeAccount {
		nonce, balance, codeHash, storageRoot, err := decodeAccountLeaf(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: account leaf %d: %w", errFoldNode, nib, err)
		}
		var accScratch [accountRLPScratch]byte
		payload = accountConsensusRLP(nonce, &balance, storageRoot, codeHash, accScratch[:0])
		return append(out, leafRef(compact, payload, encScratch[:0])...), nil
	}
	return append(out, storageLeafRef(compact, payload, encScratch[:0])...), nil
}

func foldBranchChild(parent *node, nib, depth int) ([]byte, error) {
	var ext, hash []byte
	if child := parent.child(nib); child != nil {
		if len(child.path) <= depth || !bytes.HasPrefix(child.path, parent.path) || child.path[depth] != byte(nib) {
			return nil, fmt.Errorf("%w: child %d path", errFoldNode, nib)
		}
		childHash, err := fold(child, len(child.path))
		if err != nil {
			return nil, err
		}
		ext, hash = child.path[depth+1:], childHash[:]
	} else if !parent.hasChildHash(nib) {
		return nil, fmt.Errorf("%w: child %d hash", errFoldNode, nib)
	} else {
		ext, hash = parent.childExtAt(nib), parent.childHashAt(nib)
	}
	if len(ext) == 0 {
		return hash, nil
	}
	wrapped := extensionRef(ext, hash)
	return wrapped[:], nil
}
