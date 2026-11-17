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
	"errors"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var (
	errFoldNode  = errors.New("commitment v3: invalid fold node")
	errFoldPlane = errors.New("commitment v3: invalid fold plane")
)

func fold(n *node, depth int) ([32]byte, error) {
	if n == nil {
		return [32]byte{}, errFoldNode
	}
	if n.childMask == 0 {
		return empty.RootHash, nil
	}
	if n.plane != planeAccount && n.plane != planeStorage {
		return [32]byte{}, fmt.Errorf("%w: 0x%02x", errFoldPlane, n.plane)
	}
	if len(n.path) > 63 {
		return [32]byte{}, fmt.Errorf("%w: path %d", errFoldNode, len(n.path))
	}

	if depth == 0 && n.isLeafRoot() {
		nib := bits.TrailingZeros16(n.childMask)
		var rootRef [32]byte
		ref, err := foldLeaf(n, nib, true, rootRef[:0])
		if err != nil {
			return [32]byte{}, err
		}
		return [32]byte(ref), nil
	}
	if depth == 0 && n.isExtension() {
		nib := bits.TrailingZeros16(n.childMask)
		childHash := n.childHashAt(nib)
		if len(childHash) != 32 {
			return [32]byte{}, fmt.Errorf("%w: root child %d hash", errFoldNode, nib)
		}
		return extensionRef(n.path, childHash), nil
	}

	var refStore [16][32]byte
	refs, err := branchRefs(n, &refStore)
	if err != nil {
		return [32]byte{}, err
	}
	branchHash, err := branchRef(&refs)
	if err != nil {
		return [32]byte{}, err
	}
	if depth == 0 && len(n.path) != 0 {
		return extensionRef(n.path, branchHash[:]), nil
	}
	return branchHash, nil
}

func foldLeaf(n *node, nib int, includeNib bool, out []byte) ([]byte, error) {
	if ref, ok := n.cachedLeafRef(nib); ok && !includeNib {
		return append(out, ref...), nil
	}
	var keyScratch [65]byte
	var compactScratch [34]byte
	var payloadScratch [accountRLPScratch]byte
	compact, _, _, payload, err := leafParts(n, nib, includeNib, keyScratch[:0], compactScratch[:0], payloadScratch[:0])
	if err != nil {
		return nil, err
	}
	var encScratch [leafRefScratch]byte
	if n.plane == planeAccount {
		return append(out, hashLongRef(appendLeafRLP(encScratch[:0], compact, payload))...), nil
	}
	return append(out, hashLongRef(appendStorageLeafRLP(encScratch[:0], compact, payload))...), nil
}

func branchRefs(n *node, refStore *[16][32]byte) (refs [16][]byte, err error) {
	for m := n.childMask; m != 0; m &= m - 1 {
		nib := bits.TrailingZeros16(m)
		bit := uint16(1) << nib
		if n.leafMask&bit == 0 {
			if err := foldBranchChild(n, nib, &refStore[nib]); err != nil {
				return refs, err
			}
			refs[nib] = refStore[nib][:]
			continue
		}
		ref, err := foldLeaf(n, nib, false, refStore[nib][:0])
		if err != nil {
			return refs, err
		}
		if len(ref) > 32 {
			return refs, fmt.Errorf("%w: leaf %d ref length %d", errFoldNode, nib, len(ref))
		}
		refs[nib] = refStore[nib][:copy(refStore[nib][:], ref)]
	}
	return refs, nil
}

func leafParts(n *node, nib int, includeNib bool, keyDst, compactDst, payloadDst []byte) (compact, leafKey, rawPayload, encodedPayload []byte, err error) {
	suffixCount := 64 - len(n.path) - 1
	suffix, rawPayload := n.leafAt(nib)
	if len(suffix) != packedLen(suffixCount) {
		return nil, nil, nil, nil, fmt.Errorf("%w: leaf %d suffix", errFoldNode, nib)
	}
	leafKey = keyDst[:0]
	if includeNib {
		leafKey = append(leafKey, byte(nib))
	}
	start := len(leafKey)
	leafKey = append(leafKey, make([]byte, suffixCount)...)
	unpackPath(suffix, suffixCount, leafKey[start:start+suffixCount:start+suffixCount])
	var compactKey [65]byte
	copy(compactKey[:], leafKey)
	compactKey[len(leafKey)] = nibbles.Terminator
	compact = nibbles.HexToCompactInto(compactDst[:0], compactKey[:len(leafKey)+1])
	encodedPayload = rawPayload
	if n.plane == planeAccount {
		nonce, balance, codeHash, storageRoot, decodeErr := decodeAccountLeaf(rawPayload)
		if decodeErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("%w: account leaf %d: %w", errFoldNode, nib, decodeErr)
		}
		encodedPayload = accountConsensusRLP(nonce, &balance, storageRoot, codeHash, payloadDst)
	} else if len(rawPayload) > length.Hash {
		return nil, nil, nil, nil, fmt.Errorf("%w: storage leaf %d payload length %d", errFoldNode, nib, len(rawPayload))
	}
	return compact, leafKey, rawPayload, encodedPayload, nil
}

func foldBranchChild(parent *node, nib int, dst *[32]byte) error {
	if !parent.hasChildHash(nib) {
		return fmt.Errorf("%w: child %d hash", errFoldNode, nib)
	}
	var extScratch [64]byte
	ext, hash := parent.childExtAtInto(nib, extScratch[:0]), parent.childHashAt(nib)
	if len(ext) != 0 {
		*dst = extensionRef(ext, hash)
		return nil
	}
	if len(hash) != 32 {
		return fmt.Errorf("%w: child %d hash length %d", errFoldNode, nib, len(hash))
	}
	*dst = [32]byte(hash)
	return nil
}
