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
	"context"
	"errors"
	"fmt"
	"math/bits"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var errWitnessHash = errors.New("commitment v3: witness node hash differs from its reference")

type witnessNode struct {
	n    *node
	rlp  []byte
	hash [32]byte
}

type witnessRoot struct {
	n       *node
	rlp     []byte
	hash    [32]byte
	branch  *witnessNode
	leafKey []byte
	payload []byte
}

type witnessWalk struct {
	ctx       commitment.PatriciaContext
	exclusion bool
	byHash    map[string][]byte
	nodes     map[string]*witnessNode
	roots     map[string]*witnessRoot
	exts      map[string]struct{}
}

func (t *Trie) WitnessesByHash(ctx context.Context, updates *commitment.Updates, produceExclusionProofs bool) (map[string][]byte, [][]byte, []byte, error) {
	if t.ctx == nil {
		return nil, nil, nil, errTrieContext
	}
	keys := updates.CollectedHashedKeys()
	w := &witnessWalk{ctx: t.ctx, exclusion: produceExclusionProofs, byHash: make(map[string][]byte), nodes: make(map[string]*witnessNode), roots: make(map[string]*witnessRoot), exts: make(map[string]struct{})}
	want, err := t.RootHash()
	if err != nil {
		return nil, nil, nil, err
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		if err := w.walkPlane(planeAccount, nil, key, key, want); err != nil {
			return nil, nil, nil, fmt.Errorf("witness key %x: %w", key, err)
		}
	}
	return w.byHash, keys, want, nil
}

func (w *witnessWalk) emit(rlp []byte, hash [32]byte) {
	if _, ok := w.byHash[string(hash[:])]; !ok {
		w.byHash[string(hash[:])] = rlp
	}
}

func (w *witnessWalk) seen(key string) bool {
	if _, ok := w.exts[key]; ok {
		return true
	}
	w.exts[key] = struct{}{}
	return false
}

func (w *witnessWalk) load(plane byte, addrHash, path []byte) (*witnessNode, error) {
	key := string(nodeKey(plane, addrHash, path, nil))
	if wn, ok := w.nodes[key]; ok {
		return wn, nil
	}
	n, err := unfold(w.ctx, path, plane, addrHash)
	if err != nil {
		return nil, err
	}
	var wn *witnessNode
	if n != nil && n.childMask != 0 {
		rlp, err := branchRLP(n)
		if err != nil {
			return nil, err
		}
		wn = &witnessNode{n: n, rlp: rlp, hash: keccak.Sum256(rlp)}
	}
	w.nodes[key] = wn
	return wn, nil
}

func (w *witnessWalk) child(plane byte, addrHash []byte, parent *node, nib int, path []byte) (*witnessNode, error) {
	wn, err := w.load(plane, addrHash, path)
	if err != nil {
		return nil, err
	}
	if wn == nil || !bytes.Equal(wn.hash[:], parent.childHashAt(nib)) {
		return nil, fmt.Errorf("%w: child %x at depth %d", errWitnessHash, nib, len(path))
	}
	return wn, nil
}

func (w *witnessWalk) root(plane byte, addrHash []byte) (*witnessRoot, error) {
	if r, ok := w.roots[string(addrHash)]; ok {
		return r, nil
	}
	n, err := unfold(w.ctx, nil, plane, addrHash)
	if err != nil {
		return nil, err
	}
	var r *witnessRoot
	switch {
	case n == nil || n.childMask == 0:
	case len(n.path) == 0 && bits.OnesCount16(n.childMask) == 1 && n.leafMask == n.childMask:
		r = &witnessRoot{n: n}
		if r.rlp, r.leafKey, r.payload, err = leafRLP(n, bits.TrailingZeros16(n.childMask), true); err != nil {
			return nil, err
		}
	case len(n.path) != 0 && n.leafMask == 0 && bits.OnesCount16(n.childMask) == 1:
		r = &witnessRoot{n: n}
		if r.branch, err = w.child(plane, addrHash, n, bits.TrailingZeros16(n.childMask), n.path); err != nil {
			return nil, err
		}
		r.rlp = appendExtensionRLP(nil, n.path, r.branch.hash[:])
	default:
		rlp, err := branchRLP(n)
		if err != nil {
			return nil, err
		}
		r = &witnessRoot{n: n, branch: &witnessNode{n: n, rlp: rlp, hash: keccak.Sum256(rlp)}, rlp: rlp}
		if len(n.path) != 0 {
			r.rlp = appendExtensionRLP(nil, n.path, r.branch.hash[:])
		}
	}
	if r != nil {
		r.hash = keccak.Sum256(r.rlp)
	}
	w.roots[string(addrHash)] = r
	return r, nil
}

func (w *witnessWalk) walkPlane(plane byte, addrHash, fullKey, k []byte, want []byte) error {
	r, err := w.root(plane, addrHash)
	if err != nil {
		return err
	}
	if r == nil {
		if !bytes.Equal(want, empty.RootHash[:]) {
			return fmt.Errorf("%w: empty root, want %x", errWitnessHash, want)
		}
		return nil
	}
	if !bytes.Equal(r.hash[:], want) {
		return fmt.Errorf("%w: root %x, want %x", errWitnessHash, r.hash, want)
	}
	w.emit(r.rlp, r.hash)
	if r.leafKey != nil {
		return w.leaf(plane, fullKey, k, r.leafKey, r.payload)
	}
	if len(r.n.path) == 0 {
		return w.descend(plane, addrHash, fullKey, r.branch.n, nil, k)
	}
	if !bytes.HasPrefix(k, r.n.path) {
		if w.exclusion && plane == planeStorage {
			w.emit(r.branch.rlp, r.branch.hash)
		}
		return nil
	}
	w.emit(r.branch.rlp, r.branch.hash)
	return w.descend(plane, addrHash, fullKey, r.branch.n, r.n.path, k[len(r.n.path):])
}

func (w *witnessWalk) descend(plane byte, addrHash, fullKey []byte, b *node, path, k []byte) error {
	for len(k) > 0 {
		nib := int(k[0])
		k = k[1:]
		bit := uint16(1) << nib
		if b.childMask&bit == 0 {
			return nil
		}
		if b.leafMask&bit != 0 {
			rlp, leafKey, payload, err := leafRLP(b, nib, false)
			if err != nil {
				return err
			}
			w.emit(rlp, keccak.Sum256(rlp))
			return w.leaf(plane, fullKey, k, leafKey, payload)
		}
		ext := b.childExtAt(nib)
		childPath := append(append(append(make([]byte, 0, len(path)+1+len(ext)), path...), byte(nib)), ext...)
		if len(ext) != 0 {
			if extKey := string(nodeKey(plane, addrHash, childPath, nil)); !w.seen(extKey) {
				rlp := appendExtensionRLP(nil, ext, b.childHashAt(nib))
				w.emit(rlp, keccak.Sum256(rlp))
			}
			if !bytes.HasPrefix(k, ext) {
				if !w.exclusion || len(k) == 0 {
					return nil
				}
				behind, err := w.child(plane, addrHash, b, nib, childPath)
				if err != nil {
					return err
				}
				w.emit(behind.rlp, behind.hash)
				return nil
			}
			k = k[len(ext):]
		}
		child, err := w.child(plane, addrHash, b, nib, childPath)
		if err != nil {
			return err
		}
		w.emit(child.rlp, child.hash)
		b, path = child.n, childPath
	}
	return nil
}

func (w *witnessWalk) leaf(plane byte, fullKey, k, leafKey, payload []byte) error {
	if !bytes.HasPrefix(k, leafKey) {
		return nil
	}
	rest := k[len(leafKey):]
	if plane != planeAccount || len(rest) == 0 {
		return nil
	}
	_, _, _, storageRoot, err := decodeAccountLeaf(payload)
	if err != nil {
		return err
	}
	if bytes.Equal(storageRoot, empty.RootHash[:]) {
		return nil
	}
	addrHash := hashAddressPath(fullKey[:64])
	return w.walkPlane(planeStorage, addrHash[:], fullKey, rest, storageRoot)
}

func branchRLP(n *node) ([]byte, error) {
	var refs [16][]byte
	var leafRefs [16][32]byte
	for nib := range 16 {
		bit := uint16(1) << nib
		if n.childMask&bit == 0 {
			continue
		}
		var err error
		if n.leafMask&bit != 0 {
			refs[nib], err = foldLeaf(n, nib, false, leafRefs[nib][:0])
		} else {
			refs[nib], err = foldBranchChild(n, nib)
		}
		if err != nil {
			return nil, err
		}
	}
	return appendBranchRLP(nil, &refs), nil
}

func leafRLP(n *node, nib int, includeNib bool) (rlp, leafKey, payload []byte, err error) {
	suffixCount := 64 - len(n.path) - 1
	suffix, payload := n.leafAt(nib)
	if len(suffix) != packedLen(suffixCount) {
		return nil, nil, nil, fmt.Errorf("%w: leaf %d suffix", errFoldNode, nib)
	}
	leafKey = make([]byte, 0, suffixCount+1)
	if includeNib {
		leafKey = append(leafKey, byte(nib))
	}
	leafKey = append(leafKey, unpackPath(suffix, suffixCount, nil)...)
	compact := nibbles.HexToCompact(append(bytes.Clone(leafKey), nibbles.Terminator))
	if n.plane != planeAccount {
		return appendStorageLeafRLP(nil, compact, payload), leafKey, payload, nil
	}
	nonce, balance, codeHash, storageRoot, err := decodeAccountLeaf(payload)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: account leaf %d: %w", errFoldNode, nib, err)
	}
	return appendLeafRLP(nil, compact, accountConsensusRLP(nonce, &balance, storageRoot, codeHash, nil)), leafKey, payload, nil
}
