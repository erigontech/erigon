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
	"math/bits"
	"slices"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var errReadOnlyContext = errors.New("commitment v3: read-only branch context")

type collapseCell struct {
	hext             []byte
	hash, acct, stor bool
	acctKey          []byte
}

type collapseRow struct {
	cells [16]collapseCell
	after uint16
	depth int
}

type collapseSim struct {
	ctx    commitment.PatriciaContext
	tracer commitment.CollapseTracer
	root   collapseCell
	rows   []collapseRow
	key    []byte
	nodes  map[string]*node
}

func traceCollapses(ctx commitment.PatriciaContext, items []feedEntry, tracer commitment.CollapseTracer) error {
	s := &collapseSim{ctx: ctx, tracer: tracer, rows: make([]collapseRow, 0, 130), key: make([]byte, 0, 128), nodes: make(map[string]*node)}
	if err := s.loadRoot(); err != nil {
		return err
	}
	for i := range items {
		k := items[i].hashedKey
		for !bytes.HasPrefix(k, s.key) {
			if err := s.fold(); err != nil {
				return err
			}
		}
		for {
			unfolding, err := s.needUnfolding(k)
			if err != nil {
				return err
			}
			if unfolding <= 0 {
				break
			}
			if err := s.unfold(k, unfolding); err != nil {
				return err
			}
		}
		if items[i].update != nil && items[i].update.Deleted() {
			if err := s.deleteCell(k); err != nil {
				return err
			}
			continue
		}
		s.updateCell(k, len(items[i].plainKey) == length.Addr)
	}
	for len(s.rows) > 0 {
		if err := s.fold(); err != nil {
			return err
		}
	}
	return nil
}

func splitPrefix(prefix []byte) (plane byte, addrHash, path []byte) {
	if len(prefix) < 64 {
		return planeAccount, nil, prefix
	}
	addr := hashAddressPath(prefix[:64])
	return planeStorage, addr[:], prefix[64:]
}

func liveBranch(load func(plane byte, addrHash, path []byte) (*node, error), prefix []byte) (*node, error) {
	plane, addrHash, path := splitPrefix(prefix)
	if len(path) > 63 {
		return nil, nil
	}
	n, err := load(plane, addrHash, nil)
	if err != nil || n == nil || n.childMask == 0 || !bytes.HasPrefix(path, n.path) {
		return nil, err
	}
	single := bits.OnesCount16(n.childMask) == 1
	if len(n.path) == 0 && single && n.leafMask == n.childMask {
		return nil, nil
	}
	if len(n.path) != 0 && single && n.leafMask == 0 {
		if n, err = load(plane, addrHash, n.path); err != nil || n == nil {
			return nil, err
		}
	}
	for len(n.path) < len(path) {
		nib := int(path[len(n.path)])
		if bit := uint16(1) << nib; n.childMask&bit == 0 || n.leafMask&bit != 0 {
			return nil, nil
		}
		childPath := slices.Concat(n.path, []byte{byte(nib)}, n.childExtAt(nib))
		if !bytes.HasPrefix(path, childPath) {
			return nil, nil
		}
		if n, err = load(plane, addrHash, childPath); err != nil || n == nil {
			return nil, err
		}
	}
	return n, nil
}

func (s *collapseSim) load(plane byte, addrHash, path []byte) (*node, error) {
	key := string(nodeKey(plane, addrHash, path, nil))
	if n, ok := s.nodes[key]; ok {
		return n, nil
	}
	n, err := unfold(s.ctx, path, plane, addrHash)
	if err != nil {
		return nil, err
	}
	s.nodes[key] = n
	return n, nil
}

func (s *collapseSim) loadRoot() error {
	root, err := unfold(s.ctx, nil, planeAccount, nil)
	if err != nil || root == nil || root.childMask == 0 {
		return err
	}
	switch {
	case len(root.path) == 0 && bits.OnesCount16(root.childMask) == 1 && root.leafMask == root.childMask:
		nib := bits.TrailingZeros16(root.childMask)
		s.root = recordCell(root, nib, nil)
		s.root.hext = append([]byte{byte(nib)}, s.root.hext...)
	case len(root.path) != 0:
		s.root = collapseCell{hash: true, hext: slices.Clone(root.path)}
	default:
		s.root = collapseCell{hash: true}
	}
	return nil
}

func recordCell(n *node, nib int, prefix []byte) collapseCell {
	bit := uint16(1) << nib
	if n.leafMask&bit == 0 {
		return collapseCell{hash: true, hext: slices.Clone(n.childExtAt(nib))}
	}
	suffix, payload := n.leafAt(nib)
	rest := unpackPath(suffix, 64-len(n.path)-1, nil)
	if n.plane != planeAccount {
		return collapseCell{stor: true, hext: rest}
	}
	c := collapseCell{acct: true, hext: rest}
	if _, _, _, storageRoot, err := decodeAccountLeaf(payload); err != nil || !bytes.Equal(storageRoot, empty.RootHash[:]) {
		c.acctKey = slices.Concat(prefix, []byte{byte(nib)}, rest)
	}
	return c
}

func (s *collapseSim) resolve(c *collapseCell) error {
	if c.acctKey == nil {
		return nil
	}
	addrHash := hashAddressPath(c.acctKey)
	c.acctKey = nil
	root, err := unfold(s.ctx, nil, planeStorage, addrHash[:])
	if err != nil || root == nil || root.childMask == 0 {
		return err
	}
	switch {
	case len(root.path) == 0 && bits.OnesCount16(root.childMask) == 1 && root.leafMask == root.childMask:
		nib := bits.TrailingZeros16(root.childMask)
		suffix, _ := root.leafAt(nib)
		c.stor = true
		c.hext = append(append(c.hext, byte(nib)), unpackPath(suffix, 63, nil)...)
	case len(root.path) != 0:
		c.hash = true
		c.hext = append(c.hext, root.path...)
	default:
		c.hash = true
	}
	return nil
}

func clampToAccountBoundary(depth, n int) int {
	if depth < 64 && depth+n > 64 {
		return 64 - depth
	}
	return n
}

func (s *collapseSim) needUnfolding(k []byte) (int, error) {
	var c *collapseCell
	depth := 0
	if len(s.rows) == 0 {
		if len(s.root.hext) == 0 && !s.root.hash {
			return 0, nil
		}
		c = &s.root
	} else {
		top := &s.rows[len(s.rows)-1]
		depth = top.depth
		if len(k) <= depth {
			return 0, nil
		}
		c = &top.cells[k[len(s.key)]]
	}
	if err := s.resolve(c); err != nil {
		return 0, err
	}
	if len(c.hext) == 0 {
		if !c.hash {
			return 0, nil
		}
		return 1, nil
	}
	cpl := nibbles.CommonPrefixLen(k[depth:], c.hext[:len(c.hext)-1])
	return clampToAccountBoundary(depth, cpl+1), nil
}

func (s *collapseSim) unfold(k []byte, unfolding int) error {
	up, upDepth := s.root, 0
	if len(s.rows) != 0 {
		top := &s.rows[len(s.rows)-1]
		upDepth = top.depth
		up = top.cells[k[upDepth-1]]
		s.key = append(s.key, k[upDepth-1])
	}
	if len(up.hext) == 0 {
		return s.unfoldBranch(upDepth + 1)
	}
	lowest := min(unfolding, len(up.hext))
	depth := upDepth + lowest
	nib := up.hext[lowest-1]
	row := collapseRow{after: uint16(1) << nib}
	row.cells[nib] = fillFromUpper(up, lowest)
	row.depth = depth
	s.key = append(s.key, up.hext[:lowest-1]...)
	s.rows = append(s.rows, row)
	return nil
}

func (s *collapseSim) unfoldBranch(depth int) error {
	n, err := liveBranch(s.load, s.key)
	if err != nil {
		return err
	}
	if n == nil {
		return fmt.Errorf("commitment v3: collapse trace found no branch at %x", s.key)
	}
	row := collapseRow{depth: depth, after: n.childMask}
	for bitset := n.childMask; bitset != 0; bitset &= bitset - 1 {
		nib := bits.TrailingZeros16(bitset)
		row.cells[nib] = recordCell(n, nib, s.key)
	}
	s.rows = append(s.rows, row)
	return nil
}

func fillFromUpper(up collapseCell, inc int) collapseCell {
	c := collapseCell{acct: up.acct, stor: up.stor, hash: up.hash, acctKey: up.acctKey}
	if len(up.hext) > inc {
		c.hext = slices.Clone(up.hext[inc:])
	}
	return c
}

func (s *collapseSim) updateCell(k []byte, account bool) {
	c, depth := &s.root, 0
	if len(s.rows) != 0 {
		top := &s.rows[len(s.rows)-1]
		depth = top.depth
		nib := k[len(s.key)]
		c = &top.cells[nib]
		top.after |= uint16(1) << nib
	}
	if len(c.hext) == 0 {
		c.hext = slices.Clone(k[depth:])
	}
	if account {
		c.acct = true
	} else {
		c.stor = true
	}
}

func (s *collapseSim) deleteCell(k []byte) error {
	if len(s.rows) >= 2 {
		parent := &s.rows[len(s.rows)-2]
		if bits.OnesCount16(parent.after) == 2 {
			depth := parent.depth - 1
			for bitset := parent.after; bitset != 0; bitset &= bitset - 1 {
				if sib := bits.TrailingZeros16(bitset); sib != int(k[depth]) {
					if err := s.report(parent, depth, sib); err != nil {
						return err
					}
					break
				}
			}
		}
	}
	if len(s.rows) == 0 {
		s.root = collapseCell{}
		return nil
	}
	top := &s.rows[len(s.rows)-1]
	if top.depth < len(k) {
		return nil
	}
	nib := k[len(s.key)]
	top.after &^= uint16(1) << nib
	top.cells[nib] = collapseCell{}
	return nil
}

func (s *collapseSim) report(row *collapseRow, depth, nib int) error {
	c := &row.cells[nib]
	if err := s.resolve(c); err != nil {
		return err
	}
	s.tracer(slices.Concat(s.key[:depth], []byte{byte(nib)}, c.hext), slices.Clone(s.key[:depth]))
	return nil
}

func (s *collapseSim) fold() error {
	top := len(s.rows) - 1
	r := &s.rows[top]
	up, upDepth, nib := &s.root, 0, 0
	if top > 0 {
		upDepth = s.rows[top-1].depth
		nib = int(s.key[upDepth-1])
		up = &s.rows[top-1].cells[nib]
	}
	switch bits.OnesCount16(r.after) {
	case 0:
		if top > 0 && upDepth != 64 {
			parent := &s.rows[top-1]
			parent.after &^= uint16(1) << nib
			if bits.OnesCount16(parent.after) == 1 {
				if err := s.report(parent, parent.depth-1, bits.TrailingZeros16(parent.after)); err != nil {
					return err
				}
			}
		}
		*up = collapseCell{}
	case 1:
		child := bits.TrailingZeros16(r.after)
		fillFromLower(up, &r.cells[child], r.depth, s.key[upDepth:], child)
	default:
		up.hext = slices.Clone(s.key[upDepth:])
		if r.depth < 64 {
			up.acct = false
		}
		up.stor, up.hash, up.acctKey = false, true, nil
	}
	s.rows = s.rows[:top]
	s.key = s.key[:max(upDepth-1, 0)]
	return nil
}

func fillFromLower(up, low *collapseCell, lowDepth int, pre []byte, nib int) {
	if low.acct || lowDepth < 64 {
		up.acct = low.acct
	}
	up.stor = low.stor
	if low.hash && ((!low.acct && lowDepth < 64) || (!low.stor && lowDepth > 64)) {
		up.hext = slices.Concat(pre, []byte{byte(nib)}, low.hext)
	}
	up.hash, up.acctKey = low.hash, nil
}

type branchReader func(key []byte) ([]byte, error)

func (r branchReader) Branch(key []byte) ([]byte, kv.Step, error) {
	data, err := r(key)
	return data, 0, err
}

func (branchReader) PutBranch([]byte, []byte, []byte) error { return errReadOnlyContext }

func (branchReader) Account([]byte) (*commitment.Update, error) { return nil, errReadOnlyContext }

func (branchReader) Storage([]byte) (*commitment.Update, error) { return nil, errReadOnlyContext }

func (t *Trie) BranchChildCount(read func([]byte) ([]byte, error), nibblePrefix []byte) (int, error) {
	ctx := branchReader(read)
	n, err := liveBranch(func(plane byte, addrHash, path []byte) (*node, error) { return unfold(ctx, path, plane, addrHash) }, nibblePrefix)
	if err != nil || n == nil {
		return 0, err
	}
	return bits.OnesCount16(n.childMask), nil
}
