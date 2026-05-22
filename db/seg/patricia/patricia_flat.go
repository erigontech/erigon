// Copyright 2021 The Erigon Authors
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

package patricia

import (
	"math/bits"

	"github.com/erigontech/erigon/db/seg/sais"
)

// flatNode is a cache-friendly node stored in a contiguous arena.
// Fields are indexed by bit value (0 or 1) to eliminate branching.
type flatNode struct {
	p   [2]uint32 // path descriptors, same encoding as node.p0/p1
	n   [2]uint32 // child indices into FlatTree.nodes (0 = no child)
	val uint32    // index into FlatTree.values (0 = no value)
}

// FlatTree is a patricia tree stored as a flat arena of nodes.
// Built by flattening a pointer-based PatriciaTree after all inserts.
// Note: values is []any because Match.Val is any throughout the patricia API.
// The slice is small (one entry per dictionary pattern, not per node) so GC
// overhead from scanning interface headers is negligible in practice.
type FlatTree struct {
	nodes  []flatNode
	values []any
}

// Flatten converts a pointer-based PatriciaTree into a flat arena representation.
func (pt *PatriciaTree) Flatten() *FlatTree {
	nc, vc := countNodes(&pt.root)
	ft := &FlatTree{
		nodes:  make([]flatNode, 1, nc+1), // index 0 is nil sentinel
		values: make([]any, 1, vc+1),      // index 0 is nil sentinel
	}
	ft.flattenNode(&pt.root)
	return ft
}

func countNodes(n *node) (nodes, values int) {
	nodes = 1
	if n.val != nil {
		values = 1
	}
	if n.n0 != nil {
		cn, cv := countNodes(n.n0)
		nodes += cn
		values += cv
	}
	if n.n1 != nil {
		cn, cv := countNodes(n.n1)
		nodes += cn
		values += cv
	}
	return
}

func (ft *FlatTree) flattenNode(n *node) uint32 {
	idx := uint32(len(ft.nodes))
	ft.nodes = append(ft.nodes, flatNode{
		p: [2]uint32{n.p0, n.p1},
	})

	var valIdx uint32
	if n.val != nil {
		valIdx = uint32(len(ft.values))
		ft.values = append(ft.values, n.val)
	}

	var n0idx, n1idx uint32
	if n.n0 != nil {
		n0idx = ft.flattenNode(n.n0)
	}
	if n.n1 != nil {
		n1idx = ft.flattenNode(n.n1)
	}

	ft.nodes[idx].val = valIdx
	ft.nodes[idx].n[0] = n0idx
	ft.nodes[idx].n[1] = n1idx
	return idx
}

const rootIdx = 1

type MatchFinder3 struct {
	ft         *FlatTree
	topIdx     uint32
	nodeStack  []uint32
	matchStack []Match
	matches    Matches
	sa         []int32
	lcp        []int32
	inv        []int32
	saisBuf    []int32
	headLen    int
	tailLen    int
	side       int // 0, 1, or 2 (undetermined)
}

func NewMatchFinder3(ft *FlatTree) *MatchFinder3 {
	return &MatchFinder3{
		ft:        ft,
		topIdx:    rootIdx,
		nodeStack: []uint32{rootIdx},
		side:      2,
	}
}

func (mf *MatchFinder3) unfold(b byte) uint32 {
	nodes := mf.ft.nodes
	bitsLeft := 8
	b32 := uint32(b) << 24
	topIdx := mf.topIdx
	headLen := mf.headLen
	tailLen := mf.tailLen
	side := mf.side

	for bitsLeft > 0 {
		if side == 2 {
			side = int(b32 >> 31)
			headLen = 0
			tailLen = int(nodes[topIdx].p[side] & 0x1f)
			if tailLen == 0 {
				side = 2
				mf.topIdx = topIdx
				mf.headLen = headLen
				mf.tailLen = tailLen
				mf.side = side
				return b32 | uint32(bitsLeft)
			}
		}
		// Reachable from carry-over state: previous unfold returned at the bottom
		// child==0 path (line ~193) with tailLen=0, side=0|1. Handles the case
		// where the current node has no child on this side.
		if tailLen == 0 {
			child := nodes[topIdx].n[side]
			if child == 0 {
				mf.topIdx = topIdx
				mf.headLen = headLen
				mf.tailLen = tailLen
				mf.side = side
				return b32 | uint32(bitsLeft)
			}
			mf.nodeStack = append(mf.nodeStack, child)
			topIdx = child
			headLen = 0
			side = 2
			continue
		}
		tail := (nodes[topIdx].p[side] & 0xffffffe0) << headLen
		firstDiff := bits.LeadingZeros32(tail ^ b32)
		if firstDiff < bitsLeft {
			if firstDiff >= tailLen {
				bitsLeft -= tailLen
				b32 <<= tailLen
				headLen += tailLen
				tailLen = 0
			} else {
				bitsLeft -= firstDiff
				b32 <<= firstDiff
				tailLen -= firstDiff
				headLen += firstDiff
				mf.topIdx = topIdx
				mf.headLen = headLen
				mf.tailLen = tailLen
				mf.side = side
				return b32 | uint32(bitsLeft)
			}
		} else if tailLen < bitsLeft {
			bitsLeft -= tailLen
			b32 <<= tailLen
			headLen += tailLen
			tailLen = 0
		} else {
			tailLen -= bitsLeft
			headLen += bitsLeft
			bitsLeft = 0
			b32 = 0
		}
		if tailLen == 0 {
			child := nodes[topIdx].n[side]
			if child == 0 {
				mf.topIdx = topIdx
				mf.headLen = headLen
				mf.tailLen = tailLen
				mf.side = side
				return b32 | uint32(bitsLeft)
			}
			mf.nodeStack = append(mf.nodeStack, child)
			topIdx = child
			headLen = 0
			side = 2
		}
	}
	mf.topIdx = topIdx
	mf.headLen = headLen
	mf.tailLen = tailLen
	mf.side = side
	return 0
}

func (mf *MatchFinder3) fold(nbits int) {
	nodes := mf.ft.nodes
	bitsLeft := nbits
	headLen := mf.headLen
	tailLen := mf.tailLen
	side := mf.side
	topIdx := mf.topIdx

	for bitsLeft > 0 {
		if headLen == bitsLeft {
			headLen = 0
			tailLen = 0
			side = 2
			bitsLeft = 0
		} else if headLen >= bitsLeft {
			headLen -= bitsLeft
			tailLen += bitsLeft
			bitsLeft = 0
		} else {
			bitsLeft -= headLen
			mf.nodeStack = mf.nodeStack[:len(mf.nodeStack)-1]
			prevTopIdx := topIdx
			topIdx = mf.nodeStack[len(mf.nodeStack)-1]
			if nodes[topIdx].n[0] == prevTopIdx {
				side = 0
				headLen = int(nodes[topIdx].p[0] & 0x1f)
			} else if nodes[topIdx].n[1] == prevTopIdx {
				side = 1
				headLen = int(nodes[topIdx].p[1] & 0x1f)
			} else {
				panic("invalid nodeStack: previous node is not a child of current node")
			}
			tailLen = 0
		}
	}
	mf.headLen = headLen
	mf.tailLen = tailLen
	mf.side = side
	mf.topIdx = topIdx
}

func (mf *MatchFinder3) FindLongestMatches(data []byte) []Match {
	mf.matches = mf.matches[:0]
	if len(data) < 2 {
		return mf.matches
	}
	nodes := mf.ft.nodes
	values := mf.ft.values
	mf.nodeStack = append(mf.nodeStack[:0], rootIdx)
	mf.matchStack = mf.matchStack[:0]
	mf.topIdx = rootIdx
	mf.side = 2
	mf.tailLen = 0
	mf.headLen = 0
	n := len(data)
	if cap(mf.sa) < n {
		mf.sa = make([]int32, n)
	} else {
		mf.sa = mf.sa[:n]
	}
	if err := sais.Sais(data, mf.sa, &mf.saisBuf); err != nil {
		panic(err)
	}
	if cap(mf.inv) < n {
		mf.inv = make([]int32, n)
	} else {
		mf.inv = mf.inv[:n]
	}
	for i := 0; i < n; i++ {
		mf.inv[mf.sa[i]] = int32(i)
	}
	var k int
	if cap(mf.lcp) < n {
		mf.lcp = make([]int32, n)
	} else {
		mf.lcp = mf.lcp[:n]
	}
	for i := 0; i < n; i++ {
		if mf.inv[i] == int32(n-1) {
			k = 0
			continue
		}
		j := int(mf.sa[mf.inv[i]+1])
		for i+k < n && j+k < n && data[i+k] == data[j+k] {
			k++
		}
		mf.lcp[mf.inv[i]] = int32(k)
		if k > 0 {
			k--
		}
	}
	depth := 0
	var lastMatch *Match
	for i := 0; i < n; i++ {
		if i > 0 {
			lcp := int(mf.lcp[i-1])
			if depth > 8*lcp {
				mf.fold(depth - 8*lcp)
				depth = 8 * lcp
				for lastMatch != nil && lastMatch.End-lastMatch.Start > lcp {
					mf.matchStack = mf.matchStack[:len(mf.matchStack)-1]
					if len(mf.matchStack) == 0 {
						lastMatch = nil
					} else {
						lastMatch = &mf.matchStack[len(mf.matchStack)-1]
					}
				}
			} else {
				r := depth % 8
				if r > 0 {
					mf.fold(r)
					depth -= r
				}
			}
		}
		sa := int(mf.sa[i])
		start := sa + depth/8
		for end := start + 1; end <= n; end++ {
			d := mf.unfold(data[end-1])
			depth += 8 - int(d&0x1f)
			if d != 0 {
				break
			}
			topIdx := mf.topIdx
			if mf.tailLen != 0 || nodes[topIdx].val == 0 {
				continue
			}
			if cap(mf.matchStack) == len(mf.matchStack) {
				mf.matchStack = append(mf.matchStack, Match{})
			} else {
				mf.matchStack = mf.matchStack[:len(mf.matchStack)+1]
			}
			lastMatch = &mf.matchStack[len(mf.matchStack)-1]
			lastMatch.Start = sa
			lastMatch.End = end
			lastMatch.Val = values[nodes[topIdx].val]
		}
		if lastMatch != nil {
			mf.matches = append(mf.matches, Match{})
			m := &mf.matches[len(mf.matches)-1]
			m.Start = sa
			m.End = sa + lastMatch.End - lastMatch.Start
			m.Val = lastMatch.Val
		}
	}
	return deduplicateMatches(mf.matches)
}
