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

package patricia

import (
	"encoding/binary"
	"math/bits"
	"slices"
)

// Match is a single pattern occurrence: its associated value and the [Start, End)
// byte range it covers in the searched data.
type Match struct {
	Val   any
	Start int
	End   int
}

type Matches []Match

// acNode is the compiled per-state record. The scan is memory-latency bound: for
// each input byte it needs this state's fail link, its match and its edge, all
// indexed by the same jumpy state id. Packing them into one 12-byte record turns
// three random array loads into one.
//
// edge tags the fanout in three disjoint ranges, so one field both discriminates
// and carries its payload: noEdge means none; >=0 is a lone edge on byte(edge) to
// the *next* state, which insertion order already makes the common case; a
// wideTag value means the edges are spilled to wideByte/wideChild, run-length
// prefixed at wideStart(edge)-1. A lone edge whose child is not the next state
// spills too, as a one-edge run, so no state id has to be packed into a field
// narrower than int32.
//
// match indexes patLen/patVal rather than holding the length and value inline:
// those are per-pattern, not per-state, so a dictionary of p patterns needs p
// entries and not one per state.
type acNode struct {
	fail  int32
	match int32 // longest pattern ending at this state, noMatch for none
	edge  int32
}

const (
	noEdge  = int32(-1)
	noMatch = int32(-1)
)

// wideTag encodes a spilled run's start into edge, biased past noEdge so the
// lone-edge (>=0), no-edge (-1) and spilled (<=-2) ranges stay disjoint;
// wideStart inverts it.
func wideTag(start int32) int32  { return -start - 2 }
func wideStart(edge int32) int32 { return -edge - 2 }

// AhoCorasick is a byte-level multi-pattern automaton. Build it once from a
// pattern dictionary, then share it read-only across any number of ACMatcher
// instances (one per goroutine).
type AhoCorasick struct {
	// build-time trie, freed after Build compiles the packed nodes
	children []map[byte]int32
	depth    []int32
	val      []any
	hasVal   []bool

	// compiled automaton
	rootNext  [256]int32 // dense transitions from root (-1 = none)
	nodes     []acNode
	wideByte  []byte  // spilled runs: a fanout-1 prefix byte then sorted labels
	wideChild []int32 // child states, parallel to wideByte
	patLen    []int32 // pattern lengths, indexed by acNode.match
	patVal    []any   // pattern values, parallel to patLen
	built     bool
}

func NewAhoCorasick() *AhoCorasick {
	ac := &AhoCorasick{}
	ac.addNode(0) // root
	return ac
}

func (ac *AhoCorasick) addNode(depth int32) int32 {
	ac.children = append(ac.children, nil)
	ac.depth = append(ac.depth, depth)
	ac.val = append(ac.val, nil)
	ac.hasVal = append(ac.hasVal, false)
	return int32(len(ac.children) - 1)
}

// Insert adds a pattern with its value. Must be called before Build.
func (ac *AhoCorasick) Insert(pattern []byte, v any) {
	if ac.built {
		panic("AhoCorasick: Insert after Build")
	}
	if len(pattern) == 0 {
		return
	}
	cur := int32(0)
	for _, b := range pattern {
		m := ac.children[cur]
		if m == nil {
			m = make(map[byte]int32, 1)
			ac.children[cur] = m
		}
		nxt, ok := m[b]
		if !ok {
			nxt = ac.addNode(ac.depth[cur] + 1)
			m = ac.children[cur] // addNode may have grown the slice backing array
			m[b] = nxt
		}
		cur = nxt
	}
	ac.val[cur] = v
	ac.hasVal[cur] = true
}

// Build compiles fail links and per-node longest-suffix-match info (BFS), then
// packs everything into the cache-friendly node array.
func (ac *AhoCorasick) Build() {
	if ac.built {
		return
	}
	n := len(ac.children)
	fail := make([]int32, n)
	match := make([]int32, n)
	for i := range match {
		match[i] = noMatch
	}
	for i := range ac.rootNext {
		ac.rootNext[i] = -1
	}

	// CSR edge arrays: prefix-sum offsets, then sorted labels + children
	firstEdge := make([]int32, n+1)
	totalEdges := 0
	for _, m := range ac.children {
		totalEdges += len(m)
	}
	edgeByte := make([]byte, totalEdges)
	edgeChild := make([]int32, totalEdges)
	off := int32(0)
	var bs []byte
	for node, m := range ac.children {
		firstEdge[node] = off
		if len(m) == 0 {
			continue
		}
		bs = bs[:0]
		for b := range m {
			bs = append(bs, b)
		}
		slices.Sort(bs)
		for _, b := range bs {
			edgeByte[off] = b
			edgeChild[off] = m[b]
			off++
		}
	}
	firstEdge[n] = off

	// next returns the child of node on byte b, or -1
	next := func(node int32, b byte) int32 {
		if node == 0 {
			return ac.rootNext[b]
		}
		return bsearchEdge(edgeByte, edgeChild, firstEdge[node], firstEdge[node+1], b)
	}

	// BFS fail links
	queue := make([]int32, 0, n)
	for e := firstEdge[0]; e < firstEdge[1]; e++ {
		child := edgeChild[e]
		ac.rootNext[edgeByte[e]] = child
		fail[child] = 0
		queue = append(queue, child)
	}
	for qi := 0; qi < len(queue); qi++ {
		node := queue[qi]
		// longest pattern ending at this state: own pattern wins (it is the
		// full path, longer than any proper suffix from the fail chain)
		if ac.hasVal[node] {
			match[node] = int32(len(ac.patLen))
			ac.patLen = append(ac.patLen, ac.depth[node])
			ac.patVal = append(ac.patVal, ac.val[node])
		} else {
			// BFS is depth-ordered and fail always points shallower, so the
			// fail state's match is already final here
			match[node] = match[fail[node]]
		}
		for e := firstEdge[node]; e < firstEdge[node+1]; e++ {
			b := edgeByte[e]
			child := edgeChild[e]
			f := fail[node]
			for {
				if nxt := next(f, b); nxt >= 0 {
					fail[child] = nxt
					break
				}
				if f == 0 {
					fail[child] = 0
					break
				}
				f = fail[f]
			}
			queue = append(queue, child)
		}
	}

	ac.compile(n, firstEdge, edgeByte, edgeChild, fail, match)
	ac.children = nil
	ac.depth, ac.val, ac.hasVal = nil, nil, nil
	ac.built = true
}

// compile packs the CSR scaffolding into the runtime node array, spilling the
// states whose edges do not fit acNode.edge into wideByte/wideChild.
//
// State ids keep their insertion order on purpose. That order lays each
// pattern's chain out consecutively, so walking a pattern streams the node
// array; renumbering into BFS order splits every chain across depth bands and
// costs roughly 7x.
func (ac *AhoCorasick) compile(n int, firstEdge []int32, edgeByte []byte, edgeChild, fail, match []int32) {
	spills := func(node int) bool {
		d := firstEdge[node+1] - firstEdge[node]
		return d >= 2 || (d == 1 && edgeChild[firstEdge[node]] != int32(node)+1)
	}
	wideBytes, wideKids := int32(0), int32(0)
	for node := range n {
		if spills(node) {
			d := firstEdge[node+1] - firstEdge[node]
			wideBytes += d + 1 // run-length prefix
			wideKids += d + 1
		}
	}

	ac.nodes = make([]acNode, n)
	ac.wideByte = make([]byte, 0, wideBytes+swarPad)
	ac.wideChild = make([]int32, 0, wideKids)
	for node := range n {
		start, end := firstEdge[node], firstEdge[node+1]
		nd := acNode{fail: fail[node], match: match[node], edge: noEdge}
		switch {
		case end == start: // no edge
		case !spills(node): // lone edge to the next state
			nd.edge = int32(edgeByte[start])
		default:
			// the run-length prefix occupies a slot in both arrays so that a
			// label index is directly a child index
			ac.wideByte = append(ac.wideByte, byte(end-start-1))
			ac.wideChild = append(ac.wideChild, 0)
			ws := int32(len(ac.wideByte))
			ac.wideByte = append(ac.wideByte, edgeByte[start:end]...)
			ac.wideChild = append(ac.wideChild, edgeChild[start:end]...)
			nd.edge = wideTag(ws)
		}
		ac.nodes[node] = nd
	}
	ac.wideByte = append(ac.wideByte, make([]byte, swarPad)...)
}

// bsearchEdge finds byte b in the sorted labels[lo:hi] and returns the parallel
// children entry, or -1.
func bsearchEdge(labels []byte, children []int32, lo, hi int32, b byte) int32 {
	end := hi
	for lo < hi {
		mid := lo + (hi-lo)>>1
		if labels[mid] < b {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < end && labels[lo] == b {
		return children[lo]
	}
	return -1
}

const (
	swarOnes  = 0x0101010101010101
	swarHighs = 0x8080808080808080
	swarPad   = 8 // tail slack in wideByte so the last word load stays in bounds
)

// swarEdge finds b in labels[lo:hi] and returns the parallel children entry, or
// -1. It tests eight labels per word, so a wide state costs a couple of
// predictable iterations instead of one data-dependent branch per label.
// labels carries eight bytes of tail padding to keep the last word in bounds;
// a hit in that padding, or in the next state's labels, lands at k >= hi.
func swarEdge(labels []byte, children []int32, lo, hi int32, b byte) int32 {
	bcast := uint64(b) * swarOnes
	for i := lo; i < hi; i += 8 {
		v := binary.LittleEndian.Uint64(labels[i:]) ^ bcast
		if z := (v - swarOnes) &^ v & swarHighs; z != 0 {
			// borrows only propagate up, so the lowest flagged byte is a real hit
			if k := i + int32(bits.TrailingZeros64(z)>>3); k < hi {
				return children[k]
			}
			return -1
		}
	}
	return -1
}

// measured crossover: word-at-a-time wins to fanout 64, binary search past 128
const wideBsearchMin = 64

// ACMatcher is a per-goroutine matcher over a shared AhoCorasick automaton.
// It caches per-position automaton states of the previous word: the state
// after j bytes depends only on those bytes, so for a word sharing a prefix
// with its predecessor (sorted streams) the scan resumes at the first
// differing byte.
type ACMatcher struct {
	ac      *AhoCorasick
	matches Matches
	prev    []byte
	states  []int32 // states[j] = automaton state after consuming data[:j+1]
}

func NewACMatcher(ac *AhoCorasick) *ACMatcher {
	ac.Build()
	return &ACMatcher{ac: ac}
}

// FindLongestMatches returns the maximal pattern matches in data: sorted by
// Start, End strictly increasing, no match contained in another.
func (m *ACMatcher) FindLongestMatches(data []byte) []Match {
	ac := m.ac
	n := len(data)
	k := 0
	maxK := min(len(m.prev), n)
	for k < maxK && m.prev[k] == data[k] {
		k++
	}
	if cap(m.states) < n {
		states := make([]int32, n+64)
		copy(states, m.states[:k])
		m.states = states[:n]
	} else {
		m.states = m.states[:n]
	}

	nodes := ac.nodes
	rootNext := &ac.rootNext
	wideByte := ac.wideByte
	wideChild := ac.wideChild
	patLen := ac.patLen
	patVal := ac.patVal
	states := m.states[:n]
	out := m.matches[:0]

	// Match emission is fused into the scan below rather than run as a second
	// pass over states: it reuses the just-loaded nodes[cur] line and skips a
	// re-read of states. The prefix region [0,k) has no fresh scan (states
	// carried over from the previous word) so it emits on its own here.
	for j := range k {
		st := states[j]
		if mi := nodes[st].match; mi >= 0 {
			out = appendLongest(out, j+1-int(patLen[mi]), j+1, patVal[mi])
		}
	}

	cur := int32(0)
	if k > 0 {
		cur = states[k-1]
	}
	for j := k; j < n; j++ {
		b := data[j]
		for {
			if cur == 0 {
				if nx := rootNext[b]; nx >= 0 {
					cur = nx
				}
				break
			}
			nd := &nodes[cur]
			e := nd.edge
			if e >= 0 { // lone edge to the next state
				if byte(e) == b {
					cur++
					break
				}
				cur = nd.fail
				continue
			}
			if e < noEdge { // spilled run
				lo := wideStart(e)
				hi := lo + int32(wideByte[lo-1]) + 1
				var found int32
				if hi-lo > wideBsearchMin {
					found = bsearchEdge(wideByte, wideChild, lo, hi, b)
				} else {
					found = swarEdge(wideByte, wideChild, lo, hi, b)
				}
				if found >= 0 {
					cur = found
					break
				}
			}
			cur = nd.fail
		}
		states[j] = cur
		if mi := nodes[cur].match; mi >= 0 {
			out = appendLongest(out, j+1-int(patLen[mi]), j+1, patVal[mi])
		}
	}
	m.prev = append(m.prev[:0], data...)
	m.matches = out
	return out
}

// appendLongest drops previous matches contained in [start, end) and appends it.
func appendLongest(out Matches, start, end int, val any) Matches {
	for len(out) > 0 && out[len(out)-1].Start >= start {
		out = out[:len(out)-1]
	}
	return append(out, Match{Start: start, End: end, Val: val})
}
