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
// each input byte it needs this state's fail link, its match length and its edge,
// all indexed by the same jumpy state id. Packing them into one 16-byte record
// (4 per cache line) turns four random array loads into one.
//
// child tags the fanout in three disjoint ranges, so the single field both
// discriminates and carries its payload: noEdge means none; >=0 is the single
// edge to that state on byte(label); a wideTag value means the edges live in
// wideByte/wideChild[wideStart(child) : label].
type acNode struct {
	fail     int32
	matchLen int32 // longest pattern ending at this state (0 = none)
	child    int32
	label    int32
}

const noEdge = int32(-1)

// wideTag encodes a wide state's edge-range start into child, biased past noEdge
// so the single-edge (>=0), no-edge (-1) and wide (<=-2) ranges stay disjoint;
// wideStart inverts it.
func wideTag(start int32) int32   { return -start - 2 }
func wideStart(child int32) int32 { return -child - 2 }

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
	wideByte  []byte  // sorted edge labels of fanout>=2 states, concatenated
	wideChild []int32 // child states, parallel to wideByte
	matchVal  []any
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
	matchLen := make([]int32, n)
	ac.matchVal = make([]any, n)
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
			matchLen[node] = ac.depth[node]
			ac.matchVal[node] = ac.val[node]
		} else {
			f := fail[node]
			matchLen[node] = matchLen[f]
			ac.matchVal[node] = ac.matchVal[f]
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

	ac.compile(n, firstEdge, edgeByte, edgeChild, fail, matchLen)
	ac.children = nil
	ac.depth, ac.val, ac.hasVal = nil, nil, nil
	ac.built = true
}

// compile packs the CSR scaffolding into the runtime node array, spilling the
// rare fanout>=2 states into wideByte/wideChild.
func (ac *AhoCorasick) compile(n int, firstEdge []int32, edgeByte []byte, edgeChild, fail, matchLen []int32) {
	ac.nodes = make([]acNode, n)
	wide := int32(0)
	for node := range n {
		if d := firstEdge[node+1] - firstEdge[node]; d >= 2 {
			wide += d
		}
	}
	ac.wideByte = make([]byte, 0, wide)
	ac.wideChild = make([]int32, 0, wide)
	for node := range n {
		start, end := firstEdge[node], firstEdge[node+1]
		nd := acNode{fail: fail[node], matchLen: matchLen[node], child: noEdge}
		switch end - start {
		case 0: // no edge
		case 1: // single edge
			nd.child = edgeChild[start]
			nd.label = int32(edgeByte[start])
		default: // fanout >= 2: spill to the wide arrays
			ws := int32(len(ac.wideByte))
			ac.wideByte = append(ac.wideByte, edgeByte[start:end]...)
			ac.wideChild = append(ac.wideChild, edgeChild[start:end]...)
			nd.child = wideTag(ws)
			nd.label = int32(len(ac.wideByte))
		}
		ac.nodes[node] = nd
	}
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

// linear scan beats binary search over a wide state's edges up to this fanout
const wideLinearScanMax = 16

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
	matchVal := ac.matchVal
	states := m.states
	out := m.matches[:0]

	// Match emission is fused into the scan below rather than run as a second
	// pass over states: it reuses the just-loaded nodes[cur] line and skips a
	// re-read of states. The prefix region [0,k) has no fresh scan (states
	// carried over from the previous word) so it emits on its own here.
	for j := range k {
		st := states[j]
		if ml := nodes[st].matchLen; ml != 0 {
			out = appendLongest(out, j+1-int(ml), j+1, matchVal[st])
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
			c := nd.child
			if c >= 0 { // single edge
				if byte(nd.label) == b {
					cur = c
					break
				}
				cur = nd.fail
				continue
			}
			if c < noEdge { // fanout >= 2
				lo, hi := wideStart(c), nd.label
				found := int32(-1)
				if hi-lo > wideLinearScanMax {
					found = bsearchEdge(wideByte, wideChild, lo, hi, b)
				} else {
					for i := lo; i < hi; i++ {
						if wideByte[i] == b {
							found = wideChild[i]
							break
						}
						if wideByte[i] > b {
							break
						}
					}
				}
				if found >= 0 {
					cur = found
					break
				}
			}
			cur = nd.fail
		}
		states[j] = cur
		if ml := nodes[cur].matchLen; ml != 0 {
			out = appendLongest(out, j+1-int(ml), j+1, matchVal[cur])
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
