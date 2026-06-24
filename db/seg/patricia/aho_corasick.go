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
	"sort"
)

// AhoCorasick is a byte-level multi-pattern automaton. Build it once from a
// pattern dictionary, then share it read-only across any number of ACMatcher
// instances (one per goroutine).
type AhoCorasick struct {
	// build-time trie
	children []map[byte]int32
	depth    []int32
	val      []any
	hasVal   []bool

	// compiled automaton
	rootNext  [256]int32 // dense transitions from root (-1 = none)
	edgeBytes [][]byte   // per-node sorted edge labels
	edgeTo    [][]int32  // per-node child indices, parallel to edgeBytes
	fail      []int32
	matchLen  []int32 // longest pattern ending at this node (0 = none)
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

// Build compiles fail links and per-node longest-suffix-match info (BFS).
func (ac *AhoCorasick) Build() {
	if ac.built {
		return
	}
	n := len(ac.children)
	ac.fail = make([]int32, n)
	ac.matchLen = make([]int32, n)
	ac.matchVal = make([]any, n)
	ac.edgeBytes = make([][]byte, n)
	ac.edgeTo = make([][]int32, n)
	for i := range ac.rootNext {
		ac.rootNext[i] = -1
	}

	// sorted edge arrays
	for node, m := range ac.children {
		if len(m) == 0 {
			continue
		}
		bs := make([]byte, 0, len(m))
		for b := range m {
			bs = append(bs, b)
		}
		sort.Slice(bs, func(i, j int) bool { return bs[i] < bs[j] })
		tos := make([]int32, len(bs))
		for i, b := range bs {
			tos[i] = m[b]
		}
		ac.edgeBytes[node] = bs
		ac.edgeTo[node] = tos
	}

	// BFS fail links
	queue := make([]int32, 0, n)
	for i, b := range ac.edgeBytes[0] {
		child := ac.edgeTo[0][i]
		ac.rootNext[b] = child
		ac.fail[child] = 0
		queue = append(queue, child)
	}
	for qi := 0; qi < len(queue); qi++ {
		node := queue[qi]
		// longest pattern ending at this state: own pattern wins (it is the
		// full path, longer than any proper suffix from the fail chain)
		if ac.hasVal[node] {
			ac.matchLen[node] = ac.depth[node]
			ac.matchVal[node] = ac.val[node]
		} else {
			f := ac.fail[node]
			ac.matchLen[node] = ac.matchLen[f]
			ac.matchVal[node] = ac.matchVal[f]
		}
		for i, b := range ac.edgeBytes[node] {
			child := ac.edgeTo[node][i]
			f := ac.fail[node]
			for {
				if nxt := ac.next(f, b); nxt >= 0 {
					ac.fail[child] = nxt
					break
				}
				if f == 0 {
					ac.fail[child] = 0
					break
				}
				f = ac.fail[f]
			}
			queue = append(queue, child)
		}
	}
	ac.children = nil // free build-time maps
	ac.built = true
}

// next returns the child of node on byte b, or -1.
func (ac *AhoCorasick) next(node int32, b byte) int32 {
	if node == 0 {
		return ac.rootNext[b]
	}
	bs := ac.edgeBytes[node]
	lo, hi := 0, len(bs)
	for lo < hi {
		mid := (lo + hi) >> 1
		if bs[mid] < b {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(bs) && bs[lo] == b {
		return ac.edgeTo[node][lo]
	}
	return -1
}

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
// Start, End strictly increasing, no match contained in another. Identical
// output to MatchFinder3.FindLongestMatches after deduplication.
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
		copy(states, m.states[:len(m.states)])
		m.states = states[:n]
	} else {
		m.states = m.states[:n]
	}
	cur := int32(0)
	if k > 0 {
		cur = m.states[k-1]
	}
	for j := k; j < n; j++ {
		b := data[j]
		for {
			if nxt := ac.next(cur, b); nxt >= 0 {
				cur = nxt
				break
			}
			if cur == 0 {
				break
			}
			cur = ac.fail[cur]
		}
		m.states[j] = cur
	}
	m.prev = append(m.prev[:0], data...)

	out := m.matches[:0]
	for j := 0; j < n; j++ {
		st := m.states[j]
		ml := ac.matchLen[st]
		if ml == 0 {
			continue
		}
		start := j + 1 - int(ml)
		// drop previous matches contained in this one
		for len(out) > 0 && out[len(out)-1].Start >= start {
			out = out[:len(out)-1]
		}
		out = append(out, Match{Start: start, End: j + 1, Val: ac.matchVal[st]})
	}
	m.matches = out
	return out
}
