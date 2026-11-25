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

package solid

import (
	"bytes"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common/crypto"
)

const (
	cowMaxChunkHeight = 8

	// Progressive segment k >= 4 starts at leaf (4^k-1)/3, which is 85 mod 256.
	// Shifting every leaf by 171 puts each such segment on a chunk boundary, and
	// leaves 0-84 (segments 0-3) at the end of chunk 0.
	progressiveChunkShift = 171
	progressiveHeadLeaves = 85
	progressiveHeadSegs   = 4
)

// cowOwner is a pointer token: a chunk is writable only by the list whose
// current token it carries. A copy gives both lists fresh tokens, so every
// chunk becomes shared and is cloned on its first write.
type cowOwner struct{ _ uint64 }

type cowChunk struct {
	owner     *cowOwner
	data      []byte
	hashMu    sync.Mutex
	tree      *merkle_tree.MerkleTree
	head      *[progressiveHeadSegs][32]byte
	root      [32]byte
	rootValid atomic.Bool
}

type cowLayout struct {
	elemSize    int
	limitLeaves uint64
	h           int
	shift       int
}

func (l cowLayout) progressive() bool { return l.shift != 0 }

func (l cowLayout) chunkLeaves() int { return 1 << l.h }

func binaryCowLayout(elemSize, limit int) cowLayout {
	return cowLayout{
		elemSize:    elemSize,
		limitLeaves: uint64(limit),
		h:           min(cowMaxChunkHeight, int(merkle_tree.GetDepth(uint64(limit)))),
	}
}

func progressiveCowLayout(elemSize, limit int) cowLayout {
	return cowLayout{
		elemSize:    elemSize,
		limitLeaves: uint64(limit),
		h:           cowMaxChunkHeight,
		shift:       progressiveChunkShift,
	}
}

// cowList stores one fixed-size element per merkle leaf in chunks that copies
// share.
type cowList struct {
	layout cowLayout
	leafFn func(elem, out []byte)
	c      int
	l      int

	chunks  []atomic.Pointer[cowChunk]
	owner   atomic.Pointer[cowOwner]
	cloneMu sync.Mutex

	hashMu     sync.Mutex
	chunkRoots []byte
	top        []*merkle_tree.MerkleTree
	topRoot    [32]byte
	topValid   atomic.Bool
}

func (l *cowList) init(elemSize, c int, leafFn func(elem, out []byte)) {
	l.layout = binaryCowLayout(elemSize, c)
	l.leafFn = leafFn
	l.c = c
	l.owner.Store(new(cowOwner))
}

func newSlots(n int) []atomic.Pointer[cowChunk] {
	return make([]atomic.Pointer[cowChunk], n, n+16)
}

func (l *cowList) locate(e int) (chunk, offset int) {
	p := e + l.layout.shift
	return p >> l.layout.h, p & (1<<l.layout.h - 1)
}

func (l *cowList) elem(e int) []byte {
	ci, off := l.locate(e)
	size := l.layout.elemSize
	return l.chunks[ci].Load().data[off*size : (off+1)*size : (off+1)*size]
}

func chunkLeafFn(ch *cowChunk, elemSize int, leafFn func(elem, out []byte)) func(int, []byte) {
	return func(idx int, out []byte) {
		leafFn(ch.data[idx*elemSize:(idx+1)*elemSize], out)
	}
}

func (c *cowChunk) clone(owner *cowOwner, elemSize int, leafFn func(elem, out []byte)) *cowChunk {
	n := &cowChunk{owner: owner, data: slices.Clone(c.data)}
	c.hashMu.Lock()
	defer c.hashMu.Unlock()
	if c.tree != nil {
		n.tree = new(merkle_tree.MerkleTree)
		c.tree.CopyInto(n.tree)
		n.tree.SetComputeLeafFn(chunkLeafFn(n, elemSize, leafFn))
	}
	return n
}

func (l *cowList) privateChunk(ci int) *cowChunk {
	ch := l.chunks[ci].Load()
	if ch.owner == l.owner.Load() {
		return ch
	}
	l.cloneMu.Lock()
	defer l.cloneMu.Unlock()
	ch = l.chunks[ci].Load()
	if owner := l.owner.Load(); ch.owner != owner {
		ch = ch.clone(owner, l.layout.elemSize, l.leafFn)
		l.chunks[ci].Store(ch)
	}
	return ch
}

func (l *cowList) markDirty(ch *cowChunk, off int) {
	ch.rootValid.Store(false)
	if ch.tree != nil {
		ch.tree.MarkLeafAsDirty(off)
	}
	l.topValid.Store(false)
}

func (l *cowList) write(e int) []byte {
	ci, off := l.locate(e)
	ch := l.privateChunk(ci)
	l.markDirty(ch, off)
	size := l.layout.elemSize
	return ch.data[off*size : (off+1)*size]
}

func (l *cowList) growSlots() {
	n := len(l.chunks)
	if n < cap(l.chunks) {
		l.chunks = l.chunks[:n+1]
		return
	}
	grown := newSlots(n + 1)
	for i := range l.chunks {
		grown[i].Store(l.chunks[i].Load())
	}
	l.chunks = grown
}

func (l *cowList) append(elem []byte) {
	ci, off := l.locate(l.l)
	var ch *cowChunk
	if ci == len(l.chunks) {
		l.growSlots()
		ch = &cowChunk{owner: l.owner.Load(), data: make([]byte, l.layout.chunkLeaves()*l.layout.elemSize)}
		l.chunks[ci].Store(ch)
	} else {
		ch = l.privateChunk(ci)
		if ch.tree != nil {
			ch.tree.AppendLeaf()
		}
	}
	size := l.layout.elemSize
	copy(ch.data[off*size:(off+1)*size], elem)
	l.markDirty(ch, off)
	l.l++
}

func (l *cowList) readInto(dst []byte, from int) {
	size := l.layout.elemSize
	for len(dst) > 0 && from < l.l {
		ci, off := l.locate(from)
		n := min(l.layout.chunkLeaves()-off, len(dst)/size, l.l-from)
		copy(dst, l.chunks[ci].Load().data[off*size:(off+n)*size])
		dst = dst[n*size:]
		from += n
	}
}

func (l *cowList) encode(dst []byte) []byte {
	size := l.l * l.layout.elemSize
	dst = slices.Grow(dst, size)
	l.readInto(dst[len(dst):len(dst)+size], 0)
	return dst[:len(dst)+size]
}

func (l *cowList) build(layout cowLayout, n int, read func(dst []byte, from int)) []atomic.Pointer[cowChunk] {
	per, size := layout.chunkLeaves(), layout.elemSize
	count := 0
	if n > 0 {
		count = (n + layout.shift + per - 1) / per
	}
	chunks := newSlots(count)
	owner := l.owner.Load()
	slab := make([]byte, count*per*size)
	for ci := range chunks {
		end := (ci + 1) * per * size
		ch := &cowChunk{owner: owner, data: slab[ci*per*size : end : end]}
		first := ci*per - layout.shift
		skip := max(0, -first)
		if filled := min(per-skip, n-first-skip); read != nil && filled > 0 {
			read(ch.data[skip*size:(skip+filled)*size], first+skip)
		}
		chunks[ci].Store(ch)
	}
	return chunks
}

func (l *cowList) reset(n int, read func(dst []byte, from int)) {
	l.chunks = l.build(l.layout, n, read)
	l.l = n
	l.dropTop()
}

func (l *cowList) decode(buf []byte) {
	size := l.layout.elemSize
	l.reset(len(buf)/size, func(dst []byte, from int) { copy(dst, buf[from*size:]) })
}

func (l *cowList) clear() {
	for i := range l.chunks {
		l.chunks[i].Store(nil)
	}
	l.chunks = l.chunks[:0]
	l.l = 0
	l.dropTop()
}

func (l *cowList) setProgressive(enabled bool) {
	next := binaryCowLayout(l.layout.elemSize, l.c)
	if enabled {
		next = progressiveCowLayout(l.layout.elemSize, l.c)
	}
	if next == l.layout {
		return
	}
	if l.l > 0 {
		l.chunks = l.build(next, l.l, l.readInto)
	}
	l.layout = next
	l.dropTop()
}

func (l *cowList) copyTo(dst *cowList) {
	n := len(l.chunks)
	if cap(dst.chunks) < n {
		dst.chunks = newSlots(n)
	} else {
		for i := n; i < len(dst.chunks); i++ {
			dst.chunks[i].Store(nil)
		}
		dst.chunks = dst.chunks[:n]
	}
	for i := range l.chunks {
		dst.chunks[i].Store(l.chunks[i].Load())
	}
	dst.layout, dst.l, dst.c = l.layout, l.l, l.c

	l.hashMu.Lock()
	dst.copyTop(l)
	l.hashMu.Unlock()

	l.owner.Store(new(cowOwner))
	dst.owner.Store(new(cowOwner))
}

// shareChunks replaces l's full chunks that equal ref's chunk at the same index
// with ref's chunk, and returns how many chunks are shared.
func (l *cowList) shareChunks(ref *cowList) int {
	if l.layout != ref.layout {
		return 0
	}
	shared := 0
	for ci := range (min(l.l, ref.l) + l.layout.shift) / l.layout.chunkLeaves() {
		mine, theirs := l.chunks[ci].Load(), ref.chunks[ci].Load()
		if bytes.Equal(mine.data, theirs.data) {
			l.chunks[ci].Store(theirs)
			shared++
		}
	}
	if shared == 0 {
		return 0
	}
	ref.owner.Store(new(cowOwner))
	owner := l.owner.Load()
	// Each unshared chunk gets its own bytes: one of them would otherwise keep the whole decode slab alive.
	for ci := range l.chunks {
		if ch := l.chunks[ci].Load(); ci >= len(ref.chunks) || ch != ref.chunks[ci].Load() {
			l.chunks[ci].Store(ch.clone(owner, l.layout.elemSize, l.leafFn))
		}
	}
	return shared
}

func (l *cowList) fillRoot(ci int, ch *cowChunk) {
	ch.hashMu.Lock()
	defer ch.hashMu.Unlock()
	if ch.rootValid.Load() {
		return
	}
	size := l.layout.elemSize
	switch {
	case l.layout.progressive() && ci == 0:
		l.fillHead(ch)
	case l.layout.h == 0:
		l.leafFn(ch.data[:size], ch.root[:])
	default:
		if ch.tree == nil {
			per := l.layout.chunkLeaves()
			limit := uint64(per)
			ch.tree = new(merkle_tree.MerkleTree)
			ch.tree.Initialize(min(per, l.l+l.layout.shift-ci*per), l.layout.h, chunkLeafFn(ch, size, l.leafFn), &limit)
		}
		ch.root = ch.tree.ComputeRoot()
	}
	ch.rootValid.Store(true)
}

func (l *cowList) fillHead(ch *cowChunk) {
	size := l.layout.elemSize
	n := min(l.l, progressiveHeadLeaves)
	var leaves [progressiveHeadLeaves * 32]byte
	for i := range n {
		off := (progressiveChunkShift + i) * size
		l.leafFn(ch.data[off:off+size], leaves[i*32:(i+1)*32])
	}
	ch.head = new([progressiveHeadSegs][32]byte)
	start, capacity := 0, 1
	for k := range ch.head {
		if start < n {
			end := min(start+capacity, n)
			if err := merkle_tree.MerkleRootFromFlatLeavesWithLimit(leaves[start*32:end*32], ch.head[k][:], uint64(capacity)); err != nil {
				panic(err)
			}
		}
		start += capacity
		capacity *= 4
	}
}

func (l *cowList) fillCold(cold []int) {
	helpers := validatorRootWorkers.acquireHelpers(len(cold))
	if helpers == 0 {
		for _, ci := range cold {
			l.fillRoot(ci, l.chunks[ci].Load())
		}
		return
	}
	defer validatorRootWorkers.releaseHelpers(helpers)
	var next atomic.Int64
	work := func() {
		for {
			j := int(next.Add(1)) - 1
			if j >= len(cold) {
				return
			}
			l.fillRoot(cold[j], l.chunks[cold[j]].Load())
		}
	}
	var wg sync.WaitGroup
	wg.Add(helpers)
	for range helpers {
		go func() {
			defer wg.Done()
			work()
		}()
	}
	work()
	wg.Wait()
}

func (l *cowList) dropTop() {
	l.top = nil
	l.chunkRoots = l.chunkRoots[:0]
	l.topValid.Store(false)
}

func (l *cowList) copyTop(src *cowList) {
	l.chunkRoots = append(l.chunkRoots[:0], src.chunkRoots...)
	l.top = slices.Grow(l.top[:0], len(src.top))[:len(src.top)]
	for i, tree := range src.top {
		if l.top[i] == nil {
			l.top[i] = new(merkle_tree.MerkleTree)
		}
		tree.CopyInto(l.top[i])
		first, _ := l.topSpan(i)
		l.top[i].SetComputeLeafFn(l.topLeafFn(first))
	}
	l.topRoot = src.topRoot
	l.topValid.Store(src.topValid.Load())
}

func progressiveSegmentStart(k int) int { return (1<<(2*k) - 1) / 3 }

// topSpan returns the first chunk covered by top tree i and its capacity in chunks.
// A binary list has one tree over all chunks. A progressive list has one tree
// per segment k >= progressiveHeadSegs; the earlier segments live in chunk 0's head.
func (l *cowList) topSpan(i int) (first, capacity int) {
	h := l.layout.h
	if !l.layout.progressive() {
		return 0, 1 << (int(merkle_tree.GetDepth(l.layout.limitLeaves)) - h)
	}
	k := i + progressiveHeadSegs
	return (progressiveSegmentStart(k) + l.layout.shift) >> h, (1 << (2 * k)) >> h
}

func (l *cowList) topCount() int {
	switch {
	case l.l == 0:
		return 0
	case !l.layout.progressive():
		return 1
	}
	count := 0
	for k := progressiveHeadSegs; progressiveSegmentStart(k) < l.l; k++ {
		count++
	}
	return count
}

func (l *cowList) topLeafFn(first int) func(int, []byte) {
	return func(idx int, out []byte) {
		copy(out, l.chunkRoots[(first+idx)*32:(first+idx+1)*32])
	}
}

func (l *cowList) growTop(n, oldN int) {
	count := l.topCount()
	for len(l.top) < count {
		l.top = append(l.top, nil)
	}
	for i := range count {
		first, capacity := l.topSpan(i)
		have := min(max(oldN-first, 0), capacity)
		want := min(n-first, capacity)
		tree := l.top[i]
		if tree == nil {
			limit := uint64(capacity)
			tree = new(merkle_tree.MerkleTree)
			tree.Initialize(want, merkle_tree.OptimalMaxTreeCacheDepth, l.topLeafFn(first), &limit)
			tree.SetBaseLevel(l.layout.h)
			l.top[i] = tree
			continue
		}
		for j := have; j < want; j++ {
			tree.AppendLeaf()
		}
	}
}

func (l *cowList) markTopLeaf(ci int) {
	i := 0
	if l.layout.progressive() {
		if ci == 0 {
			return
		}
		first := ci<<l.layout.h - l.layout.shift
		i = (bits.Len(uint(3*first+1))-1)/2 - progressiveHeadSegs
	}
	first, _ := l.topSpan(i)
	l.top[i].MarkLeafAsDirty(ci - first)
}

// syncRoot records the chunk's current root and marks its top leaf dirty when
// the root differs from the cached one. Comparing roots, not validity flags,
// is what keeps a list correct when a sibling filled a shared chunk's root.
func (l *cowList) syncRoot(ci, oldN int, root *[32]byte) {
	cached := l.chunkRoots[ci*32 : (ci+1)*32]
	if ci < oldN && bytes.Equal(cached, root[:]) {
		return
	}
	copy(cached, root[:])
	if ci < oldN {
		l.markTopLeaf(ci)
	}
}

func (l *cowList) refreshTop() {
	n, oldN := len(l.chunks), len(l.chunkRoots)/32
	l.chunkRoots = slices.Grow(l.chunkRoots, n*32-len(l.chunkRoots))[:n*32]
	l.growTop(n, oldN)
	var cold []int
	for ci := range n {
		if ch := l.chunks[ci].Load(); ch.rootValid.Load() {
			l.syncRoot(ci, oldN, &ch.root)
		} else {
			cold = append(cold, ci)
		}
	}
	l.fillCold(cold)
	for _, ci := range cold {
		l.syncRoot(ci, oldN, &l.chunks[ci].Load().root)
	}
}

func (l *cowList) coreRoot() [32]byte {
	l.hashMu.Lock()
	defer l.hashMu.Unlock()
	if l.topValid.Load() {
		return l.topRoot
	}
	l.refreshTop()
	var core [32]byte
	switch {
	case l.layout.progressive():
		core = l.progressiveCore()
	case l.l == 0:
		core = merkle_tree.ZeroHashes[merkle_tree.GetDepth(l.layout.limitLeaves)]
	default:
		core = l.top[0].ComputeRoot()
	}
	l.topRoot = core
	l.topValid.Store(true)
	return core
}

func (l *cowList) progressiveCore() [32]byte {
	segments := make([][32]byte, 0, 24)
	start, capacity := 0, 1
	for k := 0; start < l.l; k++ {
		if k < progressiveHeadSegs {
			segments = append(segments, l.chunks[0].Load().head[k])
		} else {
			segments = append(segments, l.top[k-progressiveHeadSegs].ComputeRoot())
		}
		start += capacity
		capacity *= 4
	}
	var root [32]byte
	for _, segment := range slices.Backward(segments) {
		root = crypto.Sha256(segment[:], root[:])
	}
	return root
}
