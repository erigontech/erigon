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
	for ci := range chunks {
		ch := &cowChunk{owner: owner, data: make([]byte, per*size)}
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
	l.topValid.Store(false)
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
	l.topValid.Store(false)
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
	l.topValid.Store(false)
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
	dst.topRoot = l.topRoot
	dst.topValid.Store(l.topValid.Load())
	l.hashMu.Unlock()

	l.owner.Store(new(cowOwner))
	dst.owner.Store(new(cowOwner))
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

func (l *cowList) refreshChunkRoots() {
	n := len(l.chunks)
	l.chunkRoots = slices.Grow(l.chunkRoots[:0], n*32)[:n*32]
	var cold []int
	for ci := range n {
		if ch := l.chunks[ci].Load(); ch.rootValid.Load() {
			copy(l.chunkRoots[ci*32:], ch.root[:])
		} else {
			cold = append(cold, ci)
		}
	}
	l.fillCold(cold)
	for _, ci := range cold {
		copy(l.chunkRoots[ci*32:], l.chunks[ci].Load().root[:])
	}
}

func (l *cowList) coreRoot() ([32]byte, error) {
	l.hashMu.Lock()
	defer l.hashMu.Unlock()
	if l.topValid.Load() {
		return l.topRoot, nil
	}
	l.refreshChunkRoots()
	var core [32]byte
	var err error
	if l.layout.progressive() {
		core, err = l.progressiveCore()
	} else {
		core, err = l.binaryCore()
	}
	if err != nil {
		return [32]byte{}, err
	}
	l.topRoot = core
	l.topValid.Store(true)
	return core, nil
}

func (l *cowList) binaryCore() (core [32]byte, err error) {
	if l.l == 0 {
		return merkle_tree.ZeroHashes[merkle_tree.GetDepth(l.layout.limitLeaves)], nil
	}
	err = merkle_tree.MerkleRootFromFlatFromIntermediateLevelWithLimit(l.chunkRoots, core[:], int(l.layout.limitLeaves), l.layout.h)
	return core, err
}

func (l *cowList) progressiveCore() ([32]byte, error) {
	segments := make([][32]byte, 0, 24)
	start, capacity := 0, 1
	for k := 0; start < l.l; k++ {
		var segment [32]byte
		if k < progressiveHeadSegs {
			segment = l.chunks[0].Load().head[k]
		} else {
			end := min(start+capacity, l.l)
			first := (start + l.layout.shift) >> l.layout.h
			last := (end - 1 + l.layout.shift) >> l.layout.h
			if err := merkle_tree.MerkleRootFromFlatFromIntermediateLevelWithLimit(l.chunkRoots[first*32:(last+1)*32], segment[:], capacity, l.layout.h); err != nil {
				return [32]byte{}, err
			}
		}
		segments = append(segments, segment)
		start += capacity
		capacity *= 4
	}
	var root [32]byte
	for _, segment := range slices.Backward(segments) {
		root = crypto.Sha256(segment[:], root[:])
	}
	return root, nil
}
