// Copyright 2024 The Erigon Authors
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

package btindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

// nolint
type indexSeeker interface {
	WarmUp(g *seg.Reader) error
	Get(g *seg.Reader, key []byte) (k []byte, found bool, di uint64, err error)
	//seekInFiles(g *seg.Reader, key []byte) (indexSeekerIterator, error)
	Seek(g *seg.Reader, seek []byte) (k []byte, di uint64, found bool, err error)
}

// nolint
type indexSeekerIterator interface {
	Next() bool
	Di() uint64
	KVFromGetter(g *seg.Reader) ([]byte, []byte, error)
}

type dataLookupFunc func(di uint64, g *seg.Reader) ([]byte, []byte, uint64, error)

// M limits amount of child for tree node.
func NewBpsTree(kv *seg.Reader, offt *eliasfano32.EliasFano, M uint64, dataLookup dataLookupFunc) *BpsTree {
	bt := &BpsTree{M: M, offt: offt, dataLookupFunc: dataLookup}
	if err := bt.WarmUp(kv); err != nil {
		panic(err)
	}
	return bt
}

// "assert key behind offset == to stored key in bt"
var envAssertBTKeys = dbg.EnvBool("BT_ASSERT_OFFSETS", false)

func NewBpsTreeWithNodes(kv *seg.Reader, offt *eliasfano32.EliasFano, M uint64, dataLookup dataLookupFunc, keysBlob []byte, nodeOfft []uint64, nodeStride uint64) *BpsTree {
	bt := &BpsTree{M: M, offt: offt, dataLookupFunc: dataLookup, keysBlob: keysBlob, nodeOfft: nodeOfft, nodeStride: nodeStride}
	if envAssertBTKeys {
		for i := range nodeOfft {
			if cmp := bt.compareKey(kv, bt.nodeKey(i), bt.nodeDi(i)); cmp != 0 {
				panic(fmt.Errorf("key mismatch at di=%d i=%d cmp=%d", bt.nodeDi(i), i, cmp))
			}
			kv.Skip() // skip value
		}
	}
	return bt
}

type BpsTree struct {
	offt *eliasfano32.EliasFano // ef with offsets to key/vals

	// pivot cache: keysBlob holds [keyLen:u16][key] records (mmap-backed on-disk,
	// heap for WarmUp); nodeOfft[i] is record i's offset and di is derived as i*nodeStride.
	keysBlob   []byte
	nodeOfft   []uint64
	nodeStride uint64

	M     uint64 // limit on amount of 'children' for node
	trace bool

	dataLookupFunc dataLookupFunc
	cursorGetter   cursorGetter
}

func (b *BpsTree) numNodes() int { return len(b.nodeOfft) }

// nodeKey returns pivot i's key without copying (points into keysBlob).
func (b *BpsTree) nodeKey(i int) []byte {
	off := b.nodeOfft[i]
	l := uint64(binary.BigEndian.Uint16(b.keysBlob[off:]))
	return b.keysBlob[off+2 : off+2+l]
}

func (b *BpsTree) nodeDi(i int) uint64 { return uint64(i) * b.nodeStride }

// compareKey resets g to the offset of item di and compares key against the file key.
// Returns Compare(key, fileKey): 0 on match, <0 if key < fileKey, >0 if key > fileKey.
// On match, g is advanced past the key (ready to read value). On mismatch, g position is reset.
// Panics if di >= offt.Count().
func (b *BpsTree) compareKey(g *seg.Reader, key []byte, di uint64) int {
	if di >= b.offt.Count() {
		panic(fmt.Errorf("compareKey: di=%d >= count=%d, file: %s", di, b.offt.Count(), g.FileName()))
	}
	g.Reset(b.offt.Get(di))
	return g.MatchCmp(key)
}

type cursorGetter func(k, v []byte, di uint64, g *seg.Reader) *Cursor

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

//// If data[i] == key, returns 0 (equal) and value, nil err
//// if data[i] <> key, returns comparation result and nil value and error -- to be able to compare later
//func (b *BpsTree) matchKeyValue(g ArchiveGetter, i uint64, key []byte) (int, []byte, error) {
//	if i >= b.offt.Count() {
//		return 0, nil, ErrBtIndexLookupBounds
//	}
//	if b.trace {
//		fmt.Printf("match %d-%x count %d\n", i, key, b.offt.Count())
//	}
//	g.Reset(b.offt.Get(i))
//	buf, _ := g.Next(nil)
//	if !bytes.Equal(buf, key) {
//		return bytes.Compare(buf, key), nil, nil
//	}
//	val, _ := g.Next(nil)
//	return 0, val, nil
//}
//
//func (b *BpsTree) lookupKeyWGetter(g ArchiveGetter, i uint64) ([]byte, uint64) {
//	if i >= b.offt.Count() {
//		return nil, 0
//	}
//	o := b.offt.Get(i)
//	g.Reset(o)
//	buf, _ := g.Next(nil)
//	return buf, o
//}

type Node struct {
	key []byte
}

// Encode writes the node key length-prefixed (reusing headerBuf, len >= 2, to
// avoid a per-node alloc). di is not stored: node i is the i-th kept key, di=i*M.
func (n Node) Encode(w io.Writer, headerBuf []byte) error {
	if len(n.key) > math.MaxUint16 {
		return fmt.Errorf("node key too long: %d bytes", len(n.key))
	}
	if len(headerBuf) < 2 {
		return fmt.Errorf("node header buffer too small: %d bytes", len(headerBuf))
	}
	binary.BigEndian.PutUint16(headerBuf[:2], uint16(len(n.key)))
	if _, err := w.Write(headerBuf[:2]); err != nil {
		return err
	}
	_, err := w.Write(n.key)
	return err
}

// decodeNodes indexes count length-prefixed keys (no count prefix on disk — the
// caller derives count), returning each record's byte offset within data. di is
// not stored; node i has di = i*M, recomputed on read.
func decodeNodes(data []byte, count uint64) (nodeOfft []uint64, end int, err error) {
	if count > uint64(len(data))/2 { // each node is at least 2 bytes (keyLen)
		return nil, 0, fmt.Errorf("corrupt index: node count %d exceeds data size", count)
	}
	nodeOfft = make([]uint64, count)
	pos := 0
	for ni := range int(count) {
		if len(data)-pos < 2 {
			return nil, 0, fmt.Errorf("decode node %d: short buffer", ni)
		}
		nodeOfft[ni] = uint64(pos)
		l := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if len(data)-pos < l {
			return nil, 0, fmt.Errorf("decode node %d: short buffer", ni)
		}
		pos += l
	}
	return nodeOfft, pos, nil
}

// decodeListNodesV0 indexes the legacy node list ([di:u64][keyLen:u16][key] per
// node), returning each key record's offset past the on-disk di. di is derived
// as i*stride; stride comes from the stored di, validated to be the arithmetic
// progression 0,stride,2*stride,... so a wrong open-time M or a corrupt file is
// rejected rather than silently mis-derived.
func decodeListNodesV0(data []byte) (nodeOfft []uint64, stride uint64, end int, err error) {
	if len(data) < 8 {
		return nil, 0, 0, fmt.Errorf("truncated index: need 8 bytes for node count, got %d", len(data))
	}
	count := binary.BigEndian.Uint64(data[:8])
	if count > uint64(len(data)-8)/10 { // each node is at least 10 bytes (di+keyLen)
		return nil, 0, 0, fmt.Errorf("corrupt index: node count %d exceeds data size", count)
	}
	nodeOfft = make([]uint64, count)
	pos := 8
	for ni := range int(count) {
		if len(data)-pos < 10 {
			return nil, 0, 0, fmt.Errorf("decode node %d: short buffer", ni)
		}
		di := binary.BigEndian.Uint64(data[pos : pos+8])
		switch ni {
		case 0:
			if di != 0 {
				return nil, 0, 0, fmt.Errorf("corrupt v0 index: first node di=%d, want 0", di)
			}
		case 1:
			if di == 0 {
				return nil, 0, 0, fmt.Errorf("corrupt v0 index: second node has zero di (stride must be > 0)")
			}
			stride = di
		default:
			if di != uint64(ni)*stride { // di must follow the 0,stride,2*stride,... progression
				return nil, 0, 0, fmt.Errorf("corrupt v0 index: node %d di=%d, want %d", ni, di, uint64(ni)*stride)
			}
		}
		l := int(binary.BigEndian.Uint16(data[pos+8 : pos+10]))
		nodeOfft[ni] = uint64(pos + 8) // skip on-disk di; offset points at the keyLen prefix
		pos += 10
		if len(data)-pos < l {
			return nil, 0, 0, fmt.Errorf("decode node %d: short buffer", ni)
		}
		pos += l
	}
	return nodeOfft, stride, pos, nil
}

func (b *BpsTree) WarmUp(kv *seg.Reader) error {
	t := time.Now()
	N := b.offt.Count()
	if N == 0 {
		return nil
	}

	step := b.M
	if N < b.M { // cache all keys if less than M
		step = 1
	}
	b.nodeStride = step

	nodeCount := (N-1)/step + 1 // ceil(N/step), N>=1 here
	b.nodeOfft = make([]uint64, 0, nodeCount)
	blob := make([]byte, 0, nodeCount*(2+32)) // 32: rough avg key length
	if b.trace {
		fmt.Printf("WarmUp nodes %d N=%d M=%d\n", nodeCount, N, b.M)
	}

	var key []byte
	var hdr [2]byte
	for i := uint64(0); i < N; i += step {
		off := b.offt.Get(i)
		kv.Reset(off)
		key, _ = kv.Next(key[:0]) // read key only; reuse buffer to avoid allocs
		kv.Skip()                 // skip value — WarmUp only needs the key
		if len(key) > math.MaxUint16 {
			return fmt.Errorf("WarmUp: key at di=%d too long: %d bytes", i, len(key))
		}
		b.nodeOfft = append(b.nodeOfft, uint64(len(blob)))
		binary.BigEndian.PutUint16(hdr[:], uint16(len(key)))
		blob = append(blob, hdr[:]...)
		blob = append(blob, key...)
	}
	b.keysBlob = blob

	log.Root().Debug("WarmUp finished", "file", kv.FileName(), "M", b.M, "N", common.PrettyCounter(N),
		"cached", fmt.Sprintf("%d %.2f%%", b.numNodes(), 100*(float64(b.numNodes())/float64(N))),
		"cacheSize", datasize.ByteSize(len(b.keysBlob)+len(b.nodeOfft)*8).HR(), "fileSize", datasize.ByteSize(kv.Size()).HR(),
		"took", time.Since(t))
	return nil
}

// bs binary-searches the warmed-up pivot list for the [dl,dr) data-index window
// of key, plus klo/khi: the pivot keys bounding it, used by interpolation search.
func (b *BpsTree) bs(x []byte) (dl, dr uint64, klo, khi []byte) {
	dr = b.offt.Count()
	l, r := 0, b.numNodes() //nolint

	for l < r {
		m := (l + r) >> 1
		k := b.nodeKey(m)

		if b.trace {
			fmt.Printf("bs di:%d k:%x\n", b.nodeDi(m), k)
		}
		switch bytes.Compare(k, x) {
		case 0:
			di := b.nodeDi(m)
			return di, di, k, k
		case 1:
			r = m
			dr = b.nodeDi(m)
			khi = k
		case -1:
			l = m + 1
			dl = b.nodeDi(m)
			if dl < dr {
				dl++
			}
			klo = k
		}
	}
	return dl, dr, klo, khi
}

// Seek returns cursor pointing at first key which is >= seekKey.
// If key is nil, returns cursor with first key
// If found item.key has a prefix of key, returns item.key
// if key is greater than all keys, returns nil
func (b *BpsTree) Seek(g *seg.Reader, seekKey []byte) (cur *Cursor, err error) {
	//b.trace = true
	if b.trace {
		fmt.Printf("seek %x\n", seekKey)
	}
	cur = b.cursorGetter(nil, nil, 0, g)
	if len(seekKey) == 0 && b.offt.Count() > 0 {
		cur.Reset(0, g)
		return cur, nil
	}

	// check cached nodes and narrow roi
	l, r, _, _ := b.bs(seekKey) // l===r when key is found
	if l == r {
		// l can be Count() when seeking past the last key (insertion point);
		// Reset then reports out-of-bounds and Seek's contract is (nil, nil).
		if err = cur.Reset(l, g); err != nil {
			return nil, err
		}
		return cur, nil
	}

	// if b.trace {
	// 	fmt.Printf("pivot di:%d di(LR): [%d %d] k: %x found: %t\n", n.di, l, r, n.key, l == r)
	// 	defer func() { fmt.Printf("found=%t %x [%d %d]\n", bytes.Equal(key, seekKey), seekKey, l, r) }()
	// }
	var m uint64
	var cmp int

	for l < r {
		m = (l + r) >> 1
		if r-l <= DefaultBtreeStartSkip { // found small range, faster to scan now
			// m = l
			if cur.d == 0 {
				cur.resetNoRead(l, g)
			} else {
				cur.nextNoRead()
			}

			cur.key, _ = g.Next(cur.key[:0])

			if cmp = bytes.Compare(cur.key, seekKey); cmp < 0 {
				l++
				continue
			}

			cur.value, _ = g.Next(cur.value[:0])
			return cur, err
		}

		cmp = b.compareKey(g, seekKey, m)
		if b.trace {
			fmt.Printf("[%d %d] cmp: %d\n", l, r, cmp)
		}

		if cmp == 0 {
			break
		} else if cmp < 0 {
			r = m
		} else {
			l = m + 1
		}
	}

	if l == r {
		m = l
	}

	err = cur.Reset(m, g)
	if err != nil || bytes.Compare(cur.Key(), seekKey) < 0 {
		return nil, err
	}
	return cur, nil
}

// Get: returns for exact given key, value and offset in file where key starts
// If given key is nil, returns first key
// If no exact match found, returns nil values
func (b *BpsTree) Get(g *seg.Reader, key []byte) (v []byte, ok bool, offset uint64, err error) {
	if b.trace {
		fmt.Printf("get   %x\n", key)
	}
	if len(key) == 0 && b.offt.Count() > 0 {
		k0, v0, _, err := b.dataLookupFunc(0, g)
		if err != nil || k0 != nil {
			return nil, false, 0, err
		}
		return v0, true, 0, nil
	}

	l, r, klo, khi := b.bs(key) // l===r when key is found
	if b.trace {
		fmt.Printf("pivot di(LR): [%d %d] k: %x found: %t\n", l, r, key, l == r)
		defer func() { fmt.Printf("found %x [%d %d]\n", key, l, r) }()
	}

	var cmp int
	var m uint64
	// Interpolation search narrows the window with position estimates from the
	// bound keys; after BtInterpBudget probes fall back to binary. The final
	// small window is handed to the linear scan below either way.
	if BtInterp && len(klo) > 0 && len(khi) > 0 {
		probes := uint64(0)
		var kmArr, kloArr, khiArr [64]byte // stack; spills to heap only for keys > 64B
		km := kmArr[:0]
		for l < r && r-l > DefaultBtreeStartSkip {
			if probes >= BtInterpBudget {
				break
			}
			m = interpMid(key, klo, khi, l, r)
			probes++
			off := b.offt.Get(m)
			g.Reset(off)
			km, _ = g.Next(km[:0])
			cmp = bytes.Compare(key, km)
			if cmp == 0 {
				v, _ = g.Next(nil)
				return v, true, off, nil
			} else if cmp < 0 {
				r = m
				khi = append(khiArr[:0], km...)
			} else {
				l = m + 1
				klo = append(kloArr[:0], km...)
			}
		}
	}
	for l < r {
		m = (l + r) >> 1
		if r-l <= DefaultBtreeStartSkip {
			m = l
			if offset == 0 {
				offset = b.offt.Get(m)
				g.Reset(offset)
			}
			if cmp = g.MatchCmp(key); cmp < 0 {
				return nil, false, 0, err
			} else if cmp > 0 {
				// on non-match MatchCmp resets position; skip key+value to advance
				g.Skip()
				g.Skip()
				l++
				continue
			}
			v, _ = g.Next(nil)
			offset = b.offt.Get(m)
			return v, true, offset, nil
		}

		cmp = b.compareKey(g, key, m)
		if cmp == 0 {
			if !g.HasNext() {
				return nil, false, 0, fmt.Errorf("pair %d/%d key not found in %s", m, b.offt.Count(), g.FileName())
			}
			v, _ = g.Next(nil)
			return v, true, b.offt.Get(m), nil
		} else if cmp < 0 {
			r = m
		} else {
			l = m + 1
		}
		if b.trace {
			fmt.Printf("narrow [%d %d]\n", l, r)
		}
	}

	if l >= b.offt.Count() {
		return nil, false, 0, nil
	}
	cmp = b.compareKey(g, key, l)
	if cmp != 0 {
		return nil, false, 0, nil
	}
	if !g.HasNext() {
		return nil, false, 0, fmt.Errorf("pair %d/%d key not found in %s", l, b.offt.Count(), g.FileName())
	}
	v, _ = g.Next(nil)
	return v, true, b.offt.Get(l), nil
}

// interpMid estimates the index of searchKey within [l,r) by linear interpolation on
// the first 8 key bytes after the common prefix of the bound keys. Falls back to
// the binary midpoint when the span is degenerate; result is clamped to [l, r-1].
func interpMid(searchKey, klo, khi []byte, l, r uint64) uint64 {
	p := commonPrefixLen(klo, khi)
	a, hi, x := u64At(klo, p), u64At(khi, p), u64At(searchKey, p)
	if hi <= a {
		return (l + r) >> 1
	}
	f := float64(x-a) / float64(hi-a)
	m := l + uint64(f*float64(r-1-l)+0.5)
	if m < l {
		m = l
	} else if m >= r {
		m = r - 1
	}
	return m
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	n = min(n, len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// Left-aligned + zero-padded so the u64 keeps lexicographic key order across
// differing key lengths, which interpolation relies on (klo <= key <= khi).
func u64At(k []byte, p int) uint64 {
	if p+8 <= len(k) {
		return binary.BigEndian.Uint64(k[p:])
	}
	var x uint64
	for i := p; i < len(k); i++ {
		x |= uint64(k[i]) << (56 - 8*uint(i-p))
	}
	return x
}

func (b *BpsTree) Offsets() *eliasfano32.EliasFano { return b.offt }
func (b *BpsTree) Distances() (map[int]int, error) {
	distances := map[int]int{}
	var prev = -1
	it := b.Offsets().Iterator()
	for it.HasNext() {
		j, err := it.Next()
		if err != nil {
			return nil, err
		}
		if prev > 0 {
			dist := int(j) - prev
			if _, ok := distances[dist]; !ok {
				distances[dist] = 0
			}
			distances[dist]++
		}
		prev = int(j)
	}
	return distances, nil
}

func (b *BpsTree) Close() {
	b.keysBlob = nil
	b.nodeOfft = nil
	b.offt = nil
}
