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
	"errors"
	"fmt"
	"math"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

const maxNodesPerBucket = 8

type prefixBucket struct {
	firstDI uint64 // first key DI with this 2-byte prefix (MaxUint64 = empty)
	endDI   uint64 // DI past last key (exclusive upper bound)
	nodes   []Node // up to 8 cached keys for binary search within bucket
}

// PrefixIndex is a standalone search engine that replaces BpsTree.
// It builds per-prefix evenly-distributed cached nodes at open time from the .kv file,
// providing consistent narrowing across all key prefixes.
type PrefixIndex struct {
	buckets [65536]prefixBucket
	l1First [256]uint64 // min(firstDI) across buckets[b0<<8..b0<<8|0xFF]
	l1End   [256]uint64 // max(endDI) across buckets[b0<<8..b0<<8|0xFF]

	offt *eliasfano32.EliasFano // offset index (from .bt file)

	dataLookupFunc dataLookupFunc
	keyCmpFunc     keyCmpFunc
	cursorGetter   cursorGetter

	trace bool
}

// record updates the bucket for the given key's 2-byte prefix with the key's DI.
func (p *PrefixIndex) record(key []byte, di uint64) {
	if len(key) < 2 {
		return
	}
	prefix := uint16(key[0])<<8 | uint16(key[1])
	bucket := &p.buckets[prefix]
	if bucket.firstDI > di {
		bucket.firstDI = di
	}
	if bucket.endDI <= di {
		bucket.endDI = di + 1
	}
}

// computeL1 aggregates bucket ranges into L1 arrays for first-byte narrowing.
func (p *PrefixIndex) computeL1() {
	for i := range p.l1First {
		p.l1First[i] = math.MaxUint64
	}
	// l1End is zero-initialized which is correct (no entries)
	for prefix := 0; prefix < 65536; prefix++ {
		b := &p.buckets[prefix]
		if b.firstDI == math.MaxUint64 {
			continue
		}
		b0 := prefix >> 8
		if b.firstDI < p.l1First[b0] {
			p.l1First[b0] = b.firstDI
		}
		if b.endDI > p.l1End[b0] {
			p.l1End[b0] = b.endDI
		}
	}
}

// lookup returns the DI range [l, r) for the given key based on its prefix.
// Returns (0, 0) when no keys with the key's prefix (or first byte) exist.
func (p *PrefixIndex) lookup(key []byte) (l, r uint64) {
	count := p.offt.Count()
	if len(key) < 1 {
		return 0, count
	}

	b0 := key[0]
	if len(key) < 2 {
		if p.l1First[b0] == math.MaxUint64 {
			return 0, 0
		}
		return p.l1First[b0], p.l1End[b0]
	}

	prefix := uint16(b0)<<8 | uint16(key[1])
	bucket := &p.buckets[prefix]
	if bucket.firstDI != math.MaxUint64 {
		return bucket.firstDI, bucket.endDI
	}

	// exact bucket empty, fall back to L1
	if p.l1First[b0] == math.MaxUint64 {
		return 0, 0
	}
	return p.l1First[b0], p.l1End[b0]
}

// narrowWithNodes refines the [l, r) range using cached nodes in the key's prefix bucket.
// If an exact match is found among nodes, returns found=true and exactDI.
func (p *PrefixIndex) narrowWithNodes(key []byte, l, r uint64) (nl, nr, exactDI uint64, found bool) {
	if len(key) < 2 {
		return l, r, 0, false
	}
	prefix := uint16(key[0])<<8 | uint16(key[1])
	bucket := &p.buckets[prefix]
	if len(bucket.nodes) == 0 {
		return l, r, 0, false
	}

	// binary search over cached nodes
	lo, hi := 0, len(bucket.nodes)
	for lo < hi {
		mid := (lo + hi) >> 1
		cmp := bytes.Compare(bucket.nodes[mid].key, key)
		if cmp == 0 {
			return 0, 0, bucket.nodes[mid].di, true
		} else if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// lo is the insertion point: nodes[lo-1].key < key < nodes[lo].key
	if lo > 0 {
		if newL := bucket.nodes[lo-1].di + 1; newL > l {
			l = newL
		}
	}
	if lo < len(bucket.nodes) {
		if newR := bucket.nodes[lo].di; newR < r {
			r = newR
		}
	}

	return l, r, 0, false
}

// addNode adds a pre-built node to the appropriate prefix bucket.
func (p *PrefixIndex) addNode(n Node) {
	if len(n.key) < 2 {
		return
	}
	prefix := uint16(n.key[0])<<8 | uint16(n.key[1])
	bucket := &p.buckets[prefix]
	if len(bucket.nodes) < maxNodesPerBucket {
		bucket.nodes = append(bucket.nodes, n)
	}
}

// initBuckets sets all bucket firstDI to sentinel value.
func (p *PrefixIndex) initBuckets() {
	for i := range p.buckets {
		p.buckets[i].firstDI = math.MaxUint64
	}
}

// scanBucketRanges performs a sequential scan over the kv file to record per-prefix
// bucket ranges (firstDI, endDI) and optionally count keys per prefix.
// If counts is non-nil, it will be populated with key counts per prefix.
func (p *PrefixIndex) scanBucketRanges(kv *seg.Reader, counts []uint32) {
	var key []byte
	kv.Reset(0)
	for di := uint64(0); kv.HasNext(); di++ {
		key, _ = kv.Next(key[:0])
		kv.Skip()
		p.record(key, di)
		if counts != nil && len(key) >= 2 {
			prefix := uint16(key[0])<<8 | uint16(key[1])
			counts[prefix]++
		}
	}
}

// NewPrefixIndex builds a PrefixIndex by scanning the .kv file twice:
// Pass 1: count keys per prefix and record bucket ranges.
// Pass 2: select evenly-spaced nodes per prefix bucket.
func NewPrefixIndex(kv *seg.Reader, offt *eliasfano32.EliasFano, dataLookup dataLookupFunc, keyCmp keyCmpFunc) *PrefixIndex {
	p := &PrefixIndex{
		offt:           offt,
		dataLookupFunc: dataLookup,
		keyCmpFunc:     keyCmp,
	}
	p.initBuckets()

	count := offt.Count()
	if count == 0 {
		p.computeL1()
		return p
	}

	// Pass 1: sequential scan to record bucket ranges and count per prefix.
	counts := make([]uint32, 65536)
	p.scanBucketRanges(kv, counts)

	// Prepare node selection state per prefix.
	type prefixScan struct {
		seen    uint32
		curPick uint8
		nPicks  uint8
	}
	scanState := make([]prefixScan, 65536)

	for prefix := 0; prefix < 65536; prefix++ {
		c := counts[prefix]
		if c == 0 {
			continue
		}
		if c <= maxNodesPerBucket {
			scanState[prefix].nPicks = uint8(c)
		} else {
			scanState[prefix].nPicks = maxNodesPerBucket
		}
		p.buckets[prefix].nodes = make([]Node, 0, scanState[prefix].nPicks)
	}

	// Pass 2: sequential scan to pick evenly-spaced nodes.
	var key []byte
	kv.Reset(0)
	for di := uint64(0); kv.HasNext(); di++ {
		key, _ = kv.Next(key[:0])
		kv.Skip()

		if len(key) < 2 {
			continue
		}
		prefix := uint16(key[0])<<8 | uint16(key[1])
		s := &scanState[prefix]

		if s.curPick < s.nPicks {
			c := counts[prefix]
			var targetPos uint32
			if c <= maxNodesPerBucket {
				targetPos = uint32(s.seen)
			} else {
				targetPos = uint32(uint64(s.curPick) * uint64(c-1) / 7)
			}

			if uint32(s.seen) == targetPos {
				off := offt.Get(di)
				p.buckets[prefix].nodes = append(p.buckets[prefix].nodes, Node{
					key: common.Copy(key),
					off: off,
					di:  di,
				})
				s.curPick++
			}
		}
		s.seen++
	}

	p.computeL1()
	return p
}

// NewPrefixIndexWithNodes builds a PrefixIndex using pre-built nodes from a .bt file.
// It still scans .kv to establish bucket ranges, then distributes existing nodes into buckets.
func NewPrefixIndexWithNodes(kv *seg.Reader, offt *eliasfano32.EliasFano, dataLookup dataLookupFunc, keyCmp keyCmpFunc, nodes []Node) *PrefixIndex {
	p := &PrefixIndex{
		offt:           offt,
		dataLookupFunc: dataLookup,
		keyCmpFunc:     keyCmp,
	}
	p.initBuckets()

	count := offt.Count()
	if count == 0 {
		p.computeL1()
		return p
	}

	// Sequential scan to record bucket ranges.
	p.scanBucketRanges(kv, nil)

	// Distribute pre-built nodes into prefix buckets.
	for i := range nodes {
		nodes[i].off = offt.Get(nodes[i].di)
		p.addNode(nodes[i])
	}

	// Adaptive: if pre-built nodes are too sparse for good narrowing,
	// do a supplementary scan to ensure each non-empty bucket gets at least 1 node.
	// With M=256 and 65K buckets, pre-built nodes cover <6% of buckets.
	emptyNodeBuckets := 0
	for i := 0; i < 65536; i++ {
		if p.buckets[i].firstDI != math.MaxUint64 && len(p.buckets[i].nodes) == 0 {
			emptyNodeBuckets++
		}
	}
	if emptyNodeBuckets > 0 {
		// Supplementary pass: pick 1 node per empty bucket (middle key)
		counts := make([]uint32, 65536)
		p.scanBucketRanges(kv, counts) // re-scan to get counts
		var key []byte
		kv.Reset(0)
		seen := make([]uint32, 65536)
		for di := uint64(0); kv.HasNext(); di++ {
			key, _ = kv.Next(key[:0])
			kv.Skip()
			if len(key) < 2 {
				continue
			}
			prefix := uint16(key[0])<<8 | uint16(key[1])
			if len(p.buckets[prefix].nodes) > 0 {
				seen[prefix]++
				continue // already has nodes
			}
			seen[prefix]++
			// Pick middle key of bucket
			mid := counts[prefix] / 2
			if seen[prefix]-1 == mid {
				p.addNode(Node{
					key: common.Copy(key),
					di:  di,
					off: offt.Get(di),
				})
			}
		}
	}

	p.computeL1()
	return p
}

// Seek returns a cursor pointing at the first key >= seekKey.
// If seekKey is nil/empty, returns cursor at position 0.
// If seekKey is beyond all keys, returns nil.
func (p *PrefixIndex) Seek(g *seg.Reader, seekKey []byte) (*Cursor, error) {
	count := p.offt.Count()
	if count == 0 {
		return nil, nil
	}

	cur := p.cursorGetter(nil, nil, 0, g)

	if len(seekKey) == 0 {
		if err := cur.Reset(0, g); err != nil {
			cur.Close()
			return nil, err
		}
		return cur, nil
	}

	l, r := p.lookup(seekKey)
	if l == 0 && r == 0 {
		// prefix doesn't exist; for Seek, search full range
		r = count
	}

	// narrow with cached nodes
	nl, nr, exactDI, found := p.narrowWithNodes(seekKey, l, r)
	if found {
		if err := cur.Reset(exactDI, g); err != nil {
			cur.Close()
			return nil, err
		}
		return cur, nil
	}
	l, r = nl, nr

	var m uint64
	var cmp int
	var err error

	for l < r {
		m = (l + r) >> 1
		if r-l <= DefaultBtreeStartSkip {
			// small range, scan sequentially
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
			return cur, nil
		}

		cmp, cur.key, err = p.keyCmpFunc(seekKey, m, g, cur.key[:0])
		if err != nil {
			cur.Close()
			return nil, err
		}

		if cmp == 0 {
			break
		} else if cmp > 0 {
			r = m
		} else {
			l = m + 1
		}
	}

	if l == r {
		m = l
	}

	err = cur.Reset(m, g)
	if err != nil {
		cur.Close()
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return nil, nil
		}
		return nil, err
	}
	if bytes.Compare(cur.Key(), seekKey) < 0 {
		cur.Close()
		return nil, nil
	}
	return cur, nil
}

// Get returns the value for an exact key match.
// Returns (nil, false, 0, nil) if key is not found.
func (p *PrefixIndex) Get(g *seg.Reader, key []byte) (v []byte, ok bool, offset uint64, err error) {
	count := p.offt.Count()
	if count == 0 {
		return nil, false, 0, nil
	}

	if len(key) == 0 {
		k0, v0, _, e := p.dataLookupFunc(0, g)
		if e != nil || k0 != nil {
			return nil, false, 0, e
		}
		return v0, true, 0, nil
	}

	// Quick check: if the exact 2-byte prefix bucket is empty, key can't exist.
	if len(key) >= 2 {
		prefix := uint16(key[0])<<8 | uint16(key[1])
		if p.buckets[prefix].firstDI == math.MaxUint64 {
			return nil, false, 0, nil
		}
	}

	l, r := p.lookup(key)
	if l == 0 && r == 0 {
		return nil, false, 0, nil
	}

	// narrow with cached nodes
	nl, nr, exactDI, found := p.narrowWithNodes(key, l, r)
	if found {
		_, v, offset, err = p.dataLookupFunc(exactDI, g)
		if err != nil {
			return nil, false, 0, err
		}
		return v, true, offset, nil
	}
	l, r = nl, nr

	// Disk binary search in [l, r)
	var cmp int
	var m uint64
	for l < r {
		m = (l + r) >> 1
		if r-l <= DefaultBtreeStartSkip {
			m = l
			if offset == 0 {
				offset = p.offt.Get(m)
				g.Reset(offset)
			}
			g.Buf, _ = g.Next(g.Buf[:0])
			if cmp = bytes.Compare(g.Buf, key); cmp > 0 {
				return nil, false, 0, nil
			} else if cmp < 0 {
				g.Skip()
				l++
				continue
			}
			v, _ = g.Next(nil)
			offset = p.offt.Get(m)
			return v, true, offset, nil
		}

		cmp, g.Buf, err = p.keyCmpFunc(key, m, g, g.Buf[:0])
		if err != nil {
			return nil, false, 0, err
		}
		if cmp == 0 {
			offset = p.offt.Get(m)
			if !g.HasNext() {
				return nil, false, 0, fmt.Errorf("pair %d/%d key not found in %s", m, count, g.FileName())
			}
			v, _ = g.Next(nil)
			return v, true, offset, nil
		} else if cmp > 0 {
			r = m
		} else {
			l = m + 1
		}
	}

	cmp, g.Buf, err = p.keyCmpFunc(key, l, g, g.Buf[:0])
	if err != nil || cmp != 0 {
		return nil, false, 0, err
	}
	if !g.HasNext() {
		return nil, false, 0, fmt.Errorf("pair %d/%d key not found in %s", l, count, g.FileName())
	}
	v, _ = g.Next(nil)
	return v, true, p.offt.Get(l), nil
}

func (p *PrefixIndex) Offsets() *eliasfano32.EliasFano { return p.offt }

func (p *PrefixIndex) Distances() (map[int]int, error) {
	distances := map[int]int{}
	prev := -1
	it := p.offt.Iterator()
	for it.HasNext() {
		j, err := it.Next()
		if err != nil {
			return nil, err
		}
		if prev > 0 {
			dist := int(j) - prev
			distances[dist]++
		}
		prev = int(j)
	}
	return distances, nil
}

func (p *PrefixIndex) Close() {
	if p == nil {
		return
	}
	for i := range p.buckets {
		p.buckets[i].nodes = nil
	}
	p.offt = nil
}
