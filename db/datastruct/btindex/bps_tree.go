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
	"errors"
	"fmt"
	"io"
	"math"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

const minKeysForPrefixIndex = 60_000
const maxNodesPerBucket = 8

// prefixBucket holds the DI range and cached nodes for a single 2-byte prefix.
type prefixBucket struct {
	firstDI uint64 // DI of first key with this 2-byte prefix (MaxUint64 = empty)
	endDI   uint64 // DI past last key with this prefix (exclusive upper bound)
	nodes   []Node // cached keys for per-bucket binary search, max 8
}

// prefixIndex maps first 2 bytes of keys to prefixBucket structs.
// Each bucket tracks the [firstDI, endDI) range and embeds up to 8 cached nodes.
// L1 arrays aggregate bucket ranges for 1-byte prefix fallback.
type prefixIndex struct {
	buckets [65536]prefixBucket
	l1First [256]uint64 // min(firstDI) across buckets[b0<<8..b0<<8|0xFF]
	l1End   [256]uint64 // max(endDI) across buckets[b0<<8..b0<<8|0xFF]
}

func newPrefixIndex() *prefixIndex {
	p := new(prefixIndex)
	for i := range p.buckets {
		p.buckets[i].firstDI = math.MaxUint64
	}
	for i := range p.l1First {
		p.l1First[i] = math.MaxUint64
	}
	return p
}

// record updates the bucket for the 2-byte prefix of key with the given di.
// On first occurrence sets firstDI; always updates endDI = di + 1.
func (p *prefixIndex) record(key []byte, di uint64) {
	if len(key) < 2 {
		return
	}
	prefix := uint16(key[0])<<8 | uint16(key[1])
	b := &p.buckets[prefix]
	if di < b.firstDI {
		b.firstDI = di
	}
	if di+1 > b.endDI {
		b.endDI = di + 1
	}
}

// addNode appends a cached node to the bucket for the key's 2-byte prefix,
// up to maxNodesPerBucket.
func (p *prefixIndex) addNode(key []byte, node Node) {
	if len(key) < 2 {
		return
	}
	prefix := uint16(key[0])<<8 | uint16(key[1])
	b := &p.buckets[prefix]
	if len(b.nodes) < maxNodesPerBucket {
		b.nodes = append(b.nodes, node)
	}
}

// computeL1 aggregates bucket ranges into L1 summary arrays.
// For each first byte b0, scans buckets[b0<<8..b0<<8|0xFF] to find
// min non-sentinel firstDI and max endDI.
func (p *prefixIndex) computeL1() {
	for b0 := 0; b0 < 256; b0++ {
		minFirst := uint64(math.MaxUint64)
		maxEnd := uint64(0)
		base := uint16(b0) << 8
		for b1 := 0; b1 < 256; b1++ {
			bkt := &p.buckets[base|uint16(b1)]
			if bkt.firstDI < minFirst {
				minFirst = bkt.firstDI
			}
			if bkt.endDI > maxEnd {
				maxEnd = bkt.endDI
			}
		}
		p.l1First[b0] = minFirst
		p.l1End[b0] = maxEnd
	}
}

// lookup returns the [l, r) DI range for a key based on its prefix.
// len<1 returns full range, len==1 uses L1 tables, len>=2 uses bucket with L1 fallback.
// Returns (0, 0) for an empty (non-existent) prefix.
func (p *prefixIndex) lookup(key []byte, totalCount uint64) (l, r uint64) {
	if len(key) < 1 {
		return 0, totalCount
	}
	if len(key) == 1 {
		b0 := key[0]
		if p.l1First[b0] == math.MaxUint64 {
			return 0, 0
		}
		return p.l1First[b0], p.l1End[b0]
	}
	// len >= 2: try exact bucket
	prefix := uint16(key[0])<<8 | uint16(key[1])
	bkt := &p.buckets[prefix]
	if bkt.firstDI != math.MaxUint64 {
		return bkt.firstDI, bkt.endDI
	}
	// Fallback to L1
	b0 := key[0]
	if p.l1First[b0] == math.MaxUint64 {
		return 0, 0
	}
	return p.l1First[b0], p.l1End[b0]
}

// narrowWithNodes performs binary search over cached nodes in the bucket
// for the key's 2-byte prefix. Returns narrowed [l, r) range, or exact match.
// On exact match: found=true, exactDI=node.di. Otherwise narrows [l, r)
// from node boundaries. Returns (0, 0, 0, false) when no narrowing is possible.
func (p *prefixIndex) narrowWithNodes(key []byte) (l, r uint64, exactDI uint64, found bool) {
	if len(key) < 2 {
		return 0, 0, 0, false
	}
	prefix := uint16(key[0])<<8 | uint16(key[1])
	bkt := &p.buckets[prefix]
	if len(bkt.nodes) == 0 {
		return 0, 0, 0, false
	}

	l, r = bkt.firstDI, bkt.endDI
	lo, hi := 0, len(bkt.nodes)
	for lo < hi {
		mid := (lo + hi) >> 1
		cmp := bytes.Compare(bkt.nodes[mid].key, key)
		if cmp == 0 {
			return 0, 0, bkt.nodes[mid].di, true
		} else if cmp < 0 {
			lo = mid + 1
			if bkt.nodes[mid].di+1 > l {
				l = bkt.nodes[mid].di + 1
			}
		} else {
			hi = mid
			if bkt.nodes[mid].di < r {
				r = bkt.nodes[mid].di
			}
		}
	}
	return l, r, 0, false
}

type dataLookupFunc func(di uint64, g *seg.Reader) ([]byte, []byte, uint64, error)
type keyCmpFunc func(k []byte, di uint64, g *seg.Reader, copyBuf []byte) (int, []byte, error)

// M limits amount of child for tree node.
func NewBpsTree(kv *seg.Reader, offt *eliasfano32.EliasFano, M uint64, dataLookup dataLookupFunc, keyCmp keyCmpFunc) *BpsTree {
	bt := &BpsTree{M: M, offt: offt, dataLookupFunc: dataLookup, keyCmpFunc: keyCmp}
	if err := bt.WarmUp(kv); err != nil {
		panic(err)
	}
	return bt
}

// "assert key behind offset == to stored key in bt"
var envAssertBTKeys = dbg.EnvBool("BT_ASSERT_OFFSETS", false)

func NewBpsTreeWithNodes(kv *seg.Reader, offt *eliasfano32.EliasFano, M uint64, dataLookup dataLookupFunc, keyCmp keyCmpFunc, nodes []Node) *BpsTree {
	bt := &BpsTree{M: M, offt: offt, dataLookupFunc: dataLookup, keyCmpFunc: keyCmp}

	nsz := uint64(unsafe.Sizeof(Node{}))
	var cachedBytes uint64
	for i := 0; i < len(nodes); i++ {
		if envAssertBTKeys {
			eq, r, err := keyCmp(nodes[i].key, nodes[i].di, kv, nil)
			if err != nil {
				panic(err)
			}
			if eq != 0 {
				panic(fmt.Errorf("key mismatch %x %x %d %d", nodes[i].key, r, nodes[i].di, i))
			}
		}
		cachedBytes += nsz + uint64(len(nodes[i].key))

		nodes[i].off = offt.Get(nodes[i].di)
	}

	N := offt.Count()
	if N >= minKeysForPrefixIndex {
		// Large-file path: build prefix index from sequential scan, distribute nodes into buckets.
		bt.prefix = newPrefixIndex()

		var key []byte
		kv.Reset(0)
		for di := uint64(0); di < N; di++ {
			key, _ = kv.Next(key[:0])
			kv.Skip()
			bt.prefix.record(key, di)
		}

		for i := range nodes {
			bt.prefix.addNode(nodes[i].key, nodes[i])
		}
		bt.prefix.computeL1()
	} else {
		// Small-file path: keep nodes in mx as before.
		bt.mx = nodes
	}
	return bt
}

type BpsTree struct {
	offt   *eliasfano32.EliasFano // ef with offsets to key/vals
	mx     []Node
	prefix *prefixIndex // 2-level prefix index for O(1) range narrowing; nil when N < minKeysForPrefixIndex
	M      uint64       // limit on amount of 'children' for node
	trace  bool

	dataLookupFunc dataLookupFunc
	keyCmpFunc     keyCmpFunc
	cursorGetter   cursorGetter
}

type cursorGetter func(k, v []byte, di uint64, g *seg.Reader) *Cursor

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

type Node struct {
	key []byte
	off uint64 // offset in kv file to key
	di  uint64 // key ordinal number in kv
}

func encodeListNodes(nodes []Node, w io.Writer) error {
	numBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(numBuf, uint64(len(nodes)))
	if _, err := w.Write(numBuf); err != nil {
		return err
	}

	for ni := 0; ni < len(nodes); ni++ {
		if _, err := w.Write(nodes[ni].Encode()); err != nil {
			return err
		}
	}
	return nil
}

func decodeListNodes(data []byte) ([]Node, error) {
	count := binary.BigEndian.Uint64(data[:8])
	nodes := make([]Node, count)
	pos := 8
	for ni := 0; ni < int(count); ni++ {
		dp, err := nodes[ni].Decode(data[pos:])
		if err != nil {
			return nil, fmt.Errorf("decode node %d: %w", ni, err)
		}
		pos += int(dp)
	}
	return nodes, nil
}

func (n Node) Encode() []byte {
	buf := make([]byte, 8+2+len(n.key))
	binary.BigEndian.PutUint64(buf[:8], n.di)
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(n.key)))
	copy(buf[10:], n.key)
	return buf
}

func (n *Node) Decode(buf []byte) (uint64, error) {
	if len(buf) < 10 {
		return 0, errors.New("short buffer (less than 10b)")
	}
	n.di = binary.BigEndian.Uint64(buf[:8])
	l := int(binary.BigEndian.Uint16(buf[8:10]))
	if len(buf) < 10+l {
		return 0, errors.New("short buffer")
	}
	n.key = buf[10 : 10+l]
	return uint64(10 + l), nil
}

func (b *BpsTree) WarmUp(kv *seg.Reader) (err error) {
	t := time.Now()
	N := b.offt.Count()
	if N == 0 {
		return nil
	}

	step := b.M
	if N < b.M { // cache all keys if less than M
		step = 1
	}

	if N >= minKeysForPrefixIndex {
		b.prefix = newPrefixIndex()
	}

	cachedBytes := uint64(0)
	nsz := uint64(unsafe.Sizeof(Node{}))
	var key []byte
	var cachedCount int

	if b.prefix != nil {
		// Large-file path: sequential scan, record all keys in prefix index,
		// embed every M-th key as a node in its prefix bucket.
		kv.Reset(0)
		nextCache := step - 1
		for di := uint64(0); di < N; di++ {
			key, _ = kv.Next(key[:0])
			kv.Skip()

			b.prefix.record(key, di)

			if di == nextCache {
				node := Node{off: b.offt.Get(di), key: common.Copy(key), di: di}
				b.prefix.addNode(key, node)
				cachedBytes += nsz + uint64(len(key))
				cachedCount++
				nextCache += step
			}
		}
		b.prefix.computeL1()
	} else {
		// Small-file path: read only every step-th key via random access, populate mx
		b.mx = make([]Node, 0, N/b.M)
		for i := step; i < N; i += step {
			di := i - 1
			_, key, err = b.keyCmpFunc(nil, di, kv, key[:0])
			if err != nil {
				return err
			}
			b.mx = append(b.mx, Node{off: b.offt.Get(di), key: common.Copy(key), di: di})
			cachedBytes += nsz + uint64(len(key))
			cachedCount++
		}
	}

	args := []interface{}{
		"file", kv.FileName(), "M", b.M, "N", common.PrettyCounter(N),
		"cached", fmt.Sprintf("%d %.2f%%", cachedCount, 100*(float64(cachedCount)/float64(N))),
		"cacheSize", datasize.ByteSize(cachedBytes).HR(), "fileSize", datasize.ByteSize(kv.Size()).HR(),
		"took", time.Since(t),
	}
	if b.prefix != nil {
		args = append(args, "prefixIdx", true)
	}
	log.Root().Debug("WarmUp finished", args...)
	return nil
}

// bs performs pre-seach over warmed-up list of nodes to figure out left and right bounds on di for key
func (b *BpsTree) bs(x []byte) (n *Node, dl, dr uint64) {
	dr = b.offt.Count()
	m, l, r := 0, 0, len(b.mx) //nolint

	for l < r {
		m = (l + r) >> 1
		n = &b.mx[m]

		if b.trace {
			fmt.Printf("bs di:%d k:%x\n", n.di, n.key)
		}
		switch bytes.Compare(n.key, x) {
		case 0:
			return n, n.di, n.di
		case 1:
			r = m
			dr = n.di
		case -1:
			l = m + 1
			dl = n.di
			if dl < dr {
				dl++
			}
		}
	}
	return n, dl, dr
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

	// Start with full range
	l, r := uint64(0), b.offt.Count()

	if b.prefix != nil {
		// Large-file path: use prefix index for range lookup and cached node search
		pl, pr := b.prefix.lookup(seekKey, b.offt.Count())
		if pl != 0 || pr != 0 {
			l, r = pl, pr
		}
		nl, nr, exactDI, found := b.prefix.narrowWithNodes(seekKey)
		if found {
			err = cur.Reset(exactDI, g)
			return cur, err
		}
		if nl != 0 || nr != 0 {
			if nl > l {
				l = nl
			}
			if nr < r {
				r = nr
			}
		}
	} else if r-l > b.M {
		// Small-file path: use bs() for cached-node binary search
		n, bl, br := b.bs(seekKey)
		if bl == br {
			cur.Reset(n.di, g)
			return cur, nil
		}
		if bl > l {
			l = bl
		}
		if br < r {
			r = br
		}
	}

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

		cmp, cur.key, err = b.keyCmpFunc(seekKey, m, g, cur.key[:0])
		if err != nil {
			return nil, err
		}
		if b.trace {
			fmt.Printf("[%d %d] k: %x\n", l, r, cur.key)
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

	// Start with full range
	l, r := uint64(0), b.offt.Count()

	if b.prefix != nil {
		// Large-file path: use prefix index for range lookup and cached node search
		pl, pr := b.prefix.lookup(key, b.offt.Count())
		if pl == 0 && pr == 0 {
			return nil, false, 0, nil // no keys with this prefix
		}
		l, r = pl, pr
		nl, nr, exactDI, found := b.prefix.narrowWithNodes(key)
		if found {
			offset = b.offt.Get(exactDI)
			g.Reset(offset)
			g.Buf, _ = g.Next(g.Buf[:0]) // skip key
			v, _ = g.Next(nil)
			return v, true, offset, nil
		}
		if nl != 0 || nr != 0 {
			if nl > l {
				l = nl
			}
			if nr < r {
				r = nr
			}
		}
	} else if r-l > b.M {
		// Small-file path: use bs() for cached-node binary search
		n, bl, br := b.bs(key)
		if b.trace {
			fmt.Printf("pivot di: %d di(LR): [%d %d] k: %x found: %t\n", n.di, bl, br, n.key, bl == br)
			defer func() { fmt.Printf("found %x [%d %d]\n", key, l, r) }()
		}
		if bl > l {
			l = bl
		}
		if br < r {
			r = br
		}
	}

	var cmp int
	var m uint64
	for l < r {
		m = (l + r) >> 1
		if r-l <= DefaultBtreeStartSkip {
			m = l
			if offset == 0 {
				offset = b.offt.Get(m)
				g.Reset(offset)
			}
			g.Buf, _ = g.Next(g.Buf[:0])
			if cmp = bytes.Compare(g.Buf, key); cmp > 0 {
				return nil, false, 0, err
			} else if cmp < 0 {
				g.Skip()
				l++
				continue
			}
			v, _ = g.Next(nil)
			offset = b.offt.Get(m)
			return v, true, offset, nil
		}

		cmp, g.Buf, err = b.keyCmpFunc(key, m, g, g.Buf[:0])
		if err != nil {
			return nil, false, 0, err
		}
		if cmp == 0 {
			offset = b.offt.Get(m)
			if !g.HasNext() {
				return nil, false, 0, fmt.Errorf("pair %d/%d key not found in %s", m, b.offt.Count(), g.FileName())
			}
			v, _ = g.Next(nil)
			return v, true, offset, nil
		} else if cmp > 0 {
			r = m
		} else {
			l = m + 1
		}
		if b.trace {
			fmt.Printf("narrow [%d %d]\n", l, r)
		}
	}

	cmp, g.Buf, err = b.keyCmpFunc(key, l, g, g.Buf[:0])
	if err != nil || cmp != 0 {
		return nil, false, 0, err
	}
	if !g.HasNext() {
		return nil, false, 0, fmt.Errorf("pair %d/%d key not found in %s", l, b.offt.Count(), g.FileName())
	}
	v, _ = g.Next(nil)
	return v, true, b.offt.Get(l), nil
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
	b.mx = nil
	b.offt = nil
	b.prefix = nil // release ~2.5MB prefix index memory
}
