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

package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
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

func NewBpsTreeWithNodes(kv *seg.Reader, offt *eliasfano32.EliasFano, M uint64, dataLookup dataLookupFunc, keyCmp keyCmpFunc, nodes []*Node) *BpsTree {
	bt := &BpsTree{M: M, offt: offt, dataLookupFunc: dataLookup, keyCmpFunc: keyCmp, mx: nodes}

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

	return bt
}

type BpsTree struct {
	offt  *eliasfano32.EliasFano // ef with offsets to key/vals
	mx    []*Node
	M     uint64 // limit on amount of 'children' for node
	trace bool

	dataLookupFunc dataLookupFunc
	keyCmpFunc     keyCmpFunc
	cursorGetter   cursorGetter
}

type cursorGetter func(k, v []byte, di uint64, g *seg.Reader) *Cursor

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

// Di returns ordinal number of current key in the tree
func (it *BpsTreeIterator) Di() uint64 {
	return it.i
}

func (it *BpsTreeIterator) KVFromGetter(g *seg.Reader) ([]byte, []byte, error) {
	if it == nil {
		return nil, nil, errors.New("iterator is nil")
	}
	//fmt.Printf("kv from %p getter %p tree %p offt %d\n", it, g, it.t, it.i)
	k, v, _, err := it.t.dataLookupFunc(it.i, g)
	if err != nil {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return k, v, nil
}

func (it *BpsTreeIterator) Next() bool {
	if it.i+1 == it.t.offt.Count() {
		return false
	}
	it.i++
	return true
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

func decodeListNodes(data []byte) ([]*Node, error) {
	count := binary.BigEndian.Uint64(data[:8])
	nodes := make([]*Node, count)
	pos := 8
	for ni := 0; ni < int(count); ni++ {
		node := new(Node)
		dp, err := node.Decode(data[pos:])
		if err != nil {
			return nil, fmt.Errorf("decode node %d: %w", ni, err)
		}
		nodes[ni] = node
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
	//madvise(k, len(k), MADV_WILL_NEED)
	return uint64(10 + l), nil
}

func (b *BpsTree) WarmUp(kv *seg.Reader) (err error) {
	t := time.Now()
	N := b.offt.Count()
	if N == 0 {
		return nil
	}
	b.mx = make([]*Node, 0, N/b.M)
	if b.trace {
		fmt.Printf("mx cap %d N=%d M=%d\n", cap(b.mx), N, b.M)
	}

	step := b.M
	if N < b.M { // cache all keys if less than M
		step = 1
	}

	// extremely stupid picking of needed nodes:
	cachedBytes := uint64(0)
	nsz := uint64(unsafe.Sizeof(Node{}))
	var key []byte
	for i := step; i < N; i += step {
		di := i - 1
		_, key, err = b.keyCmpFunc(nil, di, kv, key[:0])
		if err != nil {
			return err
		}
		b.mx = append(b.mx, &Node{off: b.offt.Get(di), key: common.Copy(key), di: di})
		cachedBytes += nsz + uint64(len(key))
	}

	log.Root().Debug("WarmUp finished", "file", kv.FileName(), "M", b.M, "N", common.PrettyCounter(N),
		"cached", fmt.Sprintf("%d %.2f%%", len(b.mx), 100*(float64(len(b.mx))/float64(N))),
		"cacheSize", datasize.ByteSize(cachedBytes).HR(), "fileSize", datasize.ByteSize(kv.Size()).HR(),
		"took", time.Since(t))
	return nil
}

// bs performs pre-seach over warmed-up list of nodes to figure out left and right bounds on di for key
func (b *BpsTree) bs(x []byte) (n *Node, dl, dr uint64) {
	dr = b.offt.Count()
	m, l, r := 0, 0, len(b.mx) //nolint

	for l < r {
		m = (l + r) >> 1
		n = b.mx[m]

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

	// check cached nodes and narrow roi
	n, l, r := b.bs(seekKey) // l===r when key is found
	if l == r {
		cur.Reset(n.di, g)
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
				cur.Reset(l, g)
			} else {
				cur.Next()
			}

			if cmp = bytes.Compare(cur.key, seekKey); cmp < 0 {
				l++
				continue
			}
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

	n, l, r := b.bs(key) // l===r when key is found
	if b.trace {
		fmt.Printf("pivot di: %d di(LR): [%d %d] k: %x found: %t\n", n.di, l, r, n.key, l == r)
		defer func() { fmt.Printf("found %x [%d %d]\n", key, l, r) }()
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
			v, _ = g.Next(v[:0])
			if cmp = bytes.Compare(v, key); cmp > 0 {
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

		cmp, _, err = b.keyCmpFunc(key, m, g, v[:0])
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

	cmp, _, err = b.keyCmpFunc(key, l, g, v[:0])
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
	var prev int = -1
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
}
