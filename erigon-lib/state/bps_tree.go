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
	"errors"
	"fmt"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
)

// nolint
type indexSeeker interface {
	WarmUp(g ArchiveGetter) error
	Get(g ArchiveGetter, key []byte) (k []byte, found bool, di uint64, err error)
	//seekInFiles(g ArchiveGetter, key []byte) (indexSeekerIterator, error)
	Seek(g ArchiveGetter, seek []byte) (k []byte, di uint64, found bool, err error)
}

// nolint
type indexSeekerIterator interface {
	Next() bool
	Di() uint64
	KVFromGetter(g ArchiveGetter) ([]byte, []byte, error)
}

type dataLookupFunc func(di uint64, g ArchiveGetter) ([]byte, []byte, error)
type keyCmpFunc func(k []byte, di uint64, g ArchiveGetter) (int, []byte, error)

// M limits amount of child for tree node.
func NewBpsTree(kv ArchiveGetter, offt *eliasfano32.EliasFano, M uint64, dataLookup dataLookupFunc, keyCmp keyCmpFunc) *BpsTree {
	bt := &BpsTree{M: M, offt: offt, dataLookupFunc: dataLookup, keyCmpFunc: keyCmp}
	if err := bt.WarmUp(kv); err != nil {
		panic(err)
	}
	return bt
}

type BpsTree struct {
	offt  *eliasfano32.EliasFano // ef with offsets to key/vals
	mx    []Node
	M     uint64 // limit on amount of 'children' for node
	trace bool

	dataLookupFunc dataLookupFunc
	keyCmpFunc     keyCmpFunc
}

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

// Di returns ordinal number of current key in the tree
func (it *BpsTreeIterator) Di() uint64 {
	return it.i
}

func (it *BpsTreeIterator) KVFromGetter(g ArchiveGetter) ([]byte, []byte, error) {
	if it == nil {
		return nil, nil, errors.New("iterator is nil")
	}
	//fmt.Printf("kv from %p getter %p tree %p offt %d\n", it, g, it.t, it.i)
	k, v, err := it.t.dataLookupFunc(it.i, g)
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

func (b *BpsTree) WarmUp(kv ArchiveGetter) error {
	N := b.offt.Count()
	if N == 0 {
		return nil
	}
	b.mx = make([]Node, 0, N/b.M)
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
	for i := step; i < N; i += step {
		di := i - 1
		_, key, err := b.keyCmpFunc(nil, di, kv)
		if err != nil {
			return err
		}
		b.mx = append(b.mx, Node{off: b.offt.Get(di), key: common.Copy(key), di: di})
		cachedBytes += nsz + uint64(len(key))
	}

	log.Root().Debug("WarmUp finished", "file", kv.FileName(), "M", b.M, "N", N,
		"cached", fmt.Sprintf("%d %%%.5f", len(b.mx), float64(len(b.mx))/float64(N)*100),
		"cacheSize", datasize.ByteSize(cachedBytes).HR(), "fileSize", datasize.ByteSize(kv.Size()).HR())
	return nil
}

// bs performs pre-seach over warmed-up list of nodes to figure out left and right bounds on di for key
func (b *BpsTree) bs(x []byte) (n Node, dl, dr uint64) {
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
		}
	}
	return n, dl, dr
}

// Seek returns first key which is >= key.
// Found is true iff exact key match is found.
// If key is nil, returns first key and found=true
// If found item.key has a prefix of key, returns found=false and item.key
// if key is greater than all keys, returns nil, found=false
func (b *BpsTree) Seek(g ArchiveGetter, seekKey []byte) (key, value []byte, di uint64, found bool, err error) {
	//b.trace = true
	if b.trace {
		fmt.Printf("seek %x\n", seekKey)
	}
	if len(seekKey) == 0 && b.offt.Count() > 0 {
		key, value, err = b.dataLookupFunc(0, g)
		if err != nil {
			return nil, nil, 0, false, err
		}
		//return key, value, 0, bytes.Compare(key, seekKey) >= 0, nil
		return key, value, 0, bytes.Equal(key, seekKey), nil
	}

	n, l, r := b.bs(seekKey) // l===r when key is found
	if b.trace {
		fmt.Printf("pivot di:%d di(LR): [%d %d] k: %x found: %t\n", n.di, l, r, n.key, l == r)
		defer func() { fmt.Printf("found=%t %x [%d %d]\n", bytes.Equal(key, seekKey), seekKey, l, r) }()
	}
	var m uint64
	var cmp int
	for l < r {
		if r-l <= DefaultBtreeStartSkip { // found small range, faster to scan now
			cmp, key, err = b.keyCmpFunc(seekKey, l, g)
			if err != nil {
				return nil, nil, 0, false, err
			}
			if b.trace {
				fmt.Printf("fs di:[%d %d] k: %x\n", l, r, key)
			}
			//fmt.Printf("N %d l %d cmp %d (found %x want %x)\n", b.offt.Count(), l, cmp, key, seekKey)
			if cmp == 0 {
				r = l
				break
			} else if cmp < 0 { //found key is greater than seekKey
				if l+1 < b.offt.Count() {
					l++
					continue
				}
			}
			r = l
			break
		}

		m = (l + r) >> 1
		cmp, key, err = b.keyCmpFunc(seekKey, m, g)
		if err != nil {
			return nil, nil, 0, false, err
		}
		if b.trace {
			fmt.Printf("fs di:[%d %d] k: %x\n", l, r, key)
		}

		if cmp == 0 {
			l, r = m, m
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
	key, value, err = b.dataLookupFunc(m, g)
	if err != nil {
		return nil, nil, 0, false, err
	}
	return key, value, l, bytes.Equal(key, seekKey), nil
}

// returns first key which is >= key.
// If key is nil, returns first key
// if key is greater than all keys, returns nil
func (b *BpsTree) Get(g ArchiveGetter, key []byte) ([]byte, bool, uint64, error) {
	if b.trace {
		fmt.Printf("get   %x\n", key)
	}
	if len(key) == 0 && b.offt.Count() > 0 {
		k0, v0, err := b.dataLookupFunc(0, g)
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

	var m uint64
	for l < r {
		m = (l + r) >> 1
		cmp, k, err := b.keyCmpFunc(key, m, g)
		if err != nil {
			return nil, false, 0, err
		}
		if b.trace {
			fmt.Printf("fs [%d %d]\n", l, r)
		}

		switch cmp {
		case 0:
			return k, true, m, nil
		case 1:
			r = m
		case -1:
			l = m + 1
		}
	}

	cmp, k, err := b.keyCmpFunc(key, l, g)
	if err != nil || cmp != 0 {
		return nil, false, 0, err
	}
	return k, true, l, nil
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
