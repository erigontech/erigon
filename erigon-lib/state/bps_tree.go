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

	"github.com/erigontech/erigon-lib/common"
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
	offt  *eliasfano32.EliasFano
	mx    [][]Node
	M     uint64
	trace bool

	dataLookupFunc dataLookupFunc
	keyCmpFunc     keyCmpFunc
}

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

func (it *BpsTreeIterator) Di() uint64 {
	return it.i
}

func (it *BpsTreeIterator) KVFromGetter(g ArchiveGetter) ([]byte, []byte, error) {
	if it == nil {
		return nil, nil, fmt.Errorf("iterator is nil")
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
	off    uint64
	di     uint64
	prefix []byte
}

func (b *BpsTree) traverse(g ArchiveGetter, mx [][]Node, n, di, i uint64) {
	if i >= n {
		return
	}

	for j := uint64(1); j <= b.M; j += b.M / 2 {
		ik := i*b.M + j
		if ik >= n {
			break
		}
		_, k, err := b.keyCmpFunc(nil, ik, g)
		if err != nil {
			panic(err)
		}
		if k != nil {
			mx[di] = append(mx[di], Node{off: b.offt.Get(ik), prefix: common.Copy(k), di: ik})
			//fmt.Printf("d=%d k %x %d\n", di+1, k, offt)
		}
		b.traverse(g, mx, n, di, ik)
	}
}

func (b *BpsTree) WarmUp(kv ArchiveGetter) error {
	k := b.offt.Count()
	d := logBase(k, b.M)

	mx := make([][]Node, d+1)
	_, key, err := b.keyCmpFunc(nil, 0, kv)
	if err != nil {
		return err
	}
	if key != nil {
		mx[0] = append(mx[0], Node{off: b.offt.Get(0), prefix: common.Copy(key)})
		//fmt.Printf("d=%d k %x %d\n", di, k, offt)
	}
	b.traverse(kv, mx, k, 0, 0)

	if b.trace {
		for i := 0; i < len(mx); i++ {
			for j := 0; j < len(mx[i]); j++ {
				fmt.Printf("mx[%d][%d] %x %d %d\n", i, j, mx[i][j].prefix, mx[i][j].off, mx[i][j].di)
			}
		}
	}
	b.mx = mx
	return nil
}

func (b *BpsTree) bs(x []byte) (n Node, dl, dr uint64) {
	dr = b.offt.Count()
	for d, row := range b.mx {
		m, l, r := 0, 0, len(row) //nolint
		for l < r {
			m = (l + r) >> 1
			n = row[m]

			if b.trace {
				fmt.Printf("bs[%d][%d] i=%d %x\n", d, m, n.di, n.prefix)
			}
			switch bytes.Compare(n.prefix, x) {
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

	}
	return n, dl, dr
}

// Seek returns first key which is >= key.
// Found is true iff exact key match is found.
// If key is nil, returns first key and found=true
// If found item.key has a prefix of key, returns found=false and item.key
// if key is greater than all keys, returns nil, found=false
func (b *BpsTree) Seek(g ArchiveGetter, key []byte) (skey []byte, di uint64, found bool, err error) {
	if key == nil && b.offt.Count() > 0 {
		//return &BpsTreeIterator{t: b, i: 0}, nil
		var cmp int
		cmp, skey, err = b.keyCmpFunc(key, 0, g)
		if err != nil {
			return nil, 0, false, err
		}
		return skey, 0, cmp == 0, nil
	}

	l, r := uint64(0), b.offt.Count()
	if b.trace {
		fmt.Printf("seek %x [%d %d]\n", key, l, r)
	}
	defer func() {
		if b.trace {
			fmt.Printf("found %x [%d %d]\n", key, l, r)
		}
	}()

	n, dl, dr := b.bs(key)
	if b.trace {
		fmt.Printf("pivot %d n %x [%d %d]\n", n.di, n.prefix, dl, dr)
	}
	l, r = dl, dr

	var m uint64
	var cmp int
	for l < r {
		m = (l + r) >> 1
		cmp, skey, err = b.keyCmpFunc(key, m, g)
		if err != nil {
			return nil, 0, false, err
		}
		if b.trace {
			fmt.Printf("lr %x [%d %d]\n", skey, l, r)
		}

		switch cmp {
		case 0:
			return skey, m, true, nil
			//return &BpsTreeIterator{t: b, i: m}, nil
		case 1:
			r = m
		case -1:
			l = m + 1
		}
	}
	if l == r {
		m = l
		//return &BpsTreeIterator{t: b, i: l}, nil
	}

	cmp, skey, err = b.keyCmpFunc(key, m, g)
	if err != nil {
		return nil, 0, false, err
	}
	return skey, m, cmp == 0, nil
}

// returns first key which is >= key.
// If key is nil, returns first key
// if key is greater than all keys, returns nil
func (b *BpsTree) Get(g ArchiveGetter, key []byte) ([]byte, bool, uint64, error) {
	if key == nil && b.offt.Count() > 0 {
		k0, v0, err := b.dataLookupFunc(0, g)
		if err != nil || k0 != nil {
			return nil, false, 0, err
		}
		return v0, true, 0, nil
	}

	l, r := uint64(0), b.offt.Count()
	if b.trace {
		fmt.Printf("seek %x [%d %d]\n", key, l, r)
	}
	defer func() {
		if b.trace {
			fmt.Printf("found %x [%d %d]\n", key, l, r)
		}
	}()

	n, dl, dr := b.bs(key)
	if b.trace {
		fmt.Printf("pivot %d n %x [%d %d]\n", n.di, n.prefix, dl, dr)
	}
	l, r = dl, dr
	var m uint64
	for l < r {
		m = (l + r) >> 1
		cmp, k, err := b.keyCmpFunc(key, m, g)
		if err != nil {
			return nil, false, 0, err
		}
		if b.trace {
			fmt.Printf("lr [%d %d]\n", l, r)
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
