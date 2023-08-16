package state

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

type indexSeeker interface {
	WarmUp(g ArchiveGetter) error
	Get(g ArchiveGetter, key []byte) (*BpsTreeIterator, error)
	SeekWithGetter(g ArchiveGetter, key []byte) (*BpsTreeIterator, error)
}

type indexSeekerIterator interface {
	Next() bool
	Offset() uint64
	KV(g ArchiveGetter) ([]byte, []byte)
}

func NewBpsTree(kv ArchiveGetter, offt *eliasfano32.EliasFano, M uint64) *BpsTree {
	bt := &BpsTree{M: M, offt: offt, kv: kv}
	bt.initialize()
	return bt
}

type BpsTree struct {
	offt    *eliasfano32.EliasFano
	kv      ArchiveGetter // Getter is thread unsafe
	mx      [][]Node
	M       uint64
	trace   bool
	naccess uint64
}

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

func (it *BpsTreeIterator) KV() ([]byte, []byte) {
	k, v, _ := it.t.lookupWithGetter(it.t.kv, it.i)
	return k, v
}

func (it *BpsTreeIterator) Offset() uint64 {
	return it.t.offt.Get(it.i)
}

func (it *BpsTreeIterator) KVFromGetter(g ArchiveGetter) ([]byte, []byte, error) {
	if it == nil {
		return nil, nil, fmt.Errorf("iterator is nil")
	}
	//fmt.Printf("kv from %p getter %p tree %p offt %d\n", it, g, it.t, it.i)
	return it.t.lookupWithGetter(g, it.i)
}

func (it *BpsTreeIterator) Next() bool {
	if it.i+1 == it.t.offt.Count() {
		return false
	}
	it.i++
	return true
}

func (b *BpsTree) lookupWithGetter(g ArchiveGetter, i uint64) ([]byte, []byte, error) {
	if i >= b.offt.Count() {
		return nil, nil, ErrBtIndexLookupBounds
	}
	if b.trace {
		fmt.Printf("lookup %d count %d\n", i, b.offt.Count())
	}
	g.Reset(b.offt.Get(i))
	buf, _ := g.Next(nil)
	val, _ := g.Next(nil)
	return buf, val, nil
}

func (b *BpsTree) lookupKeyWGetter(g ArchiveGetter, i uint64) ([]byte, uint64) {
	if i >= b.offt.Count() {
		return nil, 0
	}
	o := b.offt.Get(i)
	g.Reset(o)
	buf, _ := g.Next(nil)
	return buf, o
}

type Node struct {
	off    uint64
	di     uint64
	prefix []byte
}

func (b *BpsTree) traverse(mx [][]Node, n, di, i uint64) {
	if i >= n {
		return
	}

	for j := uint64(1); j <= b.M; j += b.M / 2 {
		ik := i*b.M + j
		if ik >= n {
			break
		}
		k, offt := b.lookupKeyWGetter(b.kv, ik)
		if k != nil {
			mx[di] = append(mx[di], Node{off: offt, prefix: common.Copy(k), di: ik})
			//fmt.Printf("d=%d k %x %d\n", di+1, k, offt)
		}
		b.traverse(mx, n, di, ik)
	}
}

func (b *BpsTree) initialize() {
	k := b.offt.Count()
	d := logBase(k, b.M)

	mx := make([][]Node, d+1)
	key, offt := b.lookupKeyWGetter(b.kv, 0)
	if key != nil {
		mx[0] = append(mx[0], Node{off: offt, prefix: common.Copy(key)})
		//fmt.Printf("d=%d k %x %d\n", di, k, offt)
	}
	b.traverse(mx, k, 0, 0)

	if b.trace {
		for i := 0; i < len(mx); i++ {
			for j := 0; j < len(mx[i]); j++ {
				fmt.Printf("mx[%d][%d] %x %d %d\n", i, j, mx[i][j].prefix, mx[i][j].off, mx[i][j].di)
			}
		}
	}
	b.mx = mx
}

func (a *BpsTree) bs(x []byte) (n Node, dl, dr uint64) {
	dr = a.offt.Count()
	for d, row := range a.mx {
		m, l, r := 0, 0, len(row)
		for l < r {
			m = (l + r) >> 1
			n = row[m]
			a.naccess++

			if a.trace {
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

// returns first key which is >= key.
// If key is nil, returns first key
// if key is greater than all keys, returns nil
func (b *BpsTree) SeekWithGetter(g ArchiveGetter, key []byte) (*BpsTreeIterator, error) {
	if key == nil && b.offt.Count() > 0 {
		return &BpsTreeIterator{t: b, i: 0}, nil
	}
	l, r := uint64(0), b.offt.Count()
	if b.trace {
		fmt.Printf("seek %x [%d %d]\n", key, l, r)
	}
	defer func() {
		if b.trace {
			fmt.Printf("found %x [%d %d] naccsess %d\n", key, l, r, b.naccess)
		}
		b.naccess = 0
	}()

	n, dl, dr := b.bs(key)
	if b.trace {
		fmt.Printf("pivot %d n %x [%d %d]\n", n.di, n.prefix, dl, dr)
	}
	l, r = dl, dr

	m := uint64(0)
	for l < r {
		m = (l + r) >> 1
		k, _ := b.lookupKeyWGetter(g, m)
		b.naccess++
		if b.trace {
			fmt.Printf("lr %x [%d %d]\n", k, l, r)
		}

		switch bytes.Compare(k, key) {
		case 0:
			return &BpsTreeIterator{t: b, i: m}, nil
		case 1:
			r = m
		case -1:
			l = m + 1
		}
	}
	if l == r {
		if r == b.offt.Count() {
			return nil, nil
		}
		return &BpsTreeIterator{t: b, i: l}, nil
	}
	return &BpsTreeIterator{t: b, i: m}, nil
}
