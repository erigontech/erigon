package state

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func NewBpsTree(kv *compress.Getter, offt *eliasfano32.EliasFano, M uint64) *BpsTree {
	return &BpsTree{M: M, offt: offt, kv: kv}
}

type BpsTree struct {
	M    uint64
	offt *eliasfano32.EliasFano
	kv   *compress.Getter
	mx   [][]Node
}

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

func (b *BpsTreeIterator) KV() ([]byte, []byte) {
	return b.t.lookup(b.i)
}

func (it *BpsTreeIterator) Next() ([]byte, []byte) {
	it.i++
	return it.t.lookup(it.i)
}

func (b *BpsTree) lookupKey(i uint64) ([]byte, uint64) {
	o := b.offt.Get(i)
	fmt.Printf("lookupKey %d %d\n", i, o)
	b.kv.Reset(o)
	buf, _ := b.kv.Next(nil)
	return buf, o
}

func (b *BpsTree) lookup(i uint64) ([]byte, []byte) {
	b.kv.Reset(b.offt.Get(i))
	buf, _ := b.kv.Next(nil)
	val, _ := b.kv.Next(nil)
	return buf, val
}

// if key at i'th position matches prefix, return compare result, value
func (b *BpsTree) matchLookup(i uint64, pref []byte) ([]byte, []byte) {
	b.kv.Reset(b.offt.Get(i))
	if b.kv.MatchPrefix(pref) {
		k, _ := b.kv.Next(nil)
		v, _ := b.kv.Next(nil)
		return k, v
	}
	return nil, nil
}

type Node struct {
	off    uint64
	i      uint64
	prefix []byte
}

func (b *BpsTree) traverse(mx [][]Node, n, di, i uint64) {
	if i >= n {
		return
	}

	for j := uint64(1); j <= b.M; j += b.M - 1 {
		ik := i*b.M + j
		if ik >= n {
			break
		}
		k, offt := b.lookupKey(ik)
		if k != nil {
			mx[di+1] = append(mx[di+1], Node{off: offt, prefix: common.Copy(k), i: ik})
			//fmt.Printf("d=%d k %x %d\n", di+1, k, offt)
		}
		b.traverse(mx, n, di+1, ik)
	}
}

func (b *BpsTree) FillStack() {
	k := b.offt.Count()
	d := logBase(k, b.M)

	mx := make([][]Node, d+1)
	key, offt := b.lookupKey(0)
	if key != nil {
		mx[0] = append(mx[0], Node{off: offt, prefix: common.Copy(key)})
		//fmt.Printf("d=%d k %x %d\n", di, k, offt)
	}
	b.traverse(mx, k, 0, 0)

	for i := 0; i < len(mx); i++ {
		for j := 0; j < len(mx[i]); j++ {
			fmt.Printf("mx[%d][%d] %x %d %d\n", i, j, mx[i][j].prefix, mx[i][j].off, mx[i][j].i)
		}
	}

	b.mx = mx
}

func (b *BpsTree) Seek(key []byte) (*BpsTreeIterator, error) {
	l, r := uint64(0), b.offt.Count()
	fmt.Printf("Seek %x %d %d\n", key, l, r)
	for l < r {
		kl, _ := b.lookupKey(l)
		switch bytes.Compare(kl, key) {
		case 0:
			return &BpsTreeIterator{t: b, i: l}, nil
		case 1:
			r = b.M * (l + 1)
		case -1:
			l += 1
			//r = l + 1
		}
		fmt.Printf("l=%d r=%d kl %x\n", l, r, kl)

	}
	return nil, nil
}
