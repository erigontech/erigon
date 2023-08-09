package state

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func NewBpsTree(kv ArchiveGetter, offt *eliasfano32.EliasFano, M uint64) *BpsTree {
	return &BpsTree{M: M, offt: offt, kv: kv}
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
	if i > b.offt.Count() {
		return nil, 0
	}
	o := b.offt.Get(i)
	g.Reset(o)
	buf, _ := g.Next(nil)
	return buf, o
}
func (b *BpsTree) lookupKey(i uint64) ([]byte, uint64) {
	if i > b.offt.Count() {
		return nil, 0
	}
	o := b.offt.Get(i)
	b.kv.Reset(o)
	buf, _ := b.kv.Next(nil)
	return buf, o
}

func (b *BpsTree) lookup(i uint64) ([]byte, []byte, error) {
	if i >= b.offt.Count() {
		return nil, nil, ErrBtIndexLookupBounds
	}
	if b.trace {
		fmt.Printf("lookup %d count %d\n", i, b.offt.Count())
	}
	b.kv.Reset(b.offt.Get(i))
	buf, _ := b.kv.Next(nil)
	val, _ := b.kv.Next(nil)
	return buf, val, nil
}

// if key at i'th position matches prefix, return compare resul`t, value
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

	for j := uint64(1); j <= b.M; j += b.M / 2 {
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

func (b *BpsTree) initialize() {
	k := b.offt.Count()
	d := logBase(k, b.M)

	mx := make([][]Node, d+1)
	key, offt := b.lookupKey(0)
	if key != nil {
		mx[0] = append(mx[0], Node{off: offt, prefix: common.Copy(key)})
		//fmt.Printf("d=%d k %x %d\n", di, k, offt)
	}
	b.traverse(mx, k, 0, 0)

	if b.trace {
		for i := 0; i < len(mx); i++ {
			for j := 0; j < len(mx[i]); j++ {
				fmt.Printf("mx[%d][%d] %x %d %d\n", i, j, mx[i][j].prefix, mx[i][j].off, mx[i][j].i)
			}
		}
	}
	b.mx = mx
}

func (a *BpsTree) bs(x []byte) (n Node, dl, dr uint64) {
	dr = a.offt.Count()
	for d, _ := range a.mx {
		m, l, r := 0, 0, len(a.mx[d])
		for l < r {
			m = (l + r) >> 1
			n = a.mx[d][m]
			if n.i > dr {
				r = m
				continue
			}

			a.naccess++
			if a.trace {
				fmt.Printf("smx[%d][%d] i=%d %x\n", d, m, n.i, n.prefix)
			}
			switch bytes.Compare(n.prefix, x) {
			case 0:
				return n, n.i, n.i
			case 1:
				r = m
				dr = n.i
			case -1:
				l = m + 1
				dl = n.i
			}
		}

	}
	return n, dl, dr
}

func (b *BpsTree) Seek(key []byte) (*BpsTreeIterator, error) {
	if key == nil && b.offt.Count() > 0 {
		return &BpsTreeIterator{t: b, i: 0}, nil
	}
	l, r := uint64(0), b.offt.Count()
	if b.trace {
		fmt.Printf("Seek %x %d %d\n", key, l, r)
	}
	defer func() {
		if b.trace {
			fmt.Printf("found %x [%d %d] naccsess %d\n", key, l, r, b.naccess)
		}
		b.naccess = 0
	}()

	n, dl, dr := b.bs(key)
	switch bytes.Compare(n.prefix, key) {
	case 0:
		return &BpsTreeIterator{t: b, i: n.i}, nil
	case 1:
		if dr < r {
			r = dr
		}
	case -1:
		if dl > l {
			l = dl
		}
	}
	if b.trace {
		fmt.Printf("i %d n %x [%d %d]\n", n.i, n.prefix, l, r)
	}

	m := uint64(0)
	for l < r {
		m = (l + r) >> 1
		k, _ := b.lookupKey(m)
		if k == nil {

		}
		b.naccess++
		if b.trace {
			fmt.Printf("bs %x [%d %d]\n", k, l, r)
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
		return nil, nil
	}
	return &BpsTreeIterator{t: b, i: m}, nil
}

func (b *BpsTree) SeekWithGetter(g ArchiveGetter, key []byte) (*BpsTreeIterator, error) {
	if key == nil && b.offt.Count() > 0 {
		return &BpsTreeIterator{t: b, i: 0}, nil
	}
	l, r := uint64(0), b.offt.Count()
	if b.trace {
		fmt.Printf("Seek %x %d %d\n", key, l, r)
	}
	defer func() {
		if b.trace {
			fmt.Printf("found %x [%d %d] naccsess %d\n", key, l, r, b.naccess)
		}
		b.naccess = 0
	}()

	n, dl, dr := b.bs(key)
	switch bytes.Compare(n.prefix, key) {
	case 0:
		return &BpsTreeIterator{t: b, i: n.i}, nil
	case 1:
		if dr < r {
			r = dr
		}
	case -1:
		if dl > l {
			l = dl
		}
	}
	if b.trace {
		fmt.Printf("i %d n %x [%d %d]\n", n.i, n.prefix, l, r)
	}

	m := uint64(0)
	for l < r {
		m = (l + r) >> 1
		k, _ := b.lookupKeyWGetter(g, m)
		if k == nil {

		}
		b.naccess++
		if b.trace {
			fmt.Printf("bs %x [%d %d]\n", k, l, r)
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
	//if l == r {
	//	return nil, nil
	//}
	return &BpsTreeIterator{t: b, i: m}, nil
}

// trieNode represents a node in the prefix tree
type trieNode struct {
	children [16]*trieNode // Children nodes indexed by the next byte of the key
	prefix   uint16
	common   []byte
	offset   uint64
}

// trie represents the prefix tree
type trie struct {
	root *trieNode // Root of the trie
}

// newTrieNode creates a new trie node
func newTrieNode() *trieNode {
	return &trieNode{common: make([]byte, 0)}
}

// newTrie creates a new prefix tree
func newTrie() *trie {
	return &trie{
		root: newTrieNode(),
	}
}

// insert adds a key to the prefix tree
func (t *trie) insert(n Node) {
	node := t.root
	key := keybytesToHexNibbles(n.prefix)
	key = key[:len(key)-1]
	n.prefix = common.Copy(key)

	fmt.Printf("node insert %x %d\n", key, n.off)

	//pext := 0
	var b byte
	for pi := 0; pi < len(key); pi++ {
		b = key[pi]
		fmt.Printf("currentKey %x c {%x} common [%x] %b branch\n{", key[:pi+1], b, node.common, node.prefix)
		for n, t := range node.children {
			if t != nil {
				fmt.Printf("\n %x) [%x] childs %d", n, t.common, bits.OnesCount16(t.prefix))
			}
		}
		fmt.Printf("\n}\n")

		if node.prefix == 0 && len(node.common) == 0 {
			node.common = common.Copy(key[pi:])
			node.offset = n.off
			break
		}
		if len(node.common) != 0 {
			// has extension
			lc := commonPrefixLen(node.common, key[pi+1:])
			p := node.common[lc]
			nn := newTrieNode()
			for i := 0; i < len(node.children); i++ {
				if node.children[i] != nil {
					nn.children[i] = node.children[i]
					node.children[i] = nil
					nn.prefix |= 1 << i
				}
			}
			nn.common = common.Copy(node.common[1:])
			nn.offset = node.offset
			node.common = nil
			node.prefix, node.offset = 0, 0

			node.prefix |= 1 << p
			node.children[p] = nn

			n1 := newTrieNode()
			n1.common = common.Copy(key[pi+1 : pi+1+lc])
			n1.offset = n.off
			node.children[b] = n1
			node.prefix |= 1 << uint16(b)
		}

		if node.prefix&(1<<uint16(b)) != 0 {
			// node exists
			node = node.children[b]
			continue
		} else {
			// no branch

		}

		//	if node.prefix&(1<<uint16(b)) != 0 {
		//		// node exists
		//		existed := node.children[b]
		//		if existed.common == nil {
		//			continue
		//		}
		//		lc := commonPrefixLen(existed.common, key[pi+1:])
		//		fmt.Printf("key ..%x & %x branches at %d: common %x rest %x\n", key[pi+1:], existed.common, lc, key[pi+1:pi+1+lc], key[pi+1+lc:])
		//
		//		if lc > 0 {
		//			fmt.Printf("extension %x->%x\n", existed.common, key[pi+1:pi+1+lc])
		//			existed.common = common.Copy(key[pi+1 : pi+1+lc])
		//
		//			nn := newTrieNode()
		//			b := key[pi+1+lc]
		//
		//			nn.children[b] = existed
		//			//pext = pi + 1
		//			node.children[b] = nn
		//			node.prefix |= 1 << uint16(b)
		//			pi = pi + lc
		//		} else {
		//			nn := newTrieNode()
		//			nn.common = common.Copy(key[pi+1:])
		//			nn.offset = n.off
		//			fmt.Printf("new char %x common %x\n", key[pi+1], nn.common)
		//			node.children[key[pi+1]] = nn
		//			node.prefix |= 1 << uint16(key[pi+1])
		//			break
		//		}
		//	} else {
		//		if len(node.common) != 0 {
		//			lc := commonPrefixLen(node.common, key[pi:])
		//			if lc > 0 {
		//				fmt.Printf("extension %x->%x\n", node.common, key[pi:pi+lc])
		//				nn := newTrieNode()
		//				nn.common = common.Copy(key[pi : pi+lc])
		//				nn.offset = node.offset
		//				node.common = common.Copy(key[pi+lc:])
		//				node.offset = 0
		//				node.prefix = 0
		//				node.children[key[pi+lc]] = nn
		//				node.prefix |= 1 << uint16(key[pi+lc])
		//				pi = pi + lc
		//			} else {
		//				nn := newTrieNode()
		//				nn.common = common.Copy(key[pi:])
		//				nn.offset = n.off
		//				fmt.Printf("new char %x common %x\n", b, nn.common)
		//				node.children[b] = nn
		//				node.prefix |= 1 << uint16(b)
		//				break
		//			}
		//			continue
		//		}
		//		nn := newTrieNode()
		//		nn.common = common.Copy(key[pi+1:])
		//		nn.offset = n.off
		//		fmt.Printf("new char %x common %x\n", b, nn.common)
		//		node.children[b] = nn
		//		node.prefix |= 1 << uint16(b)
		//		break
		//	}
	}

	//node.offset = n.off
}

// search finds if a key exists in the prefix tree
func (t *trie) search(key []byte) (bool, uint64) {
	node := t.root

	for len(key) > 0 {
		b := key[0]
		key = key[1:]

		child := node.children[b]
		//if !found {
		//	return false, 0
		//}
		node = child

		if len(node.children) == 0 {
			return true, node.offset
		}
	}

	return false, 0
}

func hexToCompact(key []byte) []byte {
	zeroByte, keyPos, keyLen := makeCompactZeroByte(key)
	bufLen := keyLen/2 + 1 // always > 0
	buf := make([]byte, bufLen)
	buf[0] = zeroByte
	return decodeKey(key[keyPos:], buf)
}

func makeCompactZeroByte(key []byte) (compactZeroByte byte, keyPos, keyLen int) {
	keyLen = len(key)
	if hasTerm(key) {
		keyLen--
		compactZeroByte = 0x20
	}
	var firstNibble byte
	if len(key) > 0 {
		firstNibble = key[0]
	}
	if keyLen&1 == 1 {
		compactZeroByte |= 0x10 | firstNibble // Odd: (1<<4) + first nibble
		keyPos++
	}

	return
}

func decodeKey(key, buf []byte) []byte {
	keyLen := len(key)
	if hasTerm(key) {
		keyLen--
	}
	for keyIndex, bufIndex := 0, 1; keyIndex < keyLen; keyIndex, bufIndex = keyIndex+2, bufIndex+1 {
		if keyIndex == keyLen-1 {
			buf[bufIndex] = buf[bufIndex] & 0x0f
		} else {
			buf[bufIndex] = key[keyIndex+1]
		}
		buf[bufIndex] |= key[keyIndex] << 4
	}
	return buf
}

func keybytesToHexNibbles(str []byte) []byte {
	l := len(str)*2 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*2] = b / 16
		nibbles[i*2+1] = b % 16
	}
	nibbles[l-1] = 16
	return nibbles
}

// hasTerm returns whether a hex key has the terminator flag.
func hasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 16
}

func commonPrefixLen(a1, b []byte) int {
	var i int
	fmt.Printf("matching %x %x\n", a1, b)
	for i = 0; i < len(a1) && i < len(b); i++ {
		if a1[i] != b[i] {
			break
		}
	}
	fmt.Printf("matched %d %x\n", i, a1[:i])
	return i
}
