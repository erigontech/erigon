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
	offt *eliasfano32.EliasFano
	kv   *compress.Getter
	mx   [][]Node
	M    uint64

	naccess uint64
}

type BpsTreeIterator struct {
	t *BpsTree
	i uint64
}

func (it *BpsTreeIterator) KV() ([]byte, []byte) {
	k, v, _ := it.t.lookup(it.i)
	return k, v
}

func (it *BpsTreeIterator) Next() bool {
	if it.i+1 == it.t.offt.Count() {
		return false
	}
	it.i++
	return true
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
	fmt.Printf("lookup %d count %d\n", i, b.offt.Count())
	b.kv.Reset(b.offt.Get(i))
	buf, _ := b.kv.Next(nil)
	val, _ := b.kv.Next(nil)
	return buf, val, nil
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

	for i := 0; i < len(mx); i++ {
		for j := 0; j < len(mx[i]); j++ {
			fmt.Printf("mx[%d][%d] %x %d %d\n", i, j, mx[i][j].prefix, mx[i][j].off, mx[i][j].i)
		}
	}

	//trie := newTrie()
	//
	//for i := 0; i < len(mx); i++ {
	//	for j := 0; j < len(mx[i]); j++ {
	//		trie.insert(mx[i][j])
	//	}
	//}

	b.mx = mx
}

// trieNode represents a node in the prefix tree
type trieNode struct {
	children map[byte]*trieNode // Children nodes indexed by the next byte of the key
	common   []byte
	offset   uint64
}

// trie represents the prefix tree
type trie struct {
	root     *trieNode // Root of the trie
	branches []uint16
	row      uint64
}

// newTrieNode creates a new trie node
func newTrieNode() *trieNode {
	return &trieNode{children: make(map[byte]*trieNode)}
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
	fmt.Printf("node insert %x %d\n", key, n.off)

	pext := 0
	for pi, b := range key {
		fmt.Printf("currentKey %x c {%x} common [%x] branch {", key[:pi+1], b, node.common)
		for n, t := range node.children {
			fmt.Printf("\n %x) [%x] size %d", n, t.common, len(t.children))
		}
		fmt.Printf("}\n")

		child, found := node.children[b]
		if found {
			node = child
			continue
		}

		if len(node.common) > 0 {
			lc := commonPrefixLen(node.common, key[pi:])
			fmt.Printf("key %x & %x branches at %d %x %x\n", key[:pi], node.common, pi, key[pi:], key[pi+lc:])
			if lc > 0 {
				fmt.Printf("branches at %d %x %x %x\n", pi, node.common, key[pi:], key[pi+lc:])
				node.common = key[pi : pi+lc]

				child = newTrieNode()
				child.common = key[pext+lc:]
				pext = pi
				node.children[node.common[0]] = node
			}
		}

		//child = newTrieNode()
		//node.children[b] = child
		if len(node.children) == 1 {
			node.common = key[pi:]
			child.offset = n.i
			fmt.Printf("insert leaf [%x|%x] %d\n", key[:pi], key[pi:], child.offset)
			break
		} else {
			node.common = nil
		}

	}

	node.offset = n.off
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
	for i = 0; i < len(a1) && i < len(b); i++ {
		if a1[i] != b[i] {
			break
		}
	}
	fmt.Printf("matched %d %x\n", i, a1[:i])
	return i
}

// search finds if a key exists in the prefix tree
func (t *trie) search(key []byte) (bool, uint64) {
	node := t.root

	for len(key) > 0 {
		b := key[0]
		key = key[1:]

		child, found := node.children[b]
		if !found {
			return false, 0
		}
		node = child

		if len(node.children) == 0 {
			return true, node.offset
		}
	}

	return false, 0
}

func (a *BpsTree) bs(x []byte) (n Node, dl, dr uint64) {
	for d, _ := range a.mx {
		m, l, r := 0, 0, len(a.mx[d])
		for l < r {
			m = (l + r) >> 1
			n = a.mx[d][m]

			a.naccess++
			fmt.Printf("smx[%d][%d] i=%d %x\n", d, m, n.i, n.prefix)
			switch bytes.Compare(a.mx[d][m].prefix, x) {
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
	fmt.Printf("Seek %x %d %d\n", key, l, r)
	defer func() {
		fmt.Printf("found %x [%d %d] naccsess %d\n", key, l, r, b.naccess)
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
	fmt.Printf("i %d n %x [%d %d]\n", n.i, n.prefix, l, r)

	m := uint64(0)
	//var lastKey []byte
	for l < r {
		m = (l + r) >> 1
		k, _ := b.lookupKey(m)
		if k == nil {

		}
		b.naccess++
		fmt.Printf("bs %x [%d %d]\n", k, l, r)
		//lastKey = common.Copy(k)

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
