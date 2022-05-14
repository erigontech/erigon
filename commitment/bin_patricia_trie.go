package commitment

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common/length"

	"golang.org/x/crypto/sha3"
)

type BinPatriciaTrie struct {
	root   *RootNode
	trace  bool
	stat   stat
	keccak keccakState
}

type stat struct {
	hashesTotal uint64
	nodesTotal  uint64
}

func NewBinaryPatriciaTrie() *BinPatriciaTrie {
	return &BinPatriciaTrie{
		keccak: sha3.NewLegacyKeccak256().(keccakState),
	}
}

func (t *BinPatriciaTrie) Update(key, value []byte) {
	keyPath := newBitstring(key)
	if t.root == nil {
		t.root = &RootNode{
			Node: &Node{
				Key:   key,
				Value: value,
			},
			CommonPrefix: keyPath,
		}
		t.stat.nodesTotal++
		return
	}

	edge, keyPathRest, splitAt, latest := t.root.Walk(keyPath, func(_ *Node) {})
	if len(edge) == 0 && len(keyPathRest) == 0 {
		latest.Value = value
		return
	}
	pathToLatest := keyPath[:len(keyPath)-len(keyPathRest)]

	t.stat.nodesTotal++
	newLeaf := &Node{P: latest, Key: key, Value: value}
	latest.splitEdge(pathToLatest, edge[:splitAt], edge[splitAt:], keyPathRest, newLeaf, t.hash)
	if latest.P == nil {
		t.root.CommonPrefix = edge[:splitAt]
	}
}

// Key describes path in trie to value. When UpdateHashed is used,
// hashed key describes path to the leaf node and plainKey is stored in the leaf node Key field.
func (t *BinPatriciaTrie) UpdateHashed(plainKey, hashedKey, value []byte, isStorage bool) (updates map[string][]byte) {
	keyPath := newBitstring(hashedKey)
	updates = make(map[string][]byte)
	if t.root == nil {
		t.root = &RootNode{
			Node: &Node{
				Key:       plainKey,
				Value:     value,
				isStorage: isStorage,
			},
			CommonPrefix: keyPath,
		}

		t.stat.nodesTotal++
		t.hash(t.root.Node, keyPath, 0)

		touchMap := uint16(1 << keyPath[len(keyPath)-1])

		updates[keyPath.String()] = encodeNodeUpdate(t.root.Node, touchMap, touchMap, nil)
		return updates
	}

	touchedNodes := make([]*Node, 0)

	edge, keyPathRest, splitAt, latest := t.root.Walk(keyPath, func(fn *Node) { fn.hash = fn.hash[:0]; touchedNodes = append(touchedNodes, fn) })
	pathToLatest := keyPath[:len(keyPath)-len(keyPathRest)]

	var touchMap uint16
	if len(edge) == 0 && len(keyPathRest) == 0 { // we found the leaf
		latest.Value = value
		t.hash(latest, bitstring{}, 0)

		touchMap = 1 << edge[len(edge)-1]
		updates[pathToLatest.String()] = encodeNodeUpdate(latest, touchMap, touchMap, nil)
		return updates
	}

	// split existing edge and insert new leaf
	t.stat.nodesTotal++

	newLeaf := &Node{P: latest, Key: plainKey, Value: value, isStorage: isStorage}
	updates = latest.splitEdge(pathToLatest, edge[:splitAt], edge[splitAt:], keyPathRest, newLeaf, t.hash)
	if latest.P == nil {
		t.root.CommonPrefix = edge[:splitAt]
	}
	return updates
}

// Get returns value stored by provided key.
func (t *BinPatriciaTrie) Get(key []byte) ([]byte, bool) {
	keyPath := newBitstring(key)
	if t.root == nil {
		return nil, false
	}

	edge, keyPathRest, _, latest := t.root.Walk(keyPath, func(_ *Node) {})
	if len(edge) == 0 && len(keyPathRest) == 0 {
		if latest.Value == nil {
			switch {
			case len(latest.LPrefix) == 0 && latest.L != nil:
				return latest.L.Value, true
			case len(latest.RPrefix) == 0 && latest.R != nil:
				return latest.R.Value, true
			}
		}
		return latest.Value, true
	}
	return nil, false
}

// Get returns value stored by provided key.
func (t *BinPatriciaTrie) getNode(key []byte) *Node {
	keyPath := newBitstring(key)
	if t.root == nil {
		return nil
	}

	edge, keyPathRest, _, latest := t.root.Walk(keyPath, func(_ *Node) {})
	if len(edge) == 0 && len(keyPathRest) == 0 {
		return latest
	}
	return nil
}

func (t *BinPatriciaTrie) RootHash() ([]byte, error) {
	if t.root == nil {
		return EmptyRootHash, nil
	}
	return t.hash(t.root.Node, t.root.CommonPrefix, 0), nil
}

func (t *BinPatriciaTrie) ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (branchNodeUpdates map[string][]byte, err error) {
	branchNodeUpdates = make(map[string][]byte)
	for i, update := range updates {
		account := new(Account)
		node := t.getNode(hashedKeys[i]) // check if key exist
		if node != nil && !node.isStorage {
			account.decode(node.Value)
		}

		// apply supported updates
		if update.Flags == DELETE_UPDATE {
			//continue
			if node != nil {
				if node.P != nil {
					meltPrefix := node.P.deleteChild(node)
					if node.P.P == nil {
						t.root.CommonPrefix = append(t.root.CommonPrefix, meltPrefix...)
					}
				} else { // remove root
					t.root = nil
				}
				t.stat.nodesTotal--
				branchNodeUpdates[hexToBin(hashedKeys[i]).String()] = []byte{}
			}
			continue
		}
		if update.Flags&BALANCE_UPDATE != 0 {
			account.Balance.Set(&update.Balance)
		}
		if update.Flags&NONCE_UPDATE != 0 {
			account.Nonce = update.Nonce
		}
		if update.Flags&CODE_UPDATE != 0 {
			if account.CodeHash == nil {
				account.CodeHash = make([]byte, len(update.CodeHashOrStorage))
			}
			copy(account.CodeHash, update.CodeHashOrStorage[:])
		}

		aux := make([]byte, 0)
		isStorage := false
		if update.Flags&STORAGE_UPDATE != 0 {
			isStorage = true
			aux = update.CodeHashOrStorage[:update.ValLength]
		}

		// aux is not empty only when storage update is there
		if len(aux) == 0 {
			aux = account.encode(aux)
		}

		ukey := t.UpdateHashed(plainKeys[i], hashedKeys[i], aux, isStorage)
		for pref, val := range ukey {
			branchNodeUpdates[pref] = val
			if val != nil && t.trace {
				fmt.Printf("%q => %s\n", pref, branchToString2(val))
			}
		}
		for pref, upd := range t.rootHashWithUpdates() {
			v, ex := branchNodeUpdates[pref]
			if ex {
				upd = append(v[:4], upd[4:]...)
			}
			branchNodeUpdates[pref] = upd
		}
	}

	return branchNodeUpdates, nil
}

func DecodeNodeFromUpdate(buf []byte) (touch, after uint16, node Node, err error) {
	if len(buf) < 5 {
		return
	}

	touch = binary.BigEndian.Uint16(buf[:2])
	after = binary.BigEndian.Uint16(buf[2:4])
	bits, pos := PartFlags(buf[4]), 5

	if bits&ACCOUNT_PLAIN_PART != 0 {
		n, aux, err := decodeSizedBuffer(buf[pos:])
		if err != nil {
			return touch, after, Node{}, fmt.Errorf("decode account plain key: %w", err)
		}
		pos += n
		node.Key = aux
	}

	if bits&STORAGE_PLAIN_PART != 0 {
		n, aux, err := decodeSizedBuffer(buf[pos:])
		if err != nil {
			return touch, after, Node{}, fmt.Errorf("decode storage plain key: %w", err)
		}
		pos += n
		node.Key = aux
		node.isStorage = true
	}

	if bits&HASH_PART != 0 {
		n, aux, err := decodeSizedBuffer(buf[pos:])
		if err != nil {
			return touch, after, Node{}, fmt.Errorf("decode node hash: %w", err)
		}
		pos += n
		_ = pos
		node.hash = aux
	}

	return
}

func encodeNodeUpdate(node *Node, touched, after uint16, branchData []byte) []byte {
	var numBuf [binary.MaxVarintLen64]byte
	binary.BigEndian.PutUint16(numBuf[0:], touched)
	binary.BigEndian.PutUint16(numBuf[2:], after)

	if branchData == nil {
		branchData = make([]byte, 4, 32)
	}
	copy(branchData[:4], numBuf[:])

	var fieldBits PartFlags
	if node.Value != nil {
		fieldBits = ACCOUNT_PLAIN_PART
		if node.isStorage {
			fieldBits = STORAGE_PLAIN_PART
		}
	}
	if len(node.hash) == length.Hash {
		fieldBits |= HASH_PART
	}

	branchData = append(branchData, byte(fieldBits))
	if fieldBits&(ACCOUNT_PLAIN_PART|STORAGE_PLAIN_PART) != 0 {
		n := binary.PutUvarint(numBuf[:], uint64(len(node.Key)))
		branchData = append(branchData, append(numBuf[:n], node.Key...)...)
	}

	if fieldBits&HASH_PART > 0 {
		n := binary.PutUvarint(numBuf[:], uint64(len(node.hash)))
		branchData = append(branchData, append(numBuf[:n], node.hash...)...)
	}

	return branchData
}

func (t *BinPatriciaTrie) Reset() {
	t.root = nil
	fmt.Printf("trie %v\n", t.StatString())
}

func (t *BinPatriciaTrie) ResetFns(
	branchFn func(prefix []byte) ([]byte, error),
	accountFn func(plainKey []byte, cell *Cell) error,
	storageFn func(plainKey []byte, cell *Cell) error,
) {
}

func (t *BinPatriciaTrie) Variant() TrieVariant { return VariantBinPatriciaTrie }

func (t *BinPatriciaTrie) SetTrace(b bool) { t.trace = b }

type RootNode struct {
	*Node
	CommonPrefix bitstring
}

// There are three types of nodes:
// - Leaf (with a value and without branches)
// - Branch (with left and right child)
// - Root - Either leaf or branch. When root is branch, it's Key contains their common prefix as a bitstring.
type Node struct {
	L, R, P   *Node     // left and right child, parent. For root P is nil
	LPrefix   bitstring // left child prefix, always begins with 0
	RPrefix   bitstring // right child prefix, always begins with 1
	hash      []byte    // node hash
	Key       []byte    // same as common prefix, useful for debugging, actual key should be reconstructed by path to the node
	Value     []byte    // exists only in LEAF node
	isStorage bool      // if true, then Value holds storage value for the Key, otherwise it holds encoded account
}

func (n *Node) splitEdge(pathToNode, commonPath, detachedPath, restKeyPath bitstring, newLeaf *Node, hasher func(n *Node, pref bitstring, offt int) []byte) map[string][]byte {
	var movedNode *Node
	switch {
	case n.Value == nil:
		movedNode = &Node{ // move existed branch
			L:       n.L,
			R:       n.R,
			P:       n,
			LPrefix: n.LPrefix,
			RPrefix: n.RPrefix,
			hash:    n.hash,
		}
		movedNode.L.P, movedNode.R.P = movedNode, movedNode
	default:
		movedNode = &Node{ // move existed leaf
			P:         n,
			Key:       n.Key,
			Value:     n.Value,
			isStorage: n.isStorage,
			hash:      n.hash,
		}
	}
	newLeaf.P = n

	switch restKeyPath[0] {
	case 0:
		n.LPrefix, n.L = restKeyPath, newLeaf
		n.RPrefix, n.R = detachedPath, movedNode
	case 1:
		n.LPrefix, n.L = detachedPath, movedNode
		n.RPrefix, n.R = restKeyPath, newLeaf
	}

	// node become extended, reset key and value
	n.Key, n.Value, n.hash, n.isStorage = nil, nil, nil, false
	hasher(n, commonPath, 0)

	nodeTouch := uint16(1 << pathToNode[len(pathToNode)-1])
	nodeAfter := uint16(3) // both child has been updated
	updates := make(map[string][]byte, 3)

	hasher(n, bitstring{}, 0)

	updates[pathToNode.String()] = encodeNodeUpdate(n, nodeTouch, nodeAfter, nil)

	rtouch := uint16(1 << restKeyPath[0])
	updates[append(pathToNode, restKeyPath[0]).String()] = encodeNodeUpdate(newLeaf, rtouch, rtouch, nil)

	if n.P == nil {
		// commonPath should be set to RootNode.CommonPrefix outside the function
		return updates
	}

	if len(commonPath) > 0 {
		switch commonPath[0] {
		case 1:
			//if n.P != nil {
			n.P.RPrefix = commonPath
			//}
			//n.RPrefix = commonPath
		case 0:
			//if n.P != nil {
			n.P.LPrefix = commonPath
			//}
			//n.LPrefix = commonPath
		}
	}
	return updates
}

func (n *RootNode) Walk(path bitstring, fn func(cd *Node)) (nodePath, pathRest bitstring, splitAt int, current *Node) {
	nodePath = n.CommonPrefix

	var bit uint8
	var equal bool
	for current = n.Node; current != nil; {
		fn(current)

		splitAt, bit, equal = nodePath.splitPoint(path)
		if equal {
			return bitstring{}, bitstring{}, 0, current
		}

		if splitAt < len(nodePath) {
			return nodePath, path[splitAt:], splitAt, current
		}

		if splitAt == 0 || splitAt == len(nodePath) {
			path = path[splitAt:]

			switch bit {
			case 1:
				if current.R == nil {
					return nodePath, path, splitAt, current
				}
				nodePath = current.RPrefix
				current = current.R
			case 0:
				if current.L == nil {
					return nodePath, path, splitAt, current
				}
				nodePath = current.LPrefix
				current = current.L
			}

			continue
		}
		break
	}
	return nodePath, path, splitAt, current
}

func (n *Node) deleteChild(child *Node) bitstring {
	var melt *Node
	var meltPrefix bitstring

	// remove child data
	switch child {
	case n.L:
		n.L, n.LPrefix = nil, nil
		melt = n.R
		meltPrefix = n.RPrefix
	case n.R:
		n.R, n.RPrefix = nil, nil
		melt = n.L
		meltPrefix = n.LPrefix
	default:
		panic("could delete only child nodes")
	}
	melt.P = n.P

	// merge parent path to skip this half-branch node
	if n.P != nil {
		switch {
		case n.P.L == n:
			n.P.L, n.P.LPrefix = melt, append(n.P.LPrefix, meltPrefix...)
		case n.P.R == n:
			n.P.R, n.P.RPrefix = melt, append(n.P.RPrefix, meltPrefix...)
		default:
			panic("failed to merge parent path")
		}
	} else { // n is root
		n.LPrefix, n.RPrefix = melt.LPrefix, melt.RPrefix
		n.Key = melt.Key
		n.Value = melt.Value
		n.L, n.R, n.Value = melt.L, melt.R, melt.Value
		n.hash = n.hash[:0]
	}
	return meltPrefix
}

func (t *BinPatriciaTrie) StatString() string {
	s := t.stat
	return fmt.Sprintf("hashes_total %d nodes %d", s.hashesTotal, s.nodesTotal)
}

func (t *BinPatriciaTrie) rootHashWithUpdates() map[string][]byte {
	//if t.root == nil {
	//	return EmptyRootHash, nil
	//}
	//return t.hash(t.root.Node, t.root.CommonPrefix, 0), nil
	updates := make(map[string][]byte)
	t.hashWithUpdates(t.root.Node, t.root.CommonPrefix, &updates)
	return updates
}

func (t *BinPatriciaTrie) hashWithUpdates(n *Node, pref bitstring, updates *map[string][]byte) ([]byte, []byte) {
	if len(n.hash) == 32 {
		return n.hash, nil
	}
	t.keccak.Reset()

	t.stat.hashesTotal++

	var hash []byte
	if n.Value == nil {
		// This is a branch node, so the rule is
		// branch_hash = hash(left_root_hash || right_root_hash)
		lkey := bitstring(make([]byte, len(pref)+len(n.LPrefix)))
		copy(lkey, pref)
		copy(lkey[len(pref):], n.LPrefix)

		rkey := bitstring(make([]byte, len(pref)+len(n.RPrefix)))
		copy(rkey, pref)
		copy(rkey[len(pref):], n.RPrefix)

		lh, lupd := t.hashWithUpdates(n.L, lkey, updates)
		rh, rupd := t.hashWithUpdates(n.R, rkey, updates)
		t.keccak.Write(lh)
		t.keccak.Write(rh)
		hash = t.keccak.Sum(nil)
		if len(lupd) > 0 {
			binary.BigEndian.PutUint16(lupd[0:], 1)
			(*updates)[lkey.String()] = lupd
		}
		if len(rupd) > 0 {
			binary.BigEndian.PutUint16(rupd[0:], 2)
			(*updates)[rkey.String()] = rupd
		}

		if t.trace {
			fmt.Printf("branch %v (%v|%v)\n", hex.EncodeToString(hash), hex.EncodeToString(lh), hex.EncodeToString(rh))
		}
		t.keccak.Reset()
	} else {
		// This is a leaf node, so the hashing rule is
		// leaf_hash = hash(hash(key) || hash(leaf_value))
		t.keccak.Write(n.Key)
		kh := t.keccak.Sum(nil)
		t.keccak.Reset()

		t.keccak.Write(n.Value)
		hash = t.keccak.Sum(nil)
		t.keccak.Reset()

		t.keccak.Write(kh)
		t.keccak.Write(hash)
		hash = t.keccak.Sum(nil)
		t.keccak.Reset()

		if t.trace {
			fmt.Printf("leaf   %v\n", hex.EncodeToString(hash))
		}
	}

	n.hash = hash
	upd := encodeNodeUpdate(n, 0, 3, nil)
	if n.P == nil {
		binary.BigEndian.PutUint16(upd[0:], 3)
		(*updates)[pref.String()] = upd
	}

	return hash, upd
}
func (t *BinPatriciaTrie) hash(n *Node, pref bitstring, off int) []byte {
	t.keccak.Reset()
	t.stat.hashesTotal++

	if len(n.hash) == 32 && n.P != nil {
		return n.hash
	}

	var hash []byte
	if n.Value == nil {
		// This is a branch node, so the rule is
		// branch_hash = hash(left_root_hash || right_root_hash)
		lh := t.hash(n.L, n.LPrefix, off+len(pref))
		rh := t.hash(n.R, n.RPrefix, off+len(pref))
		t.keccak.Write(lh)
		t.keccak.Write(rh)
		hash = t.keccak.Sum(nil)
		if t.trace {
			fmt.Printf("branch %v (%v|%v)\n", hex.EncodeToString(hash), hex.EncodeToString(lh), hex.EncodeToString(rh))
		}
		t.keccak.Reset()
	} else {
		// This is a leaf node, so the hashing rule is
		// leaf_hash = hash(hash(key) || hash(leaf_value))
		t.keccak.Write(n.Key)
		kh := t.keccak.Sum(nil)
		t.keccak.Reset()

		t.keccak.Write(n.Value)
		hash = t.keccak.Sum(nil)
		t.keccak.Reset()

		t.keccak.Write(kh)
		t.keccak.Write(hash)
		hash = t.keccak.Sum(nil)
		t.keccak.Reset()

		if t.trace {
			fmt.Printf("leaf   %v\n", hex.EncodeToString(hash))
		}
	}

	//if len(pref) > 1 {
	//	fpLen := len(pref) + off
	//	t.keccak.Write([]byte{byte(fpLen), byte(fpLen >> 8)})
	//	t.keccak.Write(zero30)
	//	t.keccak.Write(hash)
	//
	//	hash = t.keccak.Sum(nil)
	//	t.keccak.Reset()
	//}
	//if t.trace {
	//	fmt.Printf("hash   %v off %d, pref %d\n", hex.EncodeToString(hash), off, len(pref))
	//}
	n.hash = hash

	return hash
}

var Zero30 = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

type bitstring []uint8

func hexToBin(hex []byte) bitstring {
	bin := make([]byte, 4*len(hex))
	for i := range bin {
		if hex[i/4]&(1<<(3-i%4)) != 0 {
			bin[i] = 1
		}
	}
	return bin
}

func newBitstring(key []byte) bitstring {
	bits := make([]byte, 8*len(key))
	for i := range bits {
		if key[i/8]&(1<<(7-i%7)) != 0 {
			bits[i] = 1
		}
	}
	return bits
}

func bitstringWithPadding(key []byte, _ int) bitstring {
	bs := newBitstring(key)
	if last := key[len(key)-1]; last&0xf0 != 0 {
		padding := int(0xf0 ^ last)
		bs = bs[:len(bs)-8-padding]
	}
	// bs = bs[:len(bs)-padding-1]
	return bs
}

func (b bitstring) String() string {
	var s string
	for _, bit := range b {
		switch bit {
		case 1:
			s += "1"
		case 0:
			s += "0"
		default:
			panic(fmt.Errorf("invalid bit %d in bitstring", bit))

		}
	}
	return s
}

func (b bitstring) splitPoint(other bitstring) (at int, bit byte, equal bool) {
	for ; at < len(b) && at < len(other); at++ {
		if b[at] != other[at] {
			return at, other[at], false
		}
	}

	switch {
	case len(b) == len(other):
		return 0, 0, true
	case at == len(b): // b ends before other
		return at, other[at], false
	case at == len(other): // other ends before b
		return at, b[at], false
	default:
		panic("oro")
	}
}

// Converts b into slice of bytes.
// if len of b is not a multiple of 8, we add 1 <= padding <= 7 zeros to the latest byte
// and return amount of added zeros
func (b bitstring) reconstructHex() (re []byte, padding int) {
	re = make([]byte, len(b)/8)

	var offt, i int
	for {
		bt, ok := b.readByte(offt)
		if !ok {
			break
		}
		re[i] = bt
		offt += 8
		i++
	}

	if offt >= len(b) {
		return re, 0
	}

	padding = offt + 8 - len(b)
	pd := append(b[offt:], bytes.Repeat([]byte{0}, padding)...)

	last, ok := pd.readByte(0)
	if !ok {
		panic(fmt.Errorf("reconstruct failed: padding %d padded size %d", padding, len(pd)))
	}
	pad := byte(padding | 0xf0)
	return append(re, last, pad), padding
}

func (b bitstring) readByte(offsetBits int) (byte, bool) {
	if len(b) <= offsetBits+7 {
		return 0, false
	}
	return b[offsetBits+7] | b[offsetBits+6]<<1 | b[offsetBits+5]<<2 | b[offsetBits+4]<<3 | b[offsetBits+3]<<4 | b[offsetBits+2]<<5 | b[offsetBits+1]<<6 | b[offsetBits]<<7, true
}

// ExtractPlainKeys parses branchData and extract the plain keys for accounts and storage in the same order
// they appear witjin the branchData
func ExtractBinPlainKeys(branchData []byte) (accountPlainKeys [][]byte, storagePlainKeys [][]byte, err error) {
	storagePlainKeys = make([][]byte, 0)
	accountPlainKeys = make([][]byte, 0)

	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4

	for bitset, noop := touchMap&afterMap, 0; bitset != 0; noop++ {
		if pos >= len(branchData) {
			break
		}
		bit := bitset & -bitset

		fieldBits := PartFlags(branchData[pos])
		pos++

		if fieldBits&ACCOUNT_PLAIN_PART > 0 {
			n, aux, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				return nil, nil, fmt.Errorf("extractBinPlainKeys: [%x] account %w", branchData, err)
			}
			accountPlainKeys = append(accountPlainKeys, aux)
			pos += n
		}

		if fieldBits&STORAGE_PLAIN_PART > 0 {
			n, aux, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				return nil, nil, fmt.Errorf("extractBinPlainKeys: storage %w", err)
			}
			storagePlainKeys = append(storagePlainKeys, aux)
			pos += n
		}
		if fieldBits&HASH_PART > 0 {
			n, _, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				return nil, nil, fmt.Errorf("extractBinPlainKeys: hash %w", err)
			}
			pos += n
		}
		bitset ^= bit
	}
	return
}

func decodeSizedBuffer(buf []byte) (int, []byte, error) {
	sz, n := binary.Uvarint(buf)
	switch {
	case n == 0:
		return 0, nil, fmt.Errorf("buffer size too small")
	case n < 0:
		return 0, nil, fmt.Errorf("value overflow")
	default:
	}
	size := int(sz)
	if len(buf) < n+size {
		return n, []byte{}, fmt.Errorf("encoded size larger than buffer size")
	}
	return n + size, buf[n : n+size], nil
}

func ReplaceBinPlainKeys(branchData []byte, accountPlainKeys [][]byte, storagePlainKeys [][]byte, newData []byte) ([]byte, error) {
	var numBuf [binary.MaxVarintLen64]byte
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4
	if cap(newData) < 4 {
		newData = make([]byte, 4)
	}
	copy(newData, branchData[:4])

	var accountI, storageI int
	for bitset, noop := touchMap&afterMap, 0; bitset != 0; noop++ {
		if pos >= len(branchData) {
			break
		}
		bit := bitset & -bitset

		fieldBits := PartFlags(branchData[pos])
		newData = append(newData, byte(fieldBits))
		pos++

		if fieldBits == 0 {
			continue
		}

		if fieldBits&ACCOUNT_PLAIN_PART > 0 {
			ptr, _, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				return nil, fmt.Errorf("replaceBinPlainKeys: account %w", err)
			}
			n := binary.PutUvarint(numBuf[:], uint64(len(accountPlainKeys[accountI])))
			newData = append(newData, numBuf[:n]...)
			newData = append(newData, accountPlainKeys[accountI]...)
			accountI++
			pos += ptr
		}
		if fieldBits&STORAGE_PLAIN_PART > 0 {
			ptr, _, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				return nil, fmt.Errorf("replaceBinPlainKeys: storage %w", err)
			}
			n := binary.PutUvarint(numBuf[:], uint64(len(storagePlainKeys[storageI])))
			newData = append(newData, numBuf[:n]...)
			newData = append(newData, storagePlainKeys[storageI]...)
			storageI++
			pos += ptr
		}
		if fieldBits&HASH_PART > 0 {
			n, _, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				return nil, fmt.Errorf("extractBinPlainKeys: hash %w", err)
			}
			newData = append(newData, branchData[pos:pos+n]...)
			pos += n
		}
		bitset ^= bit
	}
	return newData, nil
}

func branchToString2(branchData []byte) string {
	if len(branchData) == 0 {
		return "{ DELETED }"
	}
	touchMap := binary.BigEndian.Uint16(branchData[0:])
	afterMap := binary.BigEndian.Uint16(branchData[2:])
	pos := 4

	var sb strings.Builder
	fmt.Fprintf(&sb, "touchMap %016b, afterMap %016b\n", touchMap, afterMap)

	for bitset, noop := touchMap&afterMap, 0; bitset != 0; noop++ {
		if pos >= len(branchData) {
			break
		}
		bit := bitset & -bitset
		if pos >= len(branchData) {
			break
		}

		fieldBits := PartFlags(branchData[pos])
		pos++

		sb.WriteString("{")
		if fieldBits&ACCOUNT_PLAIN_PART > 0 {
			n, pk, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(&sb, "accountPlainKey=[%x]", pk)
			pos += n

		}
		if fieldBits&STORAGE_PLAIN_PART > 0 {
			n, pk, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(&sb, "storagePlainKey=[%x]", pk)
			pos += n
		}
		if fieldBits&HASH_PART > 0 {
			n, hash, err := decodeSizedBuffer(branchData[pos:])
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(&sb, " hash=[%x]", hash)
			pos += n
		}
		sb.WriteString(" }\n")
		bitset ^= bit
	}
	return sb.String()
}
