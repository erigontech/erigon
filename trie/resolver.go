package trie

import (
	"bytes"
	"fmt"
	"math/big"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var emptyHash [32]byte

func (t *Trie) Rebuild(db ethdb.Database, blockNr uint64) hashNode {
	if t.root == nil {
		return nil
	}
	n, ok := t.root.(hashNode)
	if !ok {
		panic("Expected hashNode")
	}
	root, roothash, err := t.rebuildHashes(db, nil, 0, blockNr, true, n)
	if err != nil {
		panic(err)
	}
	if bytes.Equal(roothash, n) {
		t.root = root
		log.Info("Rebuilt hashfile and verified", "root hash", roothash)
	} else {
		log.Error(fmt.Sprintf("Could not rebuild %s vs %s\n", roothash, n))
	}
	t.timestampSubTree(t.root, blockNr)
	return roothash
}

const Levels = 104

type ResolveHexes [][]byte

// ResolveHexes implements sort.Interface
func (rh ResolveHexes) Len() int {
	return len(rh)
}

func (rh ResolveHexes) Less(i, j int) bool {
	return bytes.Compare(rh[i], rh[j]) < 0
}

func (rh ResolveHexes) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
}

/* One resolver per trie (prefix) */
type TrieResolver struct {
	accounts      bool         // Is this a resolver for accounts or for storage
	dbw           ethdb.Putter // For updating hashes
	hashes        bool
	continuations []*TrieContinuation
	resolveHexes  ResolveHexes
	rhIndexLte    int // index in resolveHexes with resolve key less or equal to the current key
	// if the current key is less than the first resolve key, this index is -1
	rhIndexGt int // index in resolveHexes with resolve key greater than the current key
	// if the current key is greater than the last resolve key, this index is len(resolveHexes)
	contIndices []int // Indices pointing back to continuation array from arrays retured by PrepareResolveParams
	key_array   [52]byte
	key         []byte
	value       []byte
	key_set     bool
	nodeStack   [Levels + 1]shortNode
	vertical    [Levels + 1]fullNode
	fillCount   [Levels + 1]int
	startLevel  int
	keyIdx      int
	h           *hasher
	historical  bool
}

func NewResolver(dbw ethdb.Putter, hashes bool, accounts bool) *TrieResolver {
	tr := TrieResolver{
		accounts:      accounts,
		dbw:           dbw,
		hashes:        hashes,
		continuations: []*TrieContinuation{},
		resolveHexes:  [][]byte{},
		rhIndexLte:    -1,
		rhIndexGt:     0,
		contIndices:   []int{},
	}
	return &tr
}

func (tr *TrieResolver) SetHistorical(h bool) {
	tr.historical = h
}

// TrieResolver implements sort.Interface
func (tr *TrieResolver) Len() int {
	return len(tr.continuations)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (tr *TrieResolver) Less(i, j int) bool {
	ci := tr.continuations[i]
	cj := tr.continuations[j]
	m := min(ci.resolvePos, cj.resolvePos)
	c := bytes.Compare(ci.t.prefix, cj.t.prefix)
	if c != 0 {
		return c < 0
	}
	c = bytes.Compare(ci.resolveKey[:m], cj.resolveKey[:m])
	if c != 0 {
		return c < 0
	}
	return ci.resolvePos < cj.resolvePos
}

func (tr *TrieResolver) Swap(i, j int) {
	tr.continuations[i], tr.continuations[j] = tr.continuations[j], tr.continuations[i]
}

func (tr *TrieResolver) AddContinuation(c *TrieContinuation) {
	tr.continuations = append(tr.continuations, c)
	if c.t.prefix == nil {
		tr.resolveHexes = append(tr.resolveHexes, c.resolveKey)
	} else {
		tr.resolveHexes = append(tr.resolveHexes, append(keybytesToHex(c.t.prefix)[:40], c.resolveKey...))
	}
}

func (tr *TrieResolver) Print() {
	for _, c := range tr.continuations {
		fmt.Printf("%s\n", c.String())
	}
}

// Prepares information for the MultiWalk
func (tr *TrieResolver) PrepareResolveParams() ([][]byte, []uint) {
	// Remove continuations strictly contained in the preceeding ones
	startkeys := [][]byte{}
	fixedbits := []uint{}
	if len(tr.continuations) == 0 {
		return startkeys, fixedbits
	}
	sort.Stable(tr)
	sort.Sort(tr.resolveHexes)
	newHexes := [][]byte{}
	for i, h := range tr.resolveHexes {
		if i == len(tr.resolveHexes)-1 || !bytes.HasPrefix(tr.resolveHexes[i+1], h) {
			newHexes = append(newHexes, h)
		}
	}
	tr.resolveHexes = newHexes
	var prevC *TrieContinuation
	for i, c := range tr.continuations {
		if prevC == nil || c.resolvePos < prevC.resolvePos ||
			!bytes.Equal(c.t.prefix, prevC.t.prefix) ||
			!bytes.HasPrefix(c.resolveKey[:c.resolvePos], prevC.resolveKey[:prevC.resolvePos]) {
			tr.contIndices = append(tr.contIndices, i)
			pLen := len(c.t.prefix)
			key := make([]byte, pLen+32)
			copy(key[:], c.t.prefix)
			decodeNibbles(c.resolveKey[:c.resolvePos], key[pLen:])
			startkeys = append(startkeys, key)
			c.extResolvePos = c.resolvePos + 2*pLen
			fixedbits = append(fixedbits, uint(4*c.extResolvePos))
			prevC = c
		}
	}
	tr.startLevel = tr.continuations[0].extResolvePos
	return startkeys, fixedbits
}

func (tr *TrieResolver) finishPreviousKey(k []byte) error {
	pLen := prefixLen(k, tr.key)
	stopLevel := 2 * pLen
	if k != nil && (k[pLen]^tr.key[pLen])&0xf0 == 0 {
		stopLevel++
	}
	tc := tr.continuations[tr.contIndices[tr.keyIdx]]
	startLevel := tr.startLevel
	if startLevel < tc.extResolvePos {
		startLevel = tc.extResolvePos
	}
	if startLevel < stopLevel {
		startLevel = stopLevel
	}
	hex := keybytesToHex(tr.key)
	tr.nodeStack[startLevel+1].Key = hexToCompact(hex[startLevel+1:])
	tr.nodeStack[startLevel+1].Val = valueNode(tr.value)
	tr.nodeStack[startLevel+1].flags.dirty = true
	tr.fillCount[startLevel+1] = 1
	// Adjust rhIndices if needed
	if tr.rhIndexGt < tr.resolveHexes.Len() {
		resComp := bytes.Compare(hex, tr.resolveHexes[tr.rhIndexGt])
		for tr.rhIndexGt < tr.resolveHexes.Len() && resComp != -1 {
			tr.rhIndexGt++
			tr.rhIndexLte++
			if tr.rhIndexGt < tr.resolveHexes.Len() {
				resComp = bytes.Compare(hex, tr.resolveHexes[tr.rhIndexGt])
			}
		}
	}
	var rhPrefixLen int
	if tr.rhIndexLte >= 0 {
		rhPrefixLen = prefixLen(hex, tr.resolveHexes[tr.rhIndexLte])
	}
	if tr.rhIndexGt < tr.resolveHexes.Len() {
		rhPrefixLenGt := prefixLen(hex, tr.resolveHexes[tr.rhIndexGt])
		if rhPrefixLenGt > rhPrefixLen {
			rhPrefixLen = rhPrefixLenGt
		}
	}
	for level := startLevel; level >= stopLevel; level-- {
		keynibble := hex[level]
		onResolvingPath := level <= rhPrefixLen // <= instead of < to be able to resolve deletes in one go
		if tr.fillCount[level+1] == 1 {
			// Short node, needs to be promoted to the level above
			short := &tr.nodeStack[level+1]
			tr.vertical[level].Children[keynibble] = short.copy()
			tr.vertical[level].flags.dirty = true
			if tr.fillCount[level] == 0 {
				tr.nodeStack[level].Key = hexToCompact(append([]byte{keynibble}, compactToHex(short.Key)...))
				tr.nodeStack[level].Val = short.Val
				tr.nodeStack[level].flags.dirty = true
			}
			tr.fillCount[level]++
			if level >= tc.extResolvePos {
				tr.nodeStack[level+1].Key = nil
				tr.nodeStack[level+1].Val = nil
				tr.nodeStack[level+1].flags.dirty = true
				tr.fillCount[level+1] = 0
				for i := 0; i < 17; i++ {
					tr.vertical[level+1].Children[i] = nil
				}
				tr.vertical[level+1].flags.dirty = true
			}
			continue
		}
		full := &tr.vertical[level+1]
		var storeHashTo common.Hash
		//full.flags.dirty = true
		hashLen := tr.h.hash(full, false, storeHashTo[:])
		if hashLen < 32 {
			panic("hashNode expected")
		}
		if tr.fillCount[level] == 0 {
			tr.nodeStack[level].Key = hexToCompact([]byte{keynibble})
			tr.nodeStack[level].flags.dirty = true
		}
		tr.vertical[level].flags.dirty = true
		if onResolvingPath || (tr.hashes && level == 5) {
			var c node
			if tr.fillCount[level+1] == 2 {
				c = full.duoCopy()
			} else {
				c = full.copy()
			}
			tr.vertical[level].Children[keynibble] = c
			if tr.fillCount[level] == 0 {
				tr.nodeStack[level].Val = c
			}
		} else {
			tr.vertical[level].Children[keynibble] = hashNode(storeHashTo[:])
			if tr.fillCount[level] == 0 {
				tr.nodeStack[level].Val = hashNode(storeHashTo[:])
			}
		}
		tr.fillCount[level]++
		if level >= tc.extResolvePos {
			tr.nodeStack[level+1].Key = nil
			tr.nodeStack[level+1].Val = nil
			tr.nodeStack[level+1].flags.dirty = true
			tr.fillCount[level+1] = 0
			for i := 0; i < 17; i++ {
				tr.vertical[level+1].Children[i] = nil
			}
			tr.vertical[level+1].flags.dirty = true
		}
	}
	tr.startLevel = stopLevel
	if k == nil {
		var root node
		//fmt.Printf("root fillCount %d\n", tr.fillCount[tc.resolvePos])
		if tr.fillCount[tc.extResolvePos] == 1 {
			root = tr.nodeStack[tc.extResolvePos].copy()
		} else if tr.fillCount[tc.extResolvePos] == 2 {
			root = tr.vertical[tc.extResolvePos].duoCopy()
		} else if tr.fillCount[tc.extResolvePos] > 2 {
			root = tr.vertical[tc.extResolvePos].copy()
		}
		if root == nil {
			return fmt.Errorf("Resolve returned nil root")
		}
		var gotHash common.Hash
		hashLen := tr.h.hash(root, tc.resolvePos == 0, gotHash[:])
		if hashLen == 32 {
			if !bytes.Equal(tc.resolveHash, gotHash[:]) {
				return fmt.Errorf("Resolving wrong hash for prefix %x, key %x, pos %d, \nexpected %s, got %s\n",
					tc.t.prefix,
					tc.resolveKey,
					tc.resolvePos,
					tc.resolveHash,
					hashNode(gotHash[:]),
				)
			}
		} else {
			if tc.resolveHash != nil {
				return fmt.Errorf("Resolving wrong hash for key %x, pos %d\nexpected %s, got embedded node\n",
					tc.resolveKey,
					tc.resolvePos,
					tc.resolveHash)
			}
		}
		tc.resolved = root
		for i := 0; i <= Levels; i++ {
			tr.nodeStack[i].Key = nil
			tr.nodeStack[i].Val = nil
			tr.nodeStack[i].flags.dirty = true
			for j := 0; j < 17; j++ {
				tr.vertical[i].Children[j] = nil
			}
			tr.vertical[i].flags.dirty = true
			tr.fillCount[i] = 0
		}
		switch parent := tc.resolveParent.(type) {
		case nil:
			if _, ok := tc.t.root.(hashNode); ok {
				tc.t.root = root
			}
		case *shortNode:
			if _, ok := parent.Val.(hashNode); ok {
				parent.Val = root
			}
		case *duoNode:
			i1, i2 := parent.childrenIdx()
			switch tc.resolveKey[tc.resolvePos-1] {
			case i1:
				if _, ok := parent.child1.(hashNode); ok {
					parent.child1 = root
				}
			case i2:
				if _, ok := parent.child2.(hashNode); ok {
					parent.child2 = root
				}
			}
		case *fullNode:
			idx := tc.resolveKey[tc.resolvePos-1]
			if _, ok := parent.Children[idx].(hashNode); ok {
				parent.Children[idx] = root
			}
		}
	}
	return nil
}

type ExtAccount struct {
	Nonce   uint64
	Balance *big.Int
}
type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
}

var emptyCodeHash = crypto.Keccak256(nil)

func (tr *TrieResolver) Walker(keyIdx int, k []byte, v []byte) (bool, error) {
	//fmt.Printf("%d %x %x\n", keyIdx, k, v)
	if keyIdx != tr.keyIdx {
		if tr.key_set {
			if err := tr.finishPreviousKey(nil); err != nil {
				return false, err
			}
			tr.key_set = false
		}
		tr.keyIdx = keyIdx
	}
	if len(v) > 0 {
		// First, finish off the previous key
		if tr.key_set {
			if err := tr.finishPreviousKey(k); err != nil {
				return false, err
			}
		}
		// Remember the current key and value
		if tr.accounts {
			copy(tr.key_array[:], k[:32])
			tr.key = tr.key_array[:32]
		} else {
			copy(tr.key_array[:], k[:52])
			tr.key = tr.key_array[:52]
		}
		if tr.accounts {
			var data Account
			var err error
			if len(v) == 1 {
				data.Balance = new(big.Int)
				data.CodeHash = emptyCodeHash
				data.Root = emptyRoot
				if tr.value, err = rlp.EncodeToBytes(data); err != nil {
					return false, err
				}
			} else if len(v) < 60 {
				var extData ExtAccount
				if err = rlp.DecodeBytes(v, &extData); err != nil {
					return false, err
				}
				data.Nonce = extData.Nonce
				data.Balance = extData.Balance
				data.CodeHash = emptyCodeHash
				data.Root = emptyRoot
				if tr.value, err = rlp.EncodeToBytes(data); err != nil {
					return false, err
				}
			} else {
				tr.value = common.CopyBytes(v)
			}
		} else {
			tr.value = common.CopyBytes(v)
		}
		tr.key_set = true
	}
	return true, nil
}

func (tr *TrieResolver) ResolveWithDb(db ethdb.Database, blockNr uint64) error {
	tr.h = newHasher(!tr.accounts)
	defer returnHasherToPool(tr.h)
	startkeys, fixedbits := tr.PrepareResolveParams()
	var err error
	if db == nil {
		var b strings.Builder
		fmt.Fprintf(&b, "ResolveWithDb(db=nil), tr.acounts: %t\n", tr.accounts)
		for i, sk := range startkeys {
			fmt.Fprintf(&b, "sk %x, bits: %d\n", sk, fixedbits[i])
		}
		return fmt.Errorf("Unexpected resolution: %s at %s", b.String(), debug.Stack())
	}
	if tr.accounts {
		if tr.historical {
			err = db.MultiWalkAsOf([]byte("AT"), []byte("hAT"), startkeys, fixedbits, blockNr+1, tr.Walker)
		} else {
			err = db.MultiWalk([]byte("AT"), startkeys, fixedbits, tr.Walker)
		}
	} else {
		if tr.historical {
			err = db.MultiWalkAsOf([]byte("ST"), []byte("hST"), startkeys, fixedbits, blockNr+1, tr.Walker)
		} else {
			err = db.MultiWalk([]byte("ST"), startkeys, fixedbits, tr.Walker)
		}
	}
	return err
}

func (t *Trie) rebuildHashes(db ethdb.Database, key []byte, pos int, blockNr uint64, accounts bool, expected hashNode) (node, hashNode, error) {
	tc := t.NewContinuation(key, pos, expected)
	r := NewResolver(db, true, accounts)
	r.SetHistorical(t.historical)
	r.AddContinuation(tc)
	if err := r.ResolveWithDb(db, blockNr); err != nil {
		return nil, nil, err
	}
	return tc.resolved, expected, nil
}
