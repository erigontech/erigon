package trie

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	dbutils2 "github.com/ledgerwatch/erigon-lib/kv/dbutils"
	"math/bits"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	length2 "github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/rlphacks"
)

/*
**Theoretically:** "Merkle trie root calculation" starts from state, build from state keys - trie,
on each level of trie calculates intermediate hash of underlying data.

**Practically:** It can be implemented as "Preorder trie traversal" (Preorder - visit Root, visit Left, visit Right).
But, let's make couple observations to make traversal over huge state efficient.

**Observation 1:** `TrieOfAccounts` already stores state keys in sorted way.
Iteration over this bucket will retrieve keys in same order as "Preorder trie traversal".

**Observation 2:** each Eth block - changes not big part of state - it means most of Merkle trie intermediate hashes will not change.
It means we effectively can cache them. `TrieOfAccounts` stores "Intermediate hashes of all Merkle trie levels".
It also sorted and Iteration over `TrieOfAccounts` will retrieve keys in same order as "Preorder trie traversal".

**Implementation:** by opening 1 Cursor on state and 1 more Cursor on intermediate hashes bucket - we will receive data in
 order of "Preorder trie traversal". Cursors will only do "sequential reads" and "jumps forward" - been hardware-friendly.
1 stack keeps all accumulated hashes, when sub-trie traverse ends - all hashes pulled from stack -> hashed -> new hash puts on stack - it's hash of visited sub-trie (it emulates recursive nature of "Preorder trie traversal" algo).

Imagine that account with key 0000....00 (64 zeroes, 32 bytes of zeroes) changed.
Here is an example sequence which can be seen by running 2 Cursors:
```
00   // key which came from cache, can't use it - because account with this prefix changed
0000 // key which came from cache, can't use it - because account with this prefix changed
...
{30 zero bytes}00    // key which came from cache, can't use it - because account with this prefix changed
{30 zero bytes}0000  // Account which came from state, use it - calculate hash, jump to "next sub-trie"
{30 zero bytes}01    // key which came from cache, it is "next sub-trie", use it, jump to "next sub-trie"
{30 zero bytes}02    // key which came from cache, it is "next sub-trie", use it, jump to "next sub-trie"
...
{30 zero bytes}ff    // key which came from cache, it is "next sub-trie", use it, jump to "next sub-trie"
{29 zero bytes}01    // key which came from cache, it is "next sub-trie" (1 byte shorter key), use it, jump to "next sub-trie"
{29 zero bytes}02    // key which came from cache, it is "next sub-trie" (1 byte shorter key), use it, jump to "next sub-trie"
...
ff                   // key which came from cache, it is "next sub-trie" (1 byte shorter key), use it, jump to "next sub-trie"
nil                  // db returned nil - means no more keys there, done
```
On practice Trie is not full - it means after account key `{30 zero bytes}0000` may come `{5 zero bytes}01` and amount of iterations will not be big.

### Attack - by delete account with huge state

It's possible to create Account with very big storage (increase storage size during many blocks).
Then delete this account (SELFDESTRUCT).
 Naive storage deletion may take several minutes - depends on Disk speed - means every Eth client
 will not process any incoming block that time. To protect against this attack:
 PlainState, HashedState and IntermediateTrieHash buckets have "incarnations". Account entity has field "Incarnation" -
 just a digit which increasing each SELFDESTRUCT or CREATE2 opcodes. Storage key formed by:
 `{account_key}{incarnation}{storage_hash}`. And [turbo/trie/trie_root.go](../../turbo/trie/trie_root.go) has logic - every time
 when Account visited - we save it to `accAddrHashWithInc` variable and skip any Storage or IntermediateTrieHashes with another incarnation.
*/

// FlatDBTrieLoader reads state and intermediate trie hashes in order equal to "Preorder trie traversal"
// (Preorder - visit Root, visit Left, visit Right)
//
// It produces stream of values and send this stream to `receiver`
// It skips storage with incorrect incarnations
//
// Each intermediate hash key firstly pass to RetainDecider, only if it returns "false" - such AccTrie can be used.
type FlatDBTrieLoader struct {
	logPrefix          string
	trace              bool
	rd                 RetainDeciderWithMarker
	accAddrHashWithInc [40]byte // Concatenation of addrHash of the currently build account with its incarnation encoding

	ihSeek, accSeek, storageSeek []byte
	kHex, kHexS                  []byte

	// Account item buffer
	accountValue accounts.Account

	receiver *RootHashAggregator
	hc       HashCollector2
	shc      StorageHashCollector2
}

// RootHashAggregator - calculates Merkle trie root hash from incoming data stream
type RootHashAggregator struct {
	trace          bool
	wasIH          bool
	wasIHStorage   bool
	root           libcommon.Hash
	hc             HashCollector2
	shc            StorageHashCollector2
	currStorage    bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage    bytes.Buffer
	valueStorage   []byte // Current value to be used as the value tape for the hash builder
	hadTreeStorage bool
	hashAccount    libcommon.Hash // Current value to be used as the value tape for the hash builder
	hashStorage    libcommon.Hash // Current value to be used as the value tape for the hash builder
	curr           bytes.Buffer   // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ           bytes.Buffer
	currAccK       []byte
	value          []byte // Current value to be used as the value tape for the hash builder
	hadTreeAcc     bool
	groups         []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
	hasTree        []uint16
	hasHash        []uint16
	groupsStorage  []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
	hasTreeStorage []uint16
	hasHashStorage []uint16
	hb             *HashBuilder
	hashData       GenStructStepHashData
	a              accounts.Account
	leafData       GenStructStepLeafData
	accData        GenStructStepAccountData

	// Used to construct an Account proof while calculating the tree root.
	proofRetainer *ProofRetainer
	cutoff        bool
}

func NewRootHashAggregator() *RootHashAggregator {
	return &RootHashAggregator{
		hb: NewHashBuilder(false),
	}
}

func NewFlatDBTrieLoader(logPrefix string, rd RetainDeciderWithMarker, hc HashCollector2, shc StorageHashCollector2, trace bool) *FlatDBTrieLoader {
	if trace {
		fmt.Printf("----------\n")
		fmt.Printf("CalcTrieRoot\n")
	}
	return &FlatDBTrieLoader{
		logPrefix: logPrefix,
		receiver: &RootHashAggregator{
			hb:    NewHashBuilder(false),
			hc:    hc,
			shc:   shc,
			trace: trace,
		},
		ihSeek:      make([]byte, 0, 128),
		accSeek:     make([]byte, 0, 128),
		storageSeek: make([]byte, 0, 128),
		kHex:        make([]byte, 0, 128),
		kHexS:       make([]byte, 0, 128),
		rd:          rd,
		hc:          hc,
		shc:         shc,
	}
}

func (l *FlatDBTrieLoader) SetProofRetainer(pr *ProofRetainer) {
	l.receiver.proofRetainer = pr
}

// CalcTrieRoot algo:
//
//		for iterateIHOfAccounts {
//			if canSkipState
//	         goto SkipAccounts
//
//			for iterateAccounts from prevIH to currentIH {
//				use(account)
//				for iterateIHOfStorage within accountWithIncarnation{
//					if canSkipState
//						goto SkipStorage
//
//					for iterateStorage from prevIHOfStorage to currentIHOfStorage {
//						use(storage)
//					}
//	           SkipStorage:
//					use(ihStorage)
//				}
//			}
//	   SkipAccounts:
//			use(AccTrie)
//		}
func (l *FlatDBTrieLoader) CalcTrieRoot(tx kv.Tx, quit <-chan struct{}) (libcommon.Hash, error) {

	accC, err := tx.Cursor(kv.HashedAccounts)
	if err != nil {
		return EmptyRoot, err
	}
	defer accC.Close()
	accs := NewStateCursor(accC, quit)
	trieAccC, err := tx.Cursor(kv.TrieOfAccounts)
	if err != nil {
		return EmptyRoot, err
	}
	defer trieAccC.Close()
	trieStorageC, err := tx.CursorDupSort(kv.TrieOfStorage)
	if err != nil {
		return EmptyRoot, err
	}
	defer trieStorageC.Close()

	var canUse = func(prefix []byte) (bool, []byte) {
		retain, nextCreated := l.rd.RetainWithMarker(prefix)
		return !retain, nextCreated
	}
	accTrie := AccTrie(canUse, l.hc, trieAccC, quit)
	storageTrie := StorageTrie(canUse, l.shc, trieStorageC, quit)

	ss, err := tx.CursorDupSort(kv.HashedStorage)
	if err != nil {
		return EmptyRoot, err
	}
	defer ss.Close()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	for ihK, ihV, hasTree, err := accTrie.AtPrefix(nil); ; ihK, ihV, hasTree, err = accTrie.Next() { // no loop termination is at he end of loop
		if err != nil {
			return EmptyRoot, err
		}
		var firstPrefix []byte
		var done bool
		if accTrie.SkipState {
			goto SkipAccounts
		}

		firstPrefix, done = accTrie.FirstNotCoveredPrefix()
		if done {
			goto SkipAccounts
		}

		for k, kHex, v, err1 := accs.Seek(firstPrefix); k != nil; k, kHex, v, err1 = accs.Next() {
			if err1 != nil {
				return EmptyRoot, err1
			}
			if keyIsBefore(ihK, kHex) {
				break
			}
			if err = l.accountValue.DecodeForStorage(v); err != nil {
				return EmptyRoot, fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			if err = l.receiver.Receive(AccountStreamItem, kHex, nil, &l.accountValue, nil, nil, false, 0); err != nil {
				return EmptyRoot, err
			}
			if l.accountValue.Incarnation == 0 {
				continue
			}
			copy(l.accAddrHashWithInc[:], k)
			binary.BigEndian.PutUint64(l.accAddrHashWithInc[32:], l.accountValue.Incarnation)
			accWithInc := l.accAddrHashWithInc[:]
			for ihKS, ihVS, hasTreeS, err2 := storageTrie.SeekToAccount(accWithInc); ; ihKS, ihVS, hasTreeS, err2 = storageTrie.Next() {
				if err2 != nil {
					return EmptyRoot, err2
				}

				if storageTrie.skipState {
					goto SkipStorage
				}

				firstPrefix, done = storageTrie.FirstNotCoveredPrefix()
				if done {
					goto SkipStorage
				}

				for vS, err3 := ss.SeekBothRange(accWithInc, firstPrefix); vS != nil; _, vS, err3 = ss.NextDup() {
					if err3 != nil {
						return EmptyRoot, err3
					}
					hexutil.DecompressNibbles(vS[:32], &l.kHexS)
					if keyIsBefore(ihKS, l.kHexS) { // read until next AccTrie
						break
					}
					if err = l.receiver.Receive(StorageStreamItem, accWithInc, l.kHexS, nil, vS[32:], nil, false, 0); err != nil {
						return EmptyRoot, err
					}
				}

			SkipStorage:
				if ihKS == nil { // Loop termination
					break
				}

				if err = l.receiver.Receive(SHashStreamItem, accWithInc, ihKS, nil, nil, ihVS, hasTreeS, 0); err != nil {
					return EmptyRoot, err
				}
				if len(ihKS) == 0 { // means we just sent acc.storageRoot
					break
				}
			}

			select {
			default:
			case <-logEvery.C:
				l.logProgress(k, ihK)
			}
		}

	SkipAccounts:
		if ihK == nil { // Loop termination
			break
		}

		if err = l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV, hasTree, 0); err != nil {
			return EmptyRoot, err
		}
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, false, 0); err != nil {
		return EmptyRoot, err
	}
	return l.receiver.Root(), nil
}

func (l *FlatDBTrieLoader) logProgress(accountKey, ihK []byte) {
	var k string
	if accountKey != nil {
		k = makeCurrentKeyStr(accountKey)
	} else if ihK != nil {
		k = makeCurrentKeyStr(ihK)
	}
	log.Info(fmt.Sprintf("[%s] Calculating Merkle root", l.logPrefix), "current key", k)
}

func (r *RootHashAggregator) RetainNothing(_ []byte) bool {
	return false
}

func (r *RootHashAggregator) Receive(itemType StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	hasTree bool,
	cutoff int,
) error {
	//r.traceIf("9c3dc2561d472d125d8f87dde8f2e3758386463ade768ae1a1546d34101968bb", "00")
	//if storageKey == nil {
	//	//if bytes.HasPrefix(accountKey, common.FromHex("08050d07")) {
	//	fmt.Printf("1: %d, %x, %x\n", itemType, accountKey, hash)
	//	//}
	//} else {
	//	//if bytes.HasPrefix(accountKey, common.FromHex("876f5a0f54b30254d2bad26bb5a8da19cbe748fd033004095d9c96c8e667376b")) && bytes.HasPrefix(storageKey, common.FromHex("")) {
	//	//fmt.Printf("%x\n", storageKey)
	//	fmt.Printf("1: %d, %x, %x, %x\n", itemType, accountKey, storageKey, hash)
	//	//}
	//}
	//

	switch itemType {
	case StorageStreamItem:
		if len(r.currAccK) == 0 {
			r.currAccK = append(r.currAccK[:0], accountKey...)
		}
		r.advanceKeysStorage(storageKey, true /* terminator */)
		if r.currStorage.Len() > 0 {
			if err := r.genStructStorage(); err != nil {
				return err
			}
		}
		r.saveValueStorage(false, hasTree, storageValue, hash)
	case SHashStreamItem:
		if len(storageKey) == 0 { // this is ready-to-use storage root - no reason to call GenStructStep, also GenStructStep doesn't support empty prefixes
			r.hb.hashStack = append(append(r.hb.hashStack, byte(80+length2.Hash)), hash...)
			r.hb.nodeStack = append(r.hb.nodeStack, nil)
			r.accData.FieldSet |= AccountFieldStorageOnly
			break
		}
		if len(r.currAccK) == 0 {
			r.currAccK = append(r.currAccK[:0], accountKey...)
		}
		r.advanceKeysStorage(storageKey, false /* terminator */)
		if r.currStorage.Len() > 0 {
			if err := r.genStructStorage(); err != nil {
				return err
			}
		}
		r.saveValueStorage(true, hasTree, storageValue, hash)
	case AccountStreamItem:
		r.advanceKeysAccount(accountKey, true /* terminator */)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(0)
			if r.currStorage.Len() > 0 {
				if err := r.genStructStorage(); err != nil {
					return err
				}
			}
			if r.currStorage.Len() > 0 {
				r.groupsStorage = r.groupsStorage[:0]
				r.hasTreeStorage = r.hasTreeStorage[:0]
				r.hasHashStorage = r.hasHashStorage[:0]
				r.currStorage.Reset()
				r.succStorage.Reset()
				r.wasIHStorage = false
				// There are some storage items
				r.accData.FieldSet |= AccountFieldStorageOnly
			}
		}
		r.currAccK = r.currAccK[:0]
		if r.curr.Len() > 0 {
			if err := r.genStructAccount(); err != nil {
				return err
			}
		}
		if err := r.saveValueAccount(false, hasTree, accountValue, hash); err != nil {
			return err
		}
	case AHashStreamItem:
		r.advanceKeysAccount(accountKey, false /* terminator */)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(0)
			if r.currStorage.Len() > 0 {
				if err := r.genStructStorage(); err != nil {
					return err
				}
			}
			if r.currStorage.Len() > 0 {
				r.groupsStorage = r.groupsStorage[:0]
				r.hasTreeStorage = r.hasTreeStorage[:0]
				r.hasHashStorage = r.hasHashStorage[:0]
				r.currStorage.Reset()
				r.succStorage.Reset()
				r.wasIHStorage = false
				// There are some storage items
				r.accData.FieldSet |= AccountFieldStorageOnly
			}
		}
		r.currAccK = r.currAccK[:0]
		if r.curr.Len() > 0 {
			if err := r.genStructAccount(); err != nil {
				return err
			}
		}
		if err := r.saveValueAccount(true, hasTree, accountValue, hash); err != nil {
			return err
		}
	case CutoffStreamItem:
		if r.trace {
			fmt.Printf("storage cuttoff %d\n", cutoff)
		}
		r.cutoffKeysAccount(cutoff)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(0)
			if r.currStorage.Len() > 0 {
				if err := r.genStructStorage(); err != nil {
					return err
				}
			}
			if r.currStorage.Len() > 0 {
				r.groupsStorage = r.groupsStorage[:0]
				r.hasTreeStorage = r.hasTreeStorage[:0]
				r.hasHashStorage = r.hasHashStorage[:0]
				r.currStorage.Reset()
				r.succStorage.Reset()
				r.wasIHStorage = false
				// There are some storage items
				r.accData.FieldSet |= AccountFieldStorageOnly
			}
		}

		// Used for optional GetProof calculation to trigger inclusion of the top-level node
		r.cutoff = true

		if r.curr.Len() > 0 {
			if err := r.genStructAccount(); err != nil {
				return err
			}
		}
		if r.curr.Len() > 0 {
			if len(r.groups) > cutoff {
				r.groups = r.groups[:cutoff]
				r.hasTree = r.hasTree[:cutoff]
				r.hasHash = r.hasHash[:cutoff]
			}
		}
		if r.hb.hasRoot() {
			r.root = r.hb.rootHash()
		} else {
			r.root = EmptyRoot
		}
		r.groups = r.groups[:0]
		r.hasTree = r.hasTree[:0]
		r.hasHash = r.hasHash[:0]
		r.hb.Reset()
		r.wasIH = false
		r.wasIHStorage = false
		r.curr.Reset()
		r.succ.Reset()
		r.currStorage.Reset()
		r.succStorage.Reset()
	}
	return nil
}

//nolint
// func (r *RootHashAggregator) traceIf(acc, st string) {
// 	// "succ" - because on this iteration this "succ" will become "curr"
// 	if r.succStorage.Len() == 0 {
// 		var accNibbles []byte
// 		hexutil.DecompressNibbles(common.FromHex(acc), &accNibbles)
// 		r.trace = bytes.HasPrefix(r.succ.Bytes(), accNibbles)
// 	} else {
// 		r.trace = bytes.HasPrefix(r.currAccK, common.FromHex(acc)) && bytes.HasPrefix(r.succStorage.Bytes(), common.FromHex(st))
// 	}
// }

func (r *RootHashAggregator) Root() libcommon.Hash {
	return r.root
}

func (r *RootHashAggregator) advanceKeysStorage(k []byte, terminator bool) {
	r.currStorage.Reset()
	r.currStorage.Write(r.succStorage.Bytes())
	r.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	r.succStorage.Write(k)

	if terminator {
		r.succStorage.WriteByte(16)
	}
}

func (r *RootHashAggregator) cutoffKeysStorage(cutoff int) {
	r.currStorage.Reset()
	r.currStorage.Write(r.succStorage.Bytes())
	r.succStorage.Reset()
	//if r.currStorage.Len() > 0 {
	//r.succStorage.Write(r.currStorage.Bytes()[:cutoff-1])
	//r.succStorage.WriteByte(r.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
	//}
}

func (r *RootHashAggregator) genStructStorage() error {
	var err error
	var data GenStructStepData
	if r.wasIHStorage {
		r.hashData.Hash = r.hashStorage
		r.hashData.HasTree = r.hadTreeStorage
		data = &r.hashData
	} else {
		r.leafData.Value = rlphacks.RlpSerializableBytes(r.valueStorage)
		data = &r.leafData
	}
	var wantProof func(_ []byte) *proofElement
	if r.proofRetainer != nil {
		var fullKey [2 * (length.Hash + length.Incarnation + length.Hash)]byte
		for i, b := range r.currAccK {
			fullKey[i*2] = b / 16
			fullKey[i*2+1] = b % 16
		}
		for i, b := range binary.BigEndian.AppendUint64(nil, r.a.Incarnation) {
			fullKey[2*length.Hash+i*2] = b / 16
			fullKey[2*length.Hash+i*2+1] = b % 16
		}
		baseKeyLen := 2 * (length.Hash + length.Incarnation)
		wantProof = func(prefix []byte) *proofElement {
			copy(fullKey[baseKeyLen:], prefix)
			return r.proofRetainer.ProofElement(fullKey[:baseKeyLen+len(prefix)])
		}
	}
	r.groupsStorage, r.hasTreeStorage, r.hasHashStorage, err = GenStructStepEx(r.RetainNothing, r.currStorage.Bytes(), r.succStorage.Bytes(), r.hb, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if r.shc == nil {
			return nil
		}
		return r.shc(r.currAccK, keyHex, hasState, hasTree, hasHash, hashes, rootHash)
	}, data, r.groupsStorage, r.hasTreeStorage, r.hasHashStorage,
		r.trace,
		wantProof,
		r.cutoff,
	)
	if err != nil {
		return err
	}
	return nil
}

func (r *RootHashAggregator) saveValueStorage(isIH, hasTree bool, v, h []byte) {
	// Remember the current value
	r.wasIHStorage = isIH
	r.valueStorage = nil
	if isIH {
		r.hashStorage.SetBytes(h)
		r.hadTreeStorage = hasTree
	} else {
		r.valueStorage = v
	}
}

func (r *RootHashAggregator) advanceKeysAccount(k []byte, terminator bool) {
	r.curr.Reset()
	r.curr.Write(r.succ.Bytes())
	r.succ.Reset()
	r.succ.Write(k)
	if terminator {
		r.succ.WriteByte(16)
	}
}

func (r *RootHashAggregator) cutoffKeysAccount(cutoff int) {
	r.curr.Reset()
	r.curr.Write(r.succ.Bytes())
	r.succ.Reset()
	if r.curr.Len() > 0 && cutoff > 0 {
		r.succ.Write(r.curr.Bytes()[:cutoff-1])
		r.succ.WriteByte(r.curr.Bytes()[cutoff-1] + 1) // Modify last nibble before the cutoff point
	}
}

func (r *RootHashAggregator) genStructAccount() error {
	var data GenStructStepData
	if r.wasIH {
		r.hashData.Hash = r.hashAccount
		r.hashData.HasTree = r.hadTreeAcc
		data = &r.hashData
	} else {
		r.accData.Balance.Set(&r.a.Balance)
		if !r.a.Balance.IsZero() {
			r.accData.FieldSet |= AccountFieldBalanceOnly
		}
		r.accData.Nonce = r.a.Nonce
		if r.a.Nonce != 0 {
			r.accData.FieldSet |= AccountFieldNonceOnly
		}
		r.accData.Incarnation = r.a.Incarnation
		data = &r.accData
	}
	r.wasIHStorage = false
	r.currStorage.Reset()
	r.succStorage.Reset()
	var err error

	var wantProof func(_ []byte) *proofElement
	if r.proofRetainer != nil {
		wantProof = r.proofRetainer.ProofElement
	}
	if r.groups, r.hasTree, r.hasHash, err = GenStructStepEx(r.RetainNothing, r.curr.Bytes(), r.succ.Bytes(), r.hb, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if r.hc == nil {
			return nil
		}
		return r.hc(keyHex, hasState, hasTree, hasHash, hashes, rootHash)
	}, data, r.groups, r.hasTree, r.hasHash,
		//false,
		r.trace,
		wantProof,
		r.cutoff,
	); err != nil {
		return err
	}
	r.accData.FieldSet = 0
	return nil
}

func (r *RootHashAggregator) saveValueAccount(isIH, hasTree bool, v *accounts.Account, h []byte) error {
	r.wasIH = isIH
	if isIH {
		r.hashAccount.SetBytes(h)
		r.hadTreeAcc = hasTree
		return nil
	}
	r.a.Copy(v)
	// Place code on the stack first, the storage will follow
	if !r.a.IsEmptyCodeHash() {
		// the first item ends up deepest on the stack, the second item - on the top
		r.accData.FieldSet |= AccountFieldCodeOnly
		if err := r.hb.hash(r.a.CodeHash[:]); err != nil {
			return err
		}
	}
	return nil
}

// AccTrieCursor - holds logic related to iteration over AccTrie bucket
// has 2 basic operations:  _preOrderTraversalStep and _preOrderTraversalStepNoInDepth
type AccTrieCursor struct {
	SkipState       bool
	lvl             int
	k, v            [64][]byte // store up to 64 levels of key/value pairs in nibbles format
	hasState        [64]uint16 // says that records in dbutil.HashedAccounts exists by given prefix
	hasTree         [64]uint16 // says that records in dbutil.TrieOfAccounts exists by given prefix
	hasHash         [64]uint16 // store ownership of hashes stored in .v
	childID, hashID [64]int8   // meta info: current child in .hasState[lvl] field, max child id, current hash in .v[lvl]
	deleted         [64]bool   // helper to avoid multiple deletes of same key

	c               kv.Cursor
	hc              HashCollector2
	prev, cur, next []byte
	prefix          []byte // global prefix - cursor will never return records without this prefix

	firstNotCoveredPrefix []byte
	canUse                func([]byte) (bool, []byte) // if this function returns true - then this AccTrie can be used as is and don't need continue PostorderTraversal, but switch to sibling instead
	nextCreated           []byte

	kBuf []byte
	quit <-chan struct{}
}

func AccTrie(canUse func([]byte) (bool, []byte), hc HashCollector2, c kv.Cursor, quit <-chan struct{}) *AccTrieCursor {
	return &AccTrieCursor{
		c:                     c,
		canUse:                canUse,
		firstNotCoveredPrefix: make([]byte, 0, 64),
		next:                  make([]byte, 0, 64),
		kBuf:                  make([]byte, 0, 64),
		hc:                    hc,
		quit:                  quit,
	}
}

// _preOrderTraversalStep - goToChild || nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *AccTrieCursor) _preOrderTraversalStep() error {
	if c._hasTree() {
		c.next = append(append(c.next[:0], c.k[c.lvl]...), byte(c.childID[c.lvl]))
		ok, err := c._seek(c.next, c.next)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return c._preOrderTraversalStepNoInDepth()
}

// _preOrderTraversalStepNoInDepth - nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *AccTrieCursor) _preOrderTraversalStepNoInDepth() error {
	ok := c._nextSiblingInMem() || c._nextSiblingOfParentInMem()
	if ok {
		return nil
	}
	err := c._nextSiblingInDB()
	if err != nil {
		return err
	}
	return nil
}

func (c *AccTrieCursor) FirstNotCoveredPrefix() ([]byte, bool) {
	var ok bool
	c.firstNotCoveredPrefix, ok = firstNotCoveredPrefix(c.prev, c.prefix, c.firstNotCoveredPrefix)
	return c.firstNotCoveredPrefix, ok
}

func (c *AccTrieCursor) AtPrefix(prefix []byte) (k, v []byte, hasTree bool, err error) {
	c.SkipState = false // There can be accounts with keys less than the first key in AccTrie
	_, c.nextCreated = c.canUse([]byte{})
	c.prev = append(c.prev[:0], c.cur...)
	c.prefix = prefix
	ok, err := c._seek(prefix, []byte{})
	if err != nil {
		return []byte{}, nil, false, err
	}
	if !ok {
		c.cur = nil
		c.SkipState = false
		return nil, nil, false, nil
	}
	ok, err = c._consume()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if ok {
		return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
	}

	return c._next()
}

func (c *AccTrieCursor) Next() (k, v []byte, hasTree bool, err error) {
	c.SkipState = true
	c.prev = append(c.prev[:0], c.cur...)
	err = c._preOrderTraversalStepNoInDepth()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if c.k[c.lvl] == nil {
		c.cur = nil
		c.SkipState = c.SkipState && !dbutils2.NextNibblesSubtree(c.prev, &c.next)
		return nil, nil, false, nil
	}
	ok, err := c._consume()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if ok {
		return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
	}

	return c._next()
}

func (c *AccTrieCursor) _seek(seek []byte, withinPrefix []byte) (bool, error) {
	var k, v []byte
	var err error
	if len(seek) == 0 {
		k, v, err = c.c.First()
	} else {
		//TODO: write more common optimization - maintain .canUseNext variable by hasTree info - similar to skipState
		// optimistic .Next call, can use result in 2 cases:
		// - k is not child of current key
		// - looking for first child, means: c.childID[c.lvl] <= int16(bits.TrailingZeros16(c.hasTree[c.lvl]))
		// otherwise do .Seek call
		//k, v, err = c.c.Next()
		//if err != nil {
		//	return false, err
		//}
		//if bytes.HasPrefix(k, c.k[c.lvl]) {
		//	c.is++
		k, v, err = c.c.Seek(seek)
		//}
	}
	if err != nil {
		return false, err
	}
	if len(withinPrefix) > 0 { // seek within given prefix must not terminate overall process, even if k==nil
		if k == nil {
			return false, nil
		}
		if !bytes.HasPrefix(k, withinPrefix) {
			return false, nil
		}
	} else { // seek over global prefix does terminate overall process
		if k == nil {
			c.k[c.lvl] = nil
			return false, nil
		}
		if !bytes.HasPrefix(k, c.prefix) {
			c.k[c.lvl] = nil
			return false, nil
		}
	}
	c._unmarshal(k, v)
	c._nextSiblingInMem()
	return true, nil
}

func (c *AccTrieCursor) _nextSiblingInMem() bool {
	for c.childID[c.lvl] < int8(bits.Len16(c.hasState[c.lvl])) {
		c.childID[c.lvl]++
		if c._hasHash() {
			c.hashID[c.lvl]++
			return true
		}
		if c._hasTree() {
			return true
		}
		if c._hasState() {
			c.SkipState = false
		}
	}
	return false
}

func (c *AccTrieCursor) _nextSiblingOfParentInMem() bool {
	originalLvl := c.lvl
	for c.lvl > 1 {
		c.lvl--
		if c.k[c.lvl] == nil {
			continue
		}
		c.next = append(append(c.next[:0], c.k[originalLvl]...), uint8(c.childID[originalLvl]))
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		ok, err := c._seek(c.next, c.kBuf)
		if err != nil {
			panic(err)
		}
		if ok {
			return true
		}
		if c._nextSiblingInMem() {
			return true
		}
		originalLvl = c.lvl
	}
	return false
}

func (c *AccTrieCursor) _nextSiblingInDB() error {
	ok := dbutils2.NextNibblesSubtree(c.k[c.lvl], &c.next)
	if !ok {
		c.k[c.lvl] = nil
		return nil
	}
	if _, err := c._seek(c.next, []byte{}); err != nil {
		return err
	}
	if c.k[c.lvl] == nil || !bytes.HasPrefix(c.next, c.k[c.lvl]) {
		// If the cursor has moved beyond the next subtree, we need to check to make
		// sure that any modified keys in between are processed.
		c.SkipState = false
	}
	return nil
}

func (c *AccTrieCursor) _unmarshal(k, v []byte) {
	from, to := c.lvl+1, len(k)
	if c.lvl >= len(k) {
		from, to = len(k)+1, c.lvl+2
	}

	// Consider a trie DB with keys like: [0xa, 0xbb], then unmarshaling 0xbb
	// needs to nil the existing 0xa key entry, as it is no longer a parent.
	for i := from - 1; i > 0; i-- {
		if c.k[i] == nil {
			continue
		}
		if bytes.HasPrefix(k, c.k[i]) {
			break
		}
		from = i
	}
	for i := from; i < to; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
		c.k[i], c.hasState[i], c.hasTree[i], c.hasHash[i], c.hashID[i], c.childID[i], c.deleted[i] = nil, 0, 0, 0, 0, 0, false
	}
	c.lvl = len(k)
	c.k[c.lvl] = k
	c.deleted[c.lvl] = false
	c.hasState[c.lvl], c.hasTree[c.lvl], c.hasHash[c.lvl], c.v[c.lvl], _ = UnmarshalTrieNode(v)
	c.hashID[c.lvl] = -1
	c.childID[c.lvl] = int8(bits.TrailingZeros16(c.hasState[c.lvl]) - 1)
}

func (c *AccTrieCursor) _deleteCurrent() error {
	if c.hc == nil {
		return nil
	}
	if c.hc == nil || c.deleted[c.lvl] {
		return nil
	}
	if err := c.hc(c.k[c.lvl], 0, 0, 0, nil, nil); err != nil {
		return err
	}
	c.deleted[c.lvl] = true
	return nil
}

func (c *AccTrieCursor) _hasState() bool { return (1<<c.childID[c.lvl])&c.hasState[c.lvl] != 0 }
func (c *AccTrieCursor) _hasTree() bool  { return (1<<c.childID[c.lvl])&c.hasTree[c.lvl] != 0 }
func (c *AccTrieCursor) _hasHash() bool  { return (1<<c.childID[c.lvl])&c.hasHash[c.lvl] != 0 }
func (c *AccTrieCursor) _hash(i int8) []byte {
	return c.v[c.lvl][length2.Hash*int(i) : length2.Hash*(int(i)+1)]
}

func (c *AccTrieCursor) _consume() (bool, error) {
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if ok, nextCreated := c.canUse(c.kBuf); ok {
			c.SkipState = c.SkipState && keyIsBefore(c.kBuf, c.nextCreated)
			c.nextCreated = nextCreated
			c.cur = append(c.cur[:0], c.kBuf...)
			return true, nil
		}
	}

	if err := c._deleteCurrent(); err != nil {
		return false, err
	}

	return false, nil
}

func (c *AccTrieCursor) _next() (k, v []byte, hasTree bool, err error) {
	var ok bool
	if err = libcommon.Stopped(c.quit); err != nil {
		return []byte{}, nil, false, err
	}
	c.SkipState = c.SkipState && c._hasTree()
	err = c._preOrderTraversalStep()
	if err != nil {
		return []byte{}, nil, false, err
	}

	for {
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.SkipState = c.SkipState && !dbutils2.NextNibblesSubtree(c.prev, &c.next)
			return nil, nil, false, nil
		}

		ok, err = c._consume()
		if err != nil {
			return []byte{}, nil, false, err
		}
		if ok {
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
		}

		c.SkipState = c.SkipState && c._hasTree()
		err = c._preOrderTraversalStep()
		if err != nil {
			return []byte{}, nil, false, err
		}
	}
}

// StorageTrieCursor - holds logic related to iteration over AccTrie bucket
type StorageTrieCursor struct {
	lvl                        int
	k, v                       [64][]byte
	hasState, hasTree, hasHash [64]uint16
	deleted                    [64]bool
	childID, hashID            [64]int8

	c         kv.Cursor
	shc       StorageHashCollector2
	prev, cur []byte
	seek      []byte
	root      []byte

	next                  []byte
	firstNotCoveredPrefix []byte
	canUse                func([]byte) (bool, []byte)
	nextCreated           []byte
	skipState             bool

	accWithInc []byte
	kBuf       []byte
	quit       <-chan struct{}
}

func StorageTrie(canUse func(prefix []byte) (bool, []byte), shc StorageHashCollector2, c kv.Cursor, quit <-chan struct{}) *StorageTrieCursor {
	ih := &StorageTrieCursor{c: c, canUse: canUse,
		firstNotCoveredPrefix: make([]byte, 0, 64),
		next:                  make([]byte, 0, 64),
		kBuf:                  make([]byte, 0, 64),
		shc:                   shc,
		quit:                  quit,
	}
	return ih

}

func (c *StorageTrieCursor) PrevKey() []byte {
	return c.prev
}

func (c *StorageTrieCursor) FirstNotCoveredPrefix() ([]byte, bool) {
	var ok bool
	c.firstNotCoveredPrefix, ok = firstNotCoveredPrefix(c.prev, []byte{0, 0}, c.firstNotCoveredPrefix)
	return c.firstNotCoveredPrefix, ok
}

func (c *StorageTrieCursor) SeekToAccount(accWithInc []byte) (k, v []byte, hasTree bool, err error) {
	c.skipState = true
	c.accWithInc = accWithInc
	hexutil.DecompressNibbles(c.accWithInc, &c.kBuf)
	_, c.nextCreated = c.canUse(c.kBuf)
	c.seek = append(c.seek[:0], c.accWithInc...)
	c.prev = c.cur
	var ok bool
	ok, err = c._seek(accWithInc, []byte{})
	if err != nil {
		return []byte{}, nil, false, err
	}
	if !ok {
		c.cur = nil
		c.skipState = false
		return nil, nil, false, nil
	}
	if c.root != nil { // check if acc.storageRoot can be used
		root := c.root
		c.root = nil
		ok1, nextCreated := c.canUse(c.kBuf)
		if ok1 {
			c.skipState = true
			c.nextCreated = nextCreated
			c.cur = c.k[c.lvl]
			return c.cur, root, false, nil
		}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, false, err
		}
		err = c._preOrderTraversalStepNoInDepth()
		if err != nil {
			return []byte{}, nil, false, err
		}
	}

	ok, err = c._consume()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if ok {
		return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
	}

	return c._next()
}

func (c *StorageTrieCursor) Next() (k, v []byte, hasTree bool, err error) {
	c.skipState = true
	c.prev = c.cur
	err = c._preOrderTraversalStepNoInDepth()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if c.k[c.lvl] == nil {
		c.skipState = c.skipState && !dbutils2.NextNibblesSubtree(c.prev, &c.next)
		c.cur = nil
		return nil, nil, false, nil
	}

	ok, err := c._consume()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if ok {
		return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
	}
	return c._next()
}

func (c *StorageTrieCursor) _consume() (bool, error) {
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		ok, nextCreated := c.canUse(c.kBuf)
		if ok {
			c.skipState = c.skipState && keyIsBefore(c.kBuf, c.nextCreated)
			c.nextCreated = nextCreated
			c.cur = libcommon.Copy(c.kBuf[80:])
			return true, nil
		}
	}

	if err := c._deleteCurrent(); err != nil {
		return false, err
	}
	return false, nil
}

func (c *StorageTrieCursor) _seek(seek, withinPrefix []byte) (bool, error) {
	var k, v []byte
	var err error
	if len(seek) == 40 {
		k, v, err = c.c.Seek(seek)
	} else {
		// optimistic .Next call, can use result in 2 cases:
		// - no child found, means: len(k) <= c.lvl
		// - looking for first child, means: c.childID[c.lvl] <= int8(bits.TrailingZeros16(c.hasTree[c.lvl]))
		// otherwise do .Seek call
		//k, v, err = c.c.Next()
		//if err != nil {
		//	return false, err
		//}
		//if len(k) > c.lvl && c.childID[c.lvl] > int8(bits.TrailingZeros16(c.hasTree[c.lvl])) {
		k, v, err = c.c.Seek(seek)
		//}
	}
	if err != nil {
		return false, err
	}
	if len(withinPrefix) > 0 { // seek within given prefix must not terminate overall process
		if k == nil {
			return false, nil
		}
		if !bytes.HasPrefix(k, c.accWithInc) || !bytes.HasPrefix(k[40:], withinPrefix) {
			return false, nil
		}
	} else {
		if k == nil {
			c.k[c.lvl] = nil
			return false, nil
		}
		if !bytes.HasPrefix(k, c.accWithInc) {
			c.k[c.lvl] = nil
			return false, nil
		}
	}
	c._unmarshal(k, v)
	if c.lvl > 0 { // root record, firstly storing root hash
		c._nextSiblingInMem()
	}
	return true, nil
}

// _preOrderTraversalStep - goToChild || nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *StorageTrieCursor) _preOrderTraversalStep() error {
	if c._hasTree() {
		c.seek = append(append(c.seek[:40], c.k[c.lvl]...), byte(c.childID[c.lvl]))
		ok, err := c._seek(c.seek, []byte{})
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return c._preOrderTraversalStepNoInDepth()
}

// _preOrderTraversalStepNoInDepth - nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *StorageTrieCursor) _preOrderTraversalStepNoInDepth() error {
	ok := c._nextSiblingInMem() || c._nextSiblingOfParentInMem()
	if ok {
		return nil
	}
	err := c._nextSiblingInDB()
	if err != nil {
		return err
	}
	return nil
}

func (c *StorageTrieCursor) _hasState() bool { return (1<<c.childID[c.lvl])&c.hasState[c.lvl] != 0 }
func (c *StorageTrieCursor) _hasHash() bool  { return (1<<c.childID[c.lvl])&c.hasHash[c.lvl] != 0 }
func (c *StorageTrieCursor) _hasTree() bool  { return (1<<c.childID[c.lvl])&c.hasTree[c.lvl] != 0 }
func (c *StorageTrieCursor) _hash(i int8) []byte {
	return c.v[c.lvl][int(i)*length2.Hash : (int(i)+1)*length2.Hash]
}

func (c *StorageTrieCursor) _nextSiblingInMem() bool {
	for c.childID[c.lvl] < int8(bits.Len16(c.hasState[c.lvl])) {
		c.childID[c.lvl]++
		if c._hasHash() {
			c.hashID[c.lvl]++
			return true
		}
		if c._hasTree() {
			return true
		}
		if c._hasState() {
			c.skipState = false
		}
	}
	return false
}

func (c *StorageTrieCursor) _nextSiblingOfParentInMem() bool {
	originalLvl := c.lvl
	for c.lvl > 0 {
		c.lvl--
		if c.k[c.lvl] == nil {
			continue
		}

		c.seek = append(append(c.seek[:40], c.k[originalLvl]...), uint8(c.childID[originalLvl]))
		c.next = append(append(c.next[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		ok, err := c._seek(c.seek, c.next)
		if err != nil {
			panic(err)
		}
		if ok {
			return true
		}
		if c._nextSiblingInMem() {
			return true
		}
		originalLvl = c.lvl
	}
	return false
}

func (c *StorageTrieCursor) _nextSiblingInDB() error {
	ok := dbutils2.NextNibblesSubtree(c.k[c.lvl], &c.next)
	if !ok {
		c.k[c.lvl] = nil
		return nil
	}
	c.seek = append(c.seek[:40], c.next...)
	if _, err := c._seek(c.seek, []byte{}); err != nil {
		return err
	}
	if c.k[c.lvl] == nil || !bytes.HasPrefix(c.next, c.k[c.lvl]) {
		// If the cursor has moved beyond the next subtree, we need to check to make
		// sure that any modified keys in between are processed.
		c.skipState = false
	}
	return nil
}

func (c *StorageTrieCursor) _next() (k, v []byte, hasTree bool, err error) {
	var ok bool
	if err = libcommon.Stopped(c.quit); err != nil {
		return []byte{}, nil, false, err
	}
	c.skipState = c.skipState && c._hasTree()
	if err = c._preOrderTraversalStep(); err != nil {
		return []byte{}, nil, false, err
	}

	for {
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.skipState = c.skipState && !dbutils2.NextNibblesSubtree(c.prev, &c.next)
			return nil, nil, false, nil
		}

		ok, err = c._consume()
		if err != nil {
			return []byte{}, nil, false, err
		}
		if ok {
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
		}

		c.skipState = c.skipState && c._hasTree()
		err = c._preOrderTraversalStep()
		if err != nil {
			return []byte{}, nil, false, err
		}
	}
}

func (c *StorageTrieCursor) _unmarshal(k, v []byte) {
	from, to := c.lvl+1, len(k)
	if c.lvl >= len(k) {
		from, to = len(k)+1, c.lvl+2
	}
	// Consider a trie DB with keys like: [0xa, 0xbb], then unmarshaling 0xbb
	// needs to nil the existing 0xa key entry, as it is no longer a parent.
	for i := from - 1; i > 0; i-- {
		if c.k[i] == nil {
			continue
		}
		if bytes.HasPrefix(k[40:], c.k[i]) {
			break
		}
		from = i
	}
	for i := from; i < to; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
		c.k[i], c.hasState[i], c.hasTree[i], c.hasHash[i], c.hashID[i], c.childID[i], c.deleted[i] = nil, 0, 0, 0, 0, 0, false
	}

	c.lvl = len(k) - 40
	c.k[c.lvl] = k[40:]
	c.deleted[c.lvl] = false
	c.hasState[c.lvl], c.hasTree[c.lvl], c.hasHash[c.lvl], c.v[c.lvl], c.root = UnmarshalTrieNode(v)
	c.hashID[c.lvl] = -1
	c.childID[c.lvl] = int8(bits.TrailingZeros16(c.hasState[c.lvl]) - 1)
}

func (c *StorageTrieCursor) _deleteCurrent() error {
	if c.shc == nil {
		return nil
	}
	if c.shc == nil || c.deleted[c.lvl] {
		return nil
	}
	if err := c.shc(c.accWithInc, c.k[c.lvl], 0, 0, 0, nil, nil); err != nil {
		return err
	}
	c.deleted[c.lvl] = true
	return nil
}

/*
Dense Sequence - if between 2 AccTrie records not possible insert any state record - then they form "dense sequence"
If 2 AccTrie records form Dense Sequence - then no reason to iterate over state - just use AccTrie one after another
Example1:

	1234
	1235

Example2:

	12ff
	13

Example3:

	12ff
	13000000

If 2 AccTrie records form "sequence" then it can be consumed without moving StateCursor
*/
func isDenseSequence(prev []byte, next []byte) bool {
	isSequence := false
	if len(prev) == 0 && len(next) == 0 {
		return false
	}
	ok := dbutils2.NextNibblesSubtree(prev, &isSequenceBuf)
	if len(prev) > 0 && !ok {
		return true
	}
	if bytes.HasPrefix(next, isSequenceBuf) {
		tail := next[len(isSequenceBuf):] // if tail has only zeroes, then no state records can be between fstl.nextHex and fstl.ihK
		isSequence = true
		for _, n := range tail {
			if n != 0 {
				isSequence = false
				break
			}
		}
	}

	return isSequence
}

var isSequenceBuf = make([]byte, 256)

func firstNotCoveredPrefix(prev, prefix, buf []byte) ([]byte, bool) {
	if len(prev) > 0 {
		if !dbutils2.NextNibblesSubtree(prev, &buf) {
			return buf, true
		}
	} else {
		buf = append(buf[:0], prefix...)
	}
	if len(buf)%2 == 1 {
		buf = append(buf, 0)
	}
	hexutil.CompressNibbles(buf, &buf)
	return buf, false
}

type StateCursor struct {
	c    kv.Cursor
	quit <-chan struct{}
	kHex []byte
}

func NewStateCursor(c kv.Cursor, quit <-chan struct{}) *StateCursor {
	return &StateCursor{c: c, quit: quit}
}

func (c *StateCursor) Seek(seek []byte) ([]byte, []byte, []byte, error) {
	k, v, err := c.c.Seek(seek)
	if err != nil {
		return []byte{}, nil, nil, err
	}

	hexutil.DecompressNibbles(k, &c.kHex)
	return k, c.kHex, v, nil
}

func (c *StateCursor) Next() ([]byte, []byte, []byte, error) {
	if err := libcommon.Stopped(c.quit); err != nil {
		return []byte{}, nil, nil, err
	}
	k, v, err := c.c.Next()
	if err != nil {
		return []byte{}, nil, nil, err
	}

	hexutil.DecompressNibbles(k, &c.kHex)
	return k, c.kHex, v, nil
}

// keyIsBefore - ksind of bytes.Compare, but nil is the last key. And return
func keyIsBefore(k1, k2 []byte) bool {
	if k1 == nil {
		return false
	}
	if k2 == nil {
		return true
	}
	return bytes.Compare(k1, k2) < 0
}

func UnmarshalTrieNodeTyped(v []byte) (hasState, hasTree, hasHash uint16, hashes []libcommon.Hash, rootHash libcommon.Hash) {
	hasState, hasTree, hasHash, v = binary.BigEndian.Uint16(v), binary.BigEndian.Uint16(v[2:]), binary.BigEndian.Uint16(v[4:]), v[6:]
	if bits.OnesCount16(hasHash)+1 == len(v)/length2.Hash {
		rootHash.SetBytes(libcommon.CopyBytes(v[:32]))
		v = v[32:]
	}
	hashes = make([]libcommon.Hash, len(v)/length2.Hash)
	for i := 0; i < len(hashes); i++ {
		hashes[i].SetBytes(libcommon.CopyBytes(v[i*length2.Hash : (i+1)*length2.Hash]))
	}
	return
}

func UnmarshalTrieNode(v []byte) (hasState, hasTree, hasHash uint16, hashes, rootHash []byte) {
	hasState, hasTree, hasHash, hashes = binary.BigEndian.Uint16(v), binary.BigEndian.Uint16(v[2:]), binary.BigEndian.Uint16(v[4:]), v[6:]
	if bits.OnesCount16(hasHash)+1 == len(hashes)/length2.Hash {
		rootHash = hashes[:32]
		hashes = hashes[32:]
	}
	return
}

func MarshalTrieNodeTyped(hasState, hasTree, hasHash uint16, h []libcommon.Hash, buf []byte) []byte {
	buf = buf[:6+len(h)*length2.Hash]
	meta, hashes := buf[:6], buf[6:]
	binary.BigEndian.PutUint16(meta, hasState)
	binary.BigEndian.PutUint16(meta[2:], hasTree)
	binary.BigEndian.PutUint16(meta[4:], hasHash)
	for i := 0; i < len(h); i++ {
		copy(hashes[i*length2.Hash:(i+1)*length2.Hash], h[i].Bytes())
	}
	return buf
}

func StorageKey(addressHash []byte, incarnation uint64, prefix []byte) []byte {
	return dbutils2.GenerateCompositeStoragePrefix(addressHash, incarnation, prefix)
}

func MarshalTrieNode(hasState, hasTree, hasHash uint16, hashes, rootHash []byte, buf []byte) []byte {
	buf = buf[:len(hashes)+len(rootHash)+6]
	meta, hashesList := buf[:6], buf[6:]
	binary.BigEndian.PutUint16(meta, hasState)
	binary.BigEndian.PutUint16(meta[2:], hasTree)
	binary.BigEndian.PutUint16(meta[4:], hasHash)
	if len(rootHash) == 0 {
		copy(hashesList, hashes)
	} else {
		copy(hashesList, rootHash)
		copy(hashesList[32:], hashes)
	}
	return buf
}

func CastTrieNodeValue(hashes, rootHash []byte) []libcommon.Hash {
	to := make([]libcommon.Hash, len(hashes)/length2.Hash+len(rootHash)/length2.Hash)
	i := 0
	if len(rootHash) > 0 {
		to[0].SetBytes(libcommon.CopyBytes(rootHash))
		i++
	}
	for j := 0; j < len(hashes)/length2.Hash; j++ {
		to[i].SetBytes(libcommon.CopyBytes(hashes[j*length2.Hash : (j+1)*length2.Hash]))
		i++
	}
	return to
}

// CalcRoot is a combination of `ResolveStateTrie` and `UpdateStateTrie`
// DESCRIBED: docs/programmers_guide/guide.md#organising-ethereum-state-into-a-merkle-tree
func CalcRoot(logPrefix string, tx kv.Tx) (libcommon.Hash, error) {
	loader := NewFlatDBTrieLoader(logPrefix, NewRetainList(0), nil, nil, false)

	h, err := loader.CalcTrieRoot(tx, nil)
	if err != nil {
		return EmptyRoot, err
	}

	return h, nil
}

func makeCurrentKeyStr(k []byte) string {
	var currentKeyStr string
	if k == nil {
		currentKeyStr = "final"
	} else if len(k) < 4 {
		currentKeyStr = hex.EncodeToString(k)
	} else {
		currentKeyStr = hex.EncodeToString(k[:4])
	}
	return currentKeyStr
}

func isSequenceOld(prev []byte, next []byte) bool {
	isSequence := false
	if bytes.HasPrefix(next, prev) {
		tail := next[len(prev):] // if tail has only zeroes, then no state records can be between fstl.nextHex and fstl.ihK
		isSequence = true
		for _, n := range tail {
			if n != 0 {
				isSequence = false
				break
			}
		}
	}

	return isSequence
}
