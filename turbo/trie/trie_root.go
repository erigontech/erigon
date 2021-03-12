package trie

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
)

/*
**Theoretically:** "Merkle trie root calculation" starts from state, build from state keys - trie,
on each level of trie calculates intermediate hash of underlying data.

**Practically:** It can be implemented as "Preorder trie traversal" (Preorder - visit Root, visit Left, visit Right).
But, let's make couple observations to make traversal over huge state efficient.

**Observation 1:** `TrieOfAccountsBucket` already stores state keys in sorted way.
Iteration over this bucket will retrieve keys in same order as "Preorder trie traversal".

**Observation 2:** each Eth block - changes not big part of state - it means most of Merkle trie intermediate hashes will not change.
It means we effectively can cache them. `TrieOfAccountsBucket` stores "Intermediate hashes of all Merkle trie levels".
It also sorted and Iteration over `TrieOfAccountsBucket` will retrieve keys in same order as "Preorder trie traversal".

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
// It produces stream of values and send this stream to `defaultReceiver`
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
	// Storage item buffer
	storageKey   []byte
	storageValue []byte

	// Account item buffer
	accountKey   []byte
	accountValue accounts.Account
	hashValue    []byte

	receiver        StreamReceiver
	defaultReceiver *RootHashAggregator
	hc              HashCollector2
	shc             StorageHashCollector2
}

// RootHashAggregator - calculates Merkle trie root hash from incoming data stream
type RootHashAggregator struct {
	trace          bool
	wasIH          bool
	wasIHStorage   bool
	root           common.Hash
	hc             HashCollector2
	shc            StorageHashCollector2
	currStorage    bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage    bytes.Buffer
	valueStorage   []byte // Current value to be used as the value tape for the hash builder
	hadTreeStorage bool
	hashAccount    common.Hash  // Current value to be used as the value tape for the hash builder
	hashStorage    common.Hash  // Current value to be used as the value tape for the hash builder
	curr           bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
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
}

func NewRootHashAggregator() *RootHashAggregator {
	return &RootHashAggregator{
		hb: NewHashBuilder(false),
	}
}

func NewFlatDBTrieLoader(logPrefix string) *FlatDBTrieLoader {
	return &FlatDBTrieLoader{
		logPrefix:       logPrefix,
		defaultReceiver: NewRootHashAggregator(),
	}
}

// Reset prepares the loader for reuse
func (l *FlatDBTrieLoader) Reset(rd RetainDeciderWithMarker, hc HashCollector2, shc StorageHashCollector2, trace bool) error {
	l.defaultReceiver.Reset(hc, shc, trace)
	l.hc = hc
	l.shc = shc
	l.receiver = l.defaultReceiver
	l.trace = trace
	l.ihSeek, l.accSeek, l.storageSeek, l.kHex, l.kHexS = make([]byte, 0, 128), make([]byte, 0, 128), make([]byte, 0, 128), make([]byte, 0, 128), make([]byte, 0, 128)
	l.rd = rd
	if l.trace {
		fmt.Printf("----------\n")
		fmt.Printf("CalcTrieRoot\n")
	}
	return nil
}

func (l *FlatDBTrieLoader) SetStreamReceiver(receiver StreamReceiver) {
	l.receiver = receiver
}

// CalcTrieRoot algo:
//	for iterateIHOfAccounts {
//		if canSkipState
//          goto SkipAccounts
//
//		for iterateAccounts from prevIH to currentIH {
//			use(account)
//			for iterateIHOfStorage within accountWithIncarnation{
//				if canSkipState
//					goto SkipStorage
//
//				for iterateStorage from prevIHOfStorage to currentIHOfStorage {
//					use(storage)
//				}
//            SkipStorage:
//				use(ihStorage)
//			}
//		}
//    SkipAccounts:
//		use(AccTrie)
//	}
func (l *FlatDBTrieLoader) CalcTrieRoot(db ethdb.Database, prefix []byte, quit <-chan struct{}) (common.Hash, error) {
	var (
		tx ethdb.Tx
	)

	var txDB ethdb.DbWithPendingMutations
	var useExternalTx bool

	// If method executed within transaction - use it, or open new read transaction
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		txDB = hasTx.(ethdb.DbWithPendingMutations)
		tx = hasTx.Tx()
		useExternalTx = true
	} else {
		var err error
		txDB, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return EmptyRoot, err
		}

		defer txDB.Rollback()
		tx = txDB.(ethdb.HasTx).Tx()
	}

	accC := tx.Cursor(dbutils.HashedAccountsBucket)
	defer accC.Close()
	accs := NewStateCursor(accC, quit)
	trieAccC, trieStorageC := tx.Cursor(dbutils.TrieOfAccountsBucket), tx.CursorDupSort(dbutils.TrieOfStorageBucket)
	defer trieAccC.Close()
	defer trieStorageC.Close()

	var canUse = func(prefix []byte) (bool, []byte) {
		retain, nextCreated := l.rd.RetainWithMarker(prefix)
		return !retain, nextCreated
	}
	accTrie := AccTrie(canUse, l.hc, trieAccC, quit)
	storageTrie := StorageTrie(canUse, l.shc, trieStorageC, quit)

	ss := tx.CursorDupSort(dbutils.HashedStorageBucket)
	defer ss.Close()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	for ihK, ihV, hasTree, err := accTrie.AtPrefix(prefix); ; ihK, ihV, hasTree, err = accTrie.Next() { // no loop termination is at he end of loop
		if err != nil {
			return EmptyRoot, err
		}
		if accTrie.SkipState {
			goto SkipAccounts
		}

		for k, kHex, v, err1 := accs.Seek(accTrie.FirstNotCoveredPrefix()); k != nil; k, kHex, v, err1 = accs.Next() {
			if err1 != nil {
				return EmptyRoot, err1
			}
			if keyIsBefore(ihK, kHex) || !bytes.HasPrefix(kHex, prefix) { // read all accounts until next AccTrie
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
				for kS, vS, err3 := ss.SeekBothRange(accWithInc, storageTrie.FirstNotCoveredPrefix()); kS != nil; kS, vS, err3 = ss.NextDup() {
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

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, false, len(prefix)); err != nil {
		return EmptyRoot, err
	}

	if !useExternalTx {
		_, err := txDB.Commit()
		if err != nil {
			return EmptyRoot, err
		}
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

func (r *RootHashAggregator) Reset(hc HashCollector2, shc StorageHashCollector2, trace bool) {
	r.hc = hc
	r.shc = shc
	r.curr.Reset()
	r.succ.Reset()
	r.value = nil
	r.groups = r.groups[:0]
	r.hasTree = r.hasTree[:0]
	r.hasHash = r.hasHash[:0]
	r.a.Reset()
	r.hb.Reset()
	r.wasIH = false
	r.currStorage.Reset()
	r.succStorage.Reset()
	r.valueStorage = nil
	r.wasIHStorage = false
	r.root = common.Hash{}
	r.trace = trace
	r.hb.trace = trace
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
			r.hb.hashStack = append(append(r.hb.hashStack, byte(80+common.HashLength)), hash...)
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
func (r *RootHashAggregator) traceIf(acc, st string) {
	// "succ" - because on this iteration this "succ" will become "curr"
	if r.succStorage.Len() == 0 {
		var accNibbles []byte
		hexutil.DecompressNibbles(common.FromHex(acc), &accNibbles)
		r.trace = bytes.HasPrefix(r.succ.Bytes(), accNibbles)
	} else {
		r.trace = bytes.HasPrefix(r.currAccK, common.FromHex(acc)) && bytes.HasPrefix(r.succStorage.Bytes(), common.FromHex(st))
	}
}

func (r *RootHashAggregator) Result() SubTries {
	panic("don't call me")
}

func (r *RootHashAggregator) Root() common.Hash {
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
	r.groupsStorage, r.hasTreeStorage, r.hasHashStorage, err = GenStructStep(r.RetainNothing, r.currStorage.Bytes(), r.succStorage.Bytes(), r.hb, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if r.shc == nil {
			return nil
		}
		return r.shc(r.currAccK, keyHex, hasState, hasTree, hasHash, hashes, rootHash)
	}, data, r.groupsStorage, r.hasTreeStorage, r.hasHashStorage,
		r.trace,
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
		if r.a.Balance.Sign() != 0 {
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
	if r.groups, r.hasTree, r.hasHash, err = GenStructStep(r.RetainNothing, r.curr.Bytes(), r.succ.Bytes(), r.hb, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if r.hc == nil {
			return nil
		}

		//if bytes.HasPrefix(keyHex, common.FromHex("060e")) {
		//	fmt.Printf("collect: %x,%b,%b, del:%t\n", keyHex, hasHash, hasTree, hashes == nil)
		//}
		return r.hc(keyHex, hasState, hasTree, hasHash, hashes, rootHash)
	}, data, r.groups, r.hasTree, r.hasHash,
		false,
		//r.trace,
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
	is, lvl         int
	k, v            [64][]byte // store up to 64 levels of key/value pairs in nibbles format
	hasState        [64]uint16 // says that records in dbutil.HashedAccountsBucket exists by given prefix
	hasTree         [64]uint16 // says that records in dbutil.TrieOfAccountsBucket exists by given prefix
	hasHash         [64]uint16 // store ownership of hashes stored in .v
	childID, hashID [64]int8   // meta info: current child in .hasState[lvl] field, max child id, current hash in .v[lvl]
	deleted         [64]bool   // helper to avoid multiple deletes of same key

	c               ethdb.Cursor
	hc              HashCollector2
	prev, cur, next []byte
	prefix          []byte // global prefix - cursor will never return records without this prefix

	firstNotCoveredPrefix []byte
	canUse                func([]byte) (bool, []byte) // if this function returns true - then this AccTrie can be used as is and don't need continue PostorderTraversal, but switch to sibling instead
	nextCreated           []byte

	kBuf []byte
	quit <-chan struct{}
}

func AccTrie(canUse func([]byte) (bool, []byte), hc HashCollector2, c ethdb.Cursor, quit <-chan struct{}) *AccTrieCursor {
	return &AccTrieCursor{
		c:                     c,
		canUse:                canUse,
		firstNotCoveredPrefix: make([]byte, 0, 64),
		next:                  make([]byte, 64),
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

func (c *AccTrieCursor) FirstNotCoveredPrefix() []byte {
	c.firstNotCoveredPrefix = firstNotCoveredPrefix(c.prev, c.prefix, c.firstNotCoveredPrefix)
	return c.firstNotCoveredPrefix
}

func (c *AccTrieCursor) AtPrefix(prefix []byte) (k, v []byte, hasTree bool, err error) {
	c.SkipState = true
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
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if ok, nextCreated := c.canUse(c.kBuf); ok {
			c.SkipState = c.SkipState && keyIsBefore(c.kBuf, c.nextCreated)
			c.nextCreated = nextCreated
			c.cur = append(c.cur[:0], c.kBuf...)
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
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
		c.SkipState = c.SkipState && !dbutils.NextNibblesSubtree(c.prev, &c.kBuf)
		return nil, nil, false, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if ok, nextCreated := c.canUse(c.kBuf); ok {
			c.SkipState = c.SkipState && keyIsBefore(c.kBuf, c.nextCreated)
			c.nextCreated = nextCreated
			c.cur = append(c.cur[:0], c.kBuf...)
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
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
		// - no child found, means: len(k) <= c.lvl
		// - looking for first child, means: c.childID[c.lvl] <= int16(bits.TrailingZeros16(c.hasTree[c.lvl]))
		// otherwise do .Seek call
		//k, v, err = c.c.Next()
		//if err != nil {
		//	return false, err
		//}
		//if len(k) > c.lvl && c.childID[c.lvl] > int8(bits.TrailingZeros16(c.hasTree[c.lvl])) {
		//	c.is++
		k, v, err = c.c.Seek(seek)
		//}
	}
	if err != nil {
		return false, err
	}
	if k == nil || !bytes.HasPrefix(k, c.prefix) {
		c.k[c.lvl] = nil
		return false, nil
	}
	if !bytes.HasPrefix(k, withinPrefix) {
		return false, nil
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
	for c.lvl > 1 {
		if c.k[c.lvl-1] == nil {
			nonNilLvl := c.lvl - 1
			for c.k[nonNilLvl] == nil && nonNilLvl > 1 {
				nonNilLvl--
			}
			c.next = append(append(c.next[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			c.kBuf = append(append(c.kBuf[:0], c.k[nonNilLvl]...), uint8(c.childID[nonNilLvl]))
			ok, err := c._seek(c.next, c.kBuf)
			if err != nil {
				panic(err)
			}
			if ok {
				return true
			}

			c.lvl = nonNilLvl + 1
			continue
		}
		c.lvl--
		if c._nextSiblingInMem() {
			return true
		}
	}
	return false
}

func (c *AccTrieCursor) _nextSiblingInDB() error {
	ok := dbutils.NextNibblesSubtree(c.k[c.lvl], &c.next)
	if !ok {
		c.k[c.lvl] = nil
		return nil
	}
	c.is++
	if _, err := c._seek(c.next, []byte{}); err != nil {
		return err
	}
	//c.SkipState = c.SkipState && bytes.Equal(c.next, c.k[c.lvl])
	return nil
}

func (c *AccTrieCursor) _unmarshal(k, v []byte) {
	from, to := c.lvl+1, len(k)
	if c.lvl >= len(k) {
		from, to = len(k)+1, c.lvl+2
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
	if c.deleted[c.lvl] {
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
	return c.v[c.lvl][common.HashLength*int(i) : common.HashLength*(int(i)+1)]
}

func (c *AccTrieCursor) _next() (k, v []byte, hasTree bool, err error) {
	if err = common.Stopped(c.quit); err != nil {
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
			c.SkipState = c.SkipState && !dbutils.NextNibblesSubtree(c.prev, &c.kBuf)
			return nil, nil, false, nil
		}

		if c._hasHash() {
			c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			if ok, nextCreated := c.canUse(c.kBuf); ok {
				c.SkipState = c.SkipState && keyIsBefore(c.kBuf, c.nextCreated)
				c.nextCreated = nextCreated
				c.cur = append(c.cur[:0], c.kBuf...)
				return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
			}
		}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, false, err
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
	is, lvl                    int
	k, v                       [64][]byte
	hasState, hasTree, hasHash [64]uint16
	deleted                    [64]bool
	childID, hashID            [64]int8

	c         ethdb.Cursor
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
	kBuf2      []byte
	quit       <-chan struct{}
}

func StorageTrie(canUse func(prefix []byte) (bool, []byte), shc StorageHashCollector2, c ethdb.Cursor, quit <-chan struct{}) *StorageTrieCursor {
	ih := &StorageTrieCursor{c: c, canUse: canUse,
		firstNotCoveredPrefix: make([]byte, 0, 64), next: make([]byte, 64),
		shc:  shc,
		quit: quit,
	}
	return ih
}

func (c *StorageTrieCursor) PrevKey() []byte {
	return c.prev
}

func (c *StorageTrieCursor) FirstNotCoveredPrefix() []byte {
	c.firstNotCoveredPrefix = firstNotCoveredPrefix(c.prev, []byte{0, 0}, c.firstNotCoveredPrefix)
	return c.firstNotCoveredPrefix
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

		if ok1, nextCreated := c.canUse(c.kBuf); ok1 {
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

	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if ok, nextCreated := c.canUse(c.kBuf); ok {
			c.skipState = c.skipState && keyIsBefore(c.kBuf, c.nextCreated)
			c.nextCreated = nextCreated
			c.cur = common.CopyBytes(c.kBuf[80:])
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
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
		c.skipState = c.skipState && !dbutils.NextNibblesSubtree(c.prev, &c.kBuf)
		c.cur = nil
		return nil, nil, false, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if ok, nextCreated := c.canUse(c.kBuf); ok {
			c.skipState = c.skipState && keyIsBefore(c.kBuf, c.nextCreated)
			c.nextCreated = nextCreated
			c.cur = common.CopyBytes(c.kBuf[80:])
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
	}

	return c._next()
}

func (c *StorageTrieCursor) _seek(seek, prefix []byte) (bool, error) {
	var k, v []byte
	var err error
	if len(seek) == 40 {
		c.is++
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
		//c.is++
		k, v, err = c.c.Seek(seek)
		//}
	}
	if err != nil {
		return false, err
	}
	if k == nil || !bytes.HasPrefix(k, c.accWithInc) || !bytes.HasPrefix(k[40:], prefix) {
		c.k[c.lvl] = nil
		return false, nil
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
	return c.v[c.lvl][int(i)*common.HashLength : (int(i)+1)*common.HashLength]
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
	for c.lvl > 0 {
		if c.k[c.lvl-1] == nil {
			nonNilLvl := c.lvl - 1
			for ; c.k[nonNilLvl] == nil && nonNilLvl > 0; nonNilLvl-- {
			}
			c.seek = append(append(c.seek[:40], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			c.kBuf2 = append(append(c.kBuf2[:0], c.k[nonNilLvl]...), uint8(c.childID[nonNilLvl]))
			ok, err := c._seek(c.seek, c.kBuf2)
			if err != nil {
				panic(err)
			}
			if ok {
				return true
			}

			c.lvl = nonNilLvl + 1
			continue
		}
		c.lvl--
		if c._nextSiblingInMem() {
			return true
		}
	}
	return false
}

func (c *StorageTrieCursor) _nextSiblingInDB() error {
	ok := dbutils.NextNibblesSubtree(c.k[c.lvl], &c.next)
	if !ok {
		c.k[c.lvl] = nil
		return nil
	}
	c.seek = append(c.seek[:40], c.next...)
	if _, err := c._seek(c.seek, []byte{}); err != nil {
		return err
	}
	//c.skipState = c.skipState && bytes.Equal(c.next, c.k[c.lvl])
	return nil
}

func (c *StorageTrieCursor) _next() (k, v []byte, hasTree bool, err error) {
	if err = common.Stopped(c.quit); err != nil {
		return []byte{}, nil, false, err
	}

	c.skipState = c.skipState && c._hasTree()
	err = c._preOrderTraversalStep()
	if err != nil {
		return []byte{}, nil, false, err
	}

	for {
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.skipState = c.skipState && !dbutils.NextNibblesSubtree(c.prev, &c.kBuf)
			return nil, nil, false, nil
		}

		if c._hasHash() {
			c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			if ok, nextCreated := c.canUse(c.kBuf); ok {
				c.skipState = c.skipState && keyIsBefore(c.kBuf, c.nextCreated)
				c.nextCreated = nextCreated
				c.cur = common.CopyBytes(c.kBuf[80:])
				return c.cur, c._hash(c.hashID[c.lvl]), c._hasTree(), nil
			}
		}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, false, err
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
	if c.deleted[c.lvl] {
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
	ok := dbutils.NextNibblesSubtree(prev, &isSequenceBuf)
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

func firstNotCoveredPrefix(prev, prefix, buf []byte) []byte {
	if len(prev) > 0 {
		_ = dbutils.NextNibblesSubtree(prev, &buf)
	} else {
		buf = append(buf[:0], prefix...)
	}
	if len(buf)%2 == 1 {
		buf = append(buf, 0)
	}
	hexutil.CompressNibbles(buf, &buf)
	return buf
}

type StateCursor struct {
	c    ethdb.Cursor
	quit <-chan struct{}
	kHex []byte
}

func NewStateCursor(c ethdb.Cursor, quit <-chan struct{}) *StateCursor {
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
	if err := common.Stopped(c.quit); err != nil {
		return []byte{}, nil, nil, err
	}
	k, v, err := c.c.Next()
	if err != nil {
		return []byte{}, nil, nil, err
	}

	hexutil.DecompressNibbles(k, &c.kHex)
	return k, c.kHex, v, nil
}

func nextAccount(in, out []byte) bool {
	copy(out, in)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 255 {
			out[i]++
			return true
		}
		out[i] = 0
	}
	return false
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

func UnmarshalTrieNodeTyped(v []byte) (uint16, uint16, uint16, []common.Hash) {
	hasState, hasTree, hasHash := binary.BigEndian.Uint16(v), binary.BigEndian.Uint16(v[2:]), binary.BigEndian.Uint16(v[4:])
	v = v[6:]
	newV := make([]common.Hash, len(v)/common.HashLength)
	for i := 0; i < len(newV); i++ {
		newV[i].SetBytes(common.CopyBytes(v[i*common.HashLength : (i+1)*common.HashLength]))
	}
	return hasState, hasTree, hasHash, newV
}

func UnmarshalTrieNode(v []byte) (hasState, hasTree, hasHash uint16, hashes, rootHash []byte) {
	hasState, hasTree, hasHash, hashes = binary.BigEndian.Uint16(v), binary.BigEndian.Uint16(v[2:]), binary.BigEndian.Uint16(v[4:]), v[6:]
	if bits.OnesCount16(hasHash)+1 == len(hashes)/common.HashLength {
		rootHash = hashes[:32]
		hashes = hashes[32:]
	}
	return
}

func MarshalTrieNodeTyped(hasState, hasTree, hasHash uint16, h []common.Hash, buf []byte) []byte {
	buf = buf[:6+len(h)*common.HashLength]
	meta, hashes := buf[:6], buf[6:]
	binary.BigEndian.PutUint16(meta, hasState)
	binary.BigEndian.PutUint16(meta[2:], hasTree)
	binary.BigEndian.PutUint16(meta[4:], hasHash)
	for i := 0; i < len(h); i++ {
		copy(hashes[i*common.HashLength:(i+1)*common.HashLength], h[i].Bytes())
	}
	return buf
}

func StorageKey(addressHash []byte, incarnation uint64, prefix []byte) []byte {
	return dbutils.GenerateCompositeStoragePrefix(addressHash, incarnation, prefix)
}

func MarshalTrieNode(hasState, hasTree, hasHash uint16, hashes []byte, rootHash []byte, buf []byte) []byte {
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

func CastTrieNodeValue(hashes []byte, rootHash []byte) []common.Hash {
	to := make([]common.Hash, len(hashes)/common.HashLength+len(rootHash)/common.HashLength)
	i := 0
	if len(rootHash) > 0 {
		to[0].SetBytes(common.CopyBytes(rootHash))
		i++
	}
	for j := 0; j < len(hashes)/common.HashLength; j++ {
		to[i].SetBytes(common.CopyBytes(hashes[j*common.HashLength : (j+1)*common.HashLength]))
		i++
	}
	return to
}

func collectMissedAccTrie(canUse func([]byte) (bool, []byte), prefix []byte, cache *shards.StateCache, quit <-chan struct{}) ([][]byte, error) {
	var misses [][]byte
	if err := cache.AccountTree(prefix, func(k []byte, v common.Hash, hasTree, hasHash bool) (toChild bool, err error) {
		if k == nil || !hasTree || !hasHash {
			return hasTree, nil
		}
		if ok, _ := canUse(k); ok {
			return false, nil
		}

		return hasTree, common.Stopped(quit)
	}, func(k []byte) {
		misses = append(misses, common.CopyBytes(k))
	}); err != nil {
		return nil, err
	}
	return misses, nil
}

func loadAccTrieToCache(ih ethdb.Cursor, prefix []byte, misses [][]byte, cache *shards.StateCache, quit <-chan struct{}) error {
	for _, miss := range misses {
		if err := common.Stopped(quit); err != nil {
			return err
		}
		for k, v, err := ih.Seek(miss); k != nil; k, v, err = ih.Next() {
			if err != nil {
				return err
			}
			if !bytes.HasPrefix(k, miss) || !bytes.HasPrefix(k, prefix) { // read all accounts until next AccTrie
				break
			}
			hasState, hasTree, hasHash, newV := UnmarshalTrieNodeTyped(v)
			cache.SetAccountHashesRead(k, hasState, hasTree, hasHash, newV)
		}
	}
	return nil
}

func loadAccsToCache(accs ethdb.Cursor, accMisses [][]byte, canUse func([]byte) (bool, []byte), cache *shards.StateCache, quit <-chan struct{}) ([][]byte, error) {
	var misses [][]byte
	for i := range accMisses {
		miss := accMisses[i]
		if err := common.Stopped(quit); err != nil {
			return nil, err
		}
		fixedBits := len(miss) * 4
		if len(miss)%2 == 1 {
			miss = append(miss, 0)
		}
		hexutil.CompressNibbles(miss, &miss)
		if err := ethdb.Walk(accs, miss, fixedBits, func(k, v []byte) (bool, error) {
			var incarnation uint64
			accountHash := common.BytesToHash(k)
			b, ok := cache.GetAccountByHashedAddress(accountHash)
			if !ok {
				var a accounts.Account
				if err := a.DecodeForStorage(v); err != nil {
					return false, err
				}
				incarnation = a.Incarnation
				cache.DeprecatedSetAccountRead(accountHash, &a)
			} else {
				incarnation = b.Incarnation
			}
			if incarnation == 0 {
				return true, nil
			}

			if err := cache.StorageTree([]byte{}, accountHash, incarnation, func(k []byte, v common.Hash, hasTree, hasHash bool) (toChild bool, err error) {
				if k == nil || !hasTree || !hasHash {
					return hasTree, nil
				}
				if ok, _ = canUse(k); ok {
					return false, nil
				}

				return hasTree, common.Stopped(quit)
			}, func(k []byte) {
				misses = append(misses, common.CopyBytes(k))
			}); err != nil {
				return false, err
			}
			return true, nil
		}); err != nil {
			return nil, err
		}
	}
	return misses, nil
}

func loadStorageToCache(ss ethdb.Cursor, misses [][]byte, cache *shards.StateCache, quit <-chan struct{}) error {
	seek, buf := make([]byte, 0, 64), make([]byte, 0, 64)
	for _, miss := range misses {
		if err := common.Stopped(quit); err != nil {
			return err
		}
		fixedBits := 40*8 + len(miss[40:])*4
		buf = append(buf[:0], miss[40:]...)
		if len(buf)%2 == 1 {
			buf = append(buf, 0)
		}
		hexutil.CompressNibbles(buf, &buf)
		seek = append(append(seek[:0], miss[:40]...), buf...)
		if err := ethdb.Walk(ss, seek, fixedBits, func(k, v []byte) (bool, error) {
			accountHash := common.BytesToHash(k[:32])
			incarnation := binary.BigEndian.Uint64(k[32:40])
			stHash := common.BytesToHash(k[40:])
			if _, ok := cache.GetStorageByHashedAddress(accountHash, incarnation, stHash); !ok {
				cache.DeprecatedSetStorageRead(accountHash, incarnation, stHash, v)
			}
			return true, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func collectMissedAccounts(canUse func([]byte) (bool, []byte), prefix []byte, cache *shards.StateCache, quit <-chan struct{}) ([][]byte, error) {
	var misses [][]byte
	if err := cache.AccountTree(prefix, func(k []byte, v common.Hash, hasTree, hasHash bool) (toChild bool, err error) {
		if k == nil {
			return hasTree, nil
		}

		if !hasHash {
			if !cache.HasAccountWithInPrefix(k) {
				misses = append(misses, common.CopyBytes(k))
			}
			return hasTree, nil
		}

		if ok, _ := canUse(k); ok {
			return false, nil
		}

		if !cache.HasAccountWithInPrefix(k) {
			misses = append(misses, common.CopyBytes(k))
		}
		return hasTree, common.Stopped(quit)
	}, func(k []byte) {
		panic(fmt.Errorf("key %x not found in cache", k))
	}); err != nil {
		return nil, err
	}
	return misses, nil
}

func (l *FlatDBTrieLoader) prep(accs, st, trieAcc ethdb.Cursor, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) error {
	defer func(t time.Time) { fmt.Printf("trie_root.go:338: %s\n", time.Since(t)) }(time.Now())
	var canUse = func(prefix []byte) (bool, []byte) {
		retain, nextCreated := l.rd.RetainWithMarker(prefix)
		return !retain, nextCreated
	}
	accIHMisses, err := collectMissedAccTrie(canUse, prefix, cache, quit)
	if err != nil {
		return err
	}
	err = loadAccTrieToCache(trieAcc, prefix, accIHMisses, cache, quit)
	if err != nil {
		return err
	}
	accMisses, err := collectMissedAccounts(canUse, prefix, cache, quit)
	if err != nil {
		return err
	}
	storageIHMisses, err := loadAccsToCache(accs, accMisses, canUse, cache, quit)
	if err != nil {
		return err
	}
	err = loadStorageToCache(st, storageIHMisses, cache, quit)
	if err != nil {
		return err
	}
	return nil
}

func (l *FlatDBTrieLoader) walkAccountTree(prefix []byte, cache *shards.StateCache, walker func(ihK []byte, ihV common.Hash, hasTree, skipState bool, accSeek []byte) error) error {
	var canUse = func(prefix []byte) (bool, []byte) {
		retain, nextCreated := l.rd.RetainWithMarker(prefix)
		return !retain, nextCreated
	}

	var prev []byte
	_, nextCreated := canUse([]byte{})
	var skipState bool

	return cache.AccountTree(prefix, func(k []byte, h common.Hash, hasTree, hasHash bool) (toChild bool, err error) {
		if k == nil {
			skipState = skipState && !dbutils.NextNibblesSubtree(prev, &l.accSeek)
			return hasTree, walker(k, h, hasTree, skipState, l.accSeek)
		}
		if !hasTree && !hasHash {
			skipState = false
			return hasTree, nil
		}
		if !hasHash {
			return hasTree, nil
		}

		if ok, newNextCreated := canUse(k); ok {
			skipState = skipState && keyIsBefore(k, nextCreated)
			nextCreated = newNextCreated
			l.accSeek = firstNotCoveredPrefix(prev, prefix, l.accSeek)
			if err = walker(k, h, hasTree, skipState, l.accSeek); err != nil {
				return false, err
			}
			prev = append(prev[:0], k...)
			skipState = true
			return false, nil
		}
		skipState = skipState && hasTree

		// TODO: delete by hash collector and add protection from double-delete
		cache.SetAccountHashDelete(k[:len(k)-1])
		return hasTree, nil
	}, func(k []byte) {
		panic(fmt.Errorf("key %x not found in cache", k))
	})
}

func (l *FlatDBTrieLoader) post(storages ethdb.CursorDupSort, ihStorage *StorageTrieCursor, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) (common.Hash, error) {
	l.accSeek = make([]byte, 0, 64)
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	defer func(t time.Time) { fmt.Printf("trie_root.go:375: %s\n", time.Since(t)) }(time.Now())
	i2, i4 := 0, 0
	if err := l.walkAccountTree(prefix, cache, func(ihK []byte, ihV common.Hash, hasTree, skipState bool, accSeek []byte) error {
		if skipState {
			goto SkipAccounts
		}

		if err := cache.WalkAccounts(accSeek, func(addrHash common.Hash, acc *accounts.Account) (bool, error) {
			if err := common.Stopped(quit); err != nil {
				return false, err
			}
			i2++
			hexutil.DecompressNibbles(addrHash.Bytes(), &l.kHex)
			if keyIsBefore(ihK, l.kHex) || !bytes.HasPrefix(l.kHex, prefix) { // read all accounts until next AccTrie
				return false, nil
			}
			l.accountValue.Copy(acc)
			if err := l.receiver.Receive(AccountStreamItem, l.kHex, nil, &l.accountValue, nil, nil, false, 0); err != nil {
				return false, err
			}
			if l.accountValue.Incarnation == 0 {
				return true, nil
			}
			copy(l.accAddrHashWithInc[:], addrHash.Bytes())
			binary.BigEndian.PutUint64(l.accAddrHashWithInc[32:], l.accountValue.Incarnation)
			accWithInc := l.accAddrHashWithInc[:]
			for ihKS, ihVS, hasTreeS, err2 := ihStorage.SeekToAccount(accWithInc); ; ihKS, ihVS, hasTreeS, err2 = ihStorage.Next() {
				if err2 != nil {
					return false, err2
				}

				if ihStorage.skipState {
					goto SkipStorage
				}
				i4++
				for kS, vS, err3 := storages.SeekBothRange(accWithInc, ihStorage.FirstNotCoveredPrefix()); kS != nil; kS, vS, err3 = storages.NextDup() {
					if err3 != nil {
						return false, err3
					}
					hexutil.DecompressNibbles(vS[:32], &l.kHexS)
					if keyIsBefore(ihKS, l.kHexS) { // read until next AccTrie
						break
					}
					if err := l.receiver.Receive(StorageStreamItem, accWithInc, common.CopyBytes(l.kHexS), nil, vS[32:], nil, false, 0); err != nil {
						return false, err
					}
				}

			SkipStorage:
				if ihKS == nil { // Loop termination
					break
				}

				if err := l.receiver.Receive(SHashStreamItem, accWithInc, common.CopyBytes(ihKS), nil, nil, ihVS, hasTreeS, 0); err != nil {
					return false, err
				}
				if len(ihKS) == 0 { // means we just sent acc.storageRoot
					break
				}
			}

			select {
			default:
			case <-logEvery.C:
				l.logProgress(addrHash.Bytes(), ihK)
			}
			return true, nil
		}); err != nil {
			return err
		}

	SkipAccounts:
		if len(ihK) == 0 { // Loop termination
			return nil
		}

		if err := l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV[:], hasTree, 0); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return EmptyRoot, err
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, false, len(prefix)); err != nil {
		return EmptyRoot, err
	}

	return EmptyRoot, nil
}

func (l *FlatDBTrieLoader) CalcSubTrieRootOnCache(db ethdb.Database, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) (common.Hash, error) {
	var (
		tx ethdb.Tx
	)

	var txDB ethdb.DbWithPendingMutations
	var useExternalTx bool

	// If method executed within transaction - use it, or open new read transaction
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		txDB = hasTx.(ethdb.DbWithPendingMutations)
		tx = hasTx.Tx()
		useExternalTx = true
	} else {
		var err error
		txDB, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return EmptyRoot, err
		}

		defer txDB.Rollback()
		tx = txDB.(ethdb.HasTx).Tx()
	}

	accsC, stC := tx.Cursor(dbutils.HashedAccountsBucket), tx.Cursor(dbutils.HashedStorageBucket)
	defer accsC.Close()
	defer stC.Close()
	trieAccC, trieStorageC := tx.Cursor(dbutils.TrieOfAccountsBucket), tx.Cursor(dbutils.TrieOfStorageBucket)
	defer trieAccC.Close()
	defer trieStorageC.Close()
	var canUse = func(prefix []byte) (bool, []byte) {
		retain, nextCreated := l.rd.RetainWithMarker(prefix)
		return !retain, nextCreated
	}
	trieStorage := StorageTrie(canUse, l.shc, trieStorageC, quit)

	ss := tx.CursorDupSort(dbutils.HashedStorageBucket)
	defer ss.Close()
	_ = trieStorageC
	_ = stC

	if err := l.prep(accsC, stC, trieAccC, prefix, cache, quit); err != nil {
		return EmptyRoot, err
	}
	if _, err := l.post(ss, trieStorage, prefix, cache, quit); err != nil {
		return EmptyRoot, err
	}
	if !useExternalTx {
		_, err := txDB.Commit()
		if err != nil {
			return EmptyRoot, err
		}
	}
	//fmt.Printf("%d,%d,%d,%d\n", i1, i2, i3, i4)
	return l.receiver.Root(), nil
}

func (l *FlatDBTrieLoader) CalcTrieRootOnCache(cache *shards.StateCache) (common.Hash, error) {
	fmt.Printf("CalcTrieRootOnCache\n")
	if err := l.walkAccountTree([]byte{}, cache, func(ihK []byte, ihV common.Hash, hasTree, skipState bool, accSeek []byte) error {
		if len(ihK) == 0 { // Loop termination
			return nil
		}
		if err := l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV[:], hasTree, 0); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return EmptyRoot, err
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, false, 0); err != nil {
		return EmptyRoot, err
	}
	return l.receiver.Root(), nil
}
