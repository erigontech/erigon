package trie

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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

**Observation 1:** `CurrentStateBucketOld2` already stores state keys in sorted way.
Iteration over this bucket will retrieve keys in same order as "Preorder trie traversal".

**Observation 2:** each Eth block - changes not big part of state - it means most of Merkle trie intermediate hashes will not change.
It means we effectively can cache them. `IntermediateTrieHashBucketOld2` stores "Intermediate hashes of all Merkle trie levels".
It also sorted and Iteration over `IntermediateTrieHashBucketOld2` will retrieve keys in same order as "Preorder trie traversal".

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
On practice Trie is no full - it means after account key `{30 zero bytes}0000` may come `{5 zero bytes}01` and amount of iterations will not be big.

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
// Each intermediate hash key firstly pass to RetainDecider, only if it returns "false" - such IH can be used.
type FlatDBTrieLoader struct {
	logPrefix          string
	trace              bool
	rd                 RetainDecider
	accAddrHash        common.Hash // Concatenation of addrHash of the currently build account with its incarnation encoding
	accAddrHashWithInc [40]byte    // Concatenation of addrHash of the currently build account with its incarnation encoding

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
	hc              HashCollector
}

// RootHashAggregator - calculates Merkle trie root hash from incoming data stream
type RootHashAggregator struct {
	trace         bool
	wasIH         bool
	wasIHStorage  bool
	wasStorage    bool
	root          common.Hash
	hc            HashCollector
	shc           StorageHashCollector
	currStorage   bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage   bytes.Buffer
	valueStorage  []byte       // Current value to be used as the value tape for the hash builder
	hashAccount   common.Hash  // Current value to be used as the value tape for the hash builder
	hashStorage   common.Hash  // Current value to be used as the value tape for the hash builder
	curr          bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ          bytes.Buffer
	currAccK      []byte
	value         []byte   // Current value to be used as the value tape for the hash builder
	groups        []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
	groupsStorage []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
	hb            *HashBuilder
	hashData      GenStructStepHashData
	a             accounts.Account
	leafData      GenStructStepLeafData
	accData       GenStructStepAccountData
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
func (l *FlatDBTrieLoader) Reset(rd RetainDecider, hc HashCollector, shc StorageHashCollector, trace bool) error {
	l.defaultReceiver.Reset(hc, shc, trace)
	l.hc = hc
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
//		if isDenseSequence
//          goto SkipAccounts
//
//		for iterateAccounts from prevIH to currentIH {
//			use(account)
//			for iterateIHOfStorage within accountWithIncarnation{
//				if isDenseSequence
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
//		use(IH)
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

	accs, storages := NewStateCursor(tx.Cursor(dbutils.HashedAccountsBucket)), NewStateCursor(tx.Cursor(dbutils.HashedStorageBucket))
	ihAccC, ihStorageC := tx.Cursor(dbutils.IntermediateHashOfAccountBucket), tx.CursorDupSort(dbutils.IntermediateHashOfStorageBucket)
	var filter = func(prefix []byte) bool {
		if !l.rd.Retain(prefix) {
			return true
		}
		if err := l.hc(prefix, nil); err != nil {
			panic(err)
		}
		return false
	}

	ih := IH(filter, ihAccC)
	ihStorage := StorageIH(filter, ihStorageC)
	_ = storages

	ss := tx.CursorDupSort(dbutils.HashedStorageBucket)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	defer func(t time.Time) { fmt.Printf("trie_root.go:225: %s\n", time.Since(t)) }(time.Now())
	i1, i2, i3, i4 := 0, 0, 0, 0
	for ihK, ihV, err := ih.Seek(prefix); ; ihK, ihV, err = ih.Next() { // no loop termination is at he end of loop
		if err != nil {
			return EmptyRoot, err
		}
		i1++
		if isDenseSequence(ih.prev, ihK) {
			goto SkipAccounts
		}
		hexutil.CompressNibbles(ih.FirstNotCoveredPrefix(), &l.accSeek)
		if len(ih.PrevKey()) > 0 && len(l.accSeek) == 0 {
			break
		}

		for k, kHex, v, err1 := accs.Seek(l.accSeek); k != nil; k, kHex, v, err1 = accs.Next() {
			if err1 != nil {
				return EmptyRoot, err1
			}
			i2++

			if err = common.Stopped(quit); err != nil {
				return EmptyRoot, err
			}

			if keyIsBefore(ihK, kHex) || !bytes.HasPrefix(kHex, prefix) { // read all accounts until next IH
				break
			}
			if err = l.accountValue.DecodeForStorage(v); err != nil {
				return EmptyRoot, fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			if err = l.receiver.Receive(AccountStreamItem, kHex, nil, &l.accountValue, nil, nil, 0); err != nil {
				return EmptyRoot, err
			}
			if l.accountValue.Incarnation == 0 {
				continue
			}
			copy(l.accAddrHashWithInc[:], k)
			binary.BigEndian.PutUint64(l.accAddrHashWithInc[32:], l.accountValue.Incarnation)
			accWithInc := l.accAddrHashWithInc[:]
			for ihKS, ihVS, err2 := ihStorage.SeekToAccount(accWithInc); ; ihKS, ihVS, err2 = ihStorage.Next() {
				if err2 != nil {
					return EmptyRoot, err2
				}

				i3++
				if isDenseSequence(ihStorage.prev, ihKS) {
					goto SkipStorage
				}

				hexutil.CompressNibbles(ihStorage.FirstNotCoveredPrefix(), &l.storageSeek)
				if len(l.storageSeek) == 0 {
					l.storageSeek = []byte{0}
				}
				for kS, vS, err3 := ss.SeekBothRange(accWithInc, l.storageSeek); kS != nil; kS, vS, err3 = ss.NextDup() {
					if err3 != nil {
						return EmptyRoot, err3
					}
					i4++
					hexutil.DecompressNibbles(vS[:32], &l.kHexS)
					if keyIsBefore(ihKS, l.kHexS) { // read until next IH
						break
					}
					if err = l.receiver.Receive(StorageStreamItem, accWithInc, l.kHexS, nil, vS[32:], nil, 0); err != nil {
						return EmptyRoot, err
					}
				}

			SkipStorage:
				if len(ihKS) == 0 || !bytes.HasPrefix(ihKS, l.ihSeek) { // Loop termination
					break
				}

				if err = l.receiver.Receive(SHashStreamItem, accWithInc, ihKS, nil, nil, ihVS, 0); err != nil {
					return EmptyRoot, err
				}
			}

			select {
			default:
			case <-logEvery.C:
				l.logProgress(k, ihK)
			}
		}

	SkipAccounts:
		if len(ihK) == 0 || !bytes.HasPrefix(ihK, prefix) { // Loop termination
			break
		}

		if err = l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV, 0); err != nil {
			return EmptyRoot, err
		}
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, len(prefix)); err != nil {
		return EmptyRoot, err
	}

	if !useExternalTx {
		_, err := txDB.Commit()
		if err != nil {
			return EmptyRoot, err
		}
	}
	fmt.Printf("%d,%d,%d,%d\n", i1, i2, i3, i4)
	fmt.Printf("%d,%d\n", ih.i, ih.is)
	return l.receiver.Root(), nil
}

func (l *FlatDBTrieLoader) prep(accs *StateCursor, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) error {
	defer func(t time.Time) { fmt.Printf("trie_root.go:326: %s\n", time.Since(t)) }(time.Now())
	var prevIHK []byte
	if err := cache.WalkAccountHashes(func(ihK []byte) error {
		if l.rd.Retain(ihK) {
			cache.SetAccountHashDelete(ihK)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := cache.AccountHashes(prefix, func(ihK []byte, _ common.Hash) error {
		if isDenseSequence(prevIHK, ihK) {
			return nil
		}

		_ = dbutils.NextNibblesSubtree(prevIHK, &l.accSeek)
		if len(l.accSeek)%2 == 1 {
			l.accSeek = append(l.accSeek, 0)
		}
		hexutil.CompressNibbles(l.accSeek, &l.accSeek)
		if len(prevIHK) > 0 && len(l.accSeek) == 0 {
			return nil
		}
		for k, kHex, v, err := accs.Seek(l.accSeek); k != nil; k, kHex, v, err = accs.Next() {
			if err != nil {
				return err
			}
			if keyIsBefore(ihK, kHex) { // read all accounts until next IH
				break
			}
			if err := l.accountValue.DecodeForStorage(v); err != nil {
				return err
			}
			cache.DeprecatedSetAccountWrite(common.BytesToHash(k), &l.accountValue)
		}
		prevIHK = ihK
		return nil
	}); err != nil {
		return err
	}

	cache.TurnWritesToReads(cache.PrepareWrites())
	ihKSBuf := make([]byte, 256)
	if err := cache.WalkStorageHashes(func(addrHash common.Hash, incarnation uint64, locHashPrefix []byte, hash common.Hash) error {
		ihK := tmpMakeIHPrefix(addrHash, incarnation, locHashPrefix, ihKSBuf)
		if l.rd.Retain(ihK) {
			cache.SetStorageHashDelete(addrHash, incarnation, locHashPrefix, hash)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (l *FlatDBTrieLoader) post(storages ethdb.CursorDupSort, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) (common.Hash, error) {
	var prevIHK []byte
	//ihKSBuf := make([]byte, 256)
	firstNotCoveredPrefix := make([]byte, 0, 128)
	//lastPart := make([]byte, 0, 128)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	l.accSeek = make([]byte, 64)
	defer func(t time.Time) { fmt.Printf("trie_root.go:375: %s\n", time.Since(t)) }(time.Now())

	i1, i2, i3, i4 := 0, 0, 0, 0
	if err := cache.AccountHashes2(prefix, func(ihK []byte, ihV common.Hash) error {
		i1++
		if isDenseSequence(prevIHK, ihK) {
			goto SkipAccounts
		}

		//CompressNibbles(ih.FirstNotCoveredPrefix(), &l.accSeek)
		_ = dbutils.NextNibblesSubtree(prevIHK, &l.accSeek)
		if len(l.accSeek)%2 == 1 {
			l.accSeek = append(l.accSeek, 0)
		}
		hexutil.CompressNibbles(l.accSeek, &l.accSeek)
		if len(prevIHK) > 0 && len(l.accSeek) == 0 {
			return nil
		}
		if err := cache.WalkAccounts(l.accSeek, func(addrHash common.Hash, acc *accounts.Account) (bool, error) {
			if err := common.Stopped(quit); err != nil {
				return false, err
			}
			i2++
			hexutil.DecompressNibbles(addrHash.Bytes(), &l.kHex)
			if keyIsBefore(ihK, l.kHex) { // read all accounts until next IH
				return false, nil
			}
			l.accountValue = *acc
			if err := l.receiver.Receive(AccountStreamItem, l.kHex, nil, &l.accountValue, nil, nil, 0); err != nil {
				return false, err
			}
			if l.accountValue.Incarnation == 0 {
				return true, nil
			}
			copy(l.accAddrHashWithInc[:], addrHash.Bytes())
			binary.BigEndian.PutUint64(l.accAddrHashWithInc[32:], l.accountValue.Incarnation)
			l.accAddrHash.SetBytes(addrHash.Bytes())
			var prevIHKS []byte
			if err := cache.StorageHashes(l.accAddrHash, l.accountValue.Incarnation, func(ihKS []byte, h common.Hash) error {
				i3++
				if isDenseSequence(prevIHKS, ihKS) {
					goto SkipStorage
				}

				_ = dbutils.NextNibblesSubtree(prevIHKS, &firstNotCoveredPrefix)
				if len(firstNotCoveredPrefix)%2 == 1 {
					firstNotCoveredPrefix = append(firstNotCoveredPrefix, 0)
				}
				hexutil.CompressNibbles(firstNotCoveredPrefix, &l.storageSeek)
				if len(l.storageSeek) == 0 {
					l.storageSeek = []byte{0}
				}
				for kS, vS, err3 := storages.SeekBothRange(l.accAddrHashWithInc[:], l.storageSeek); kS != nil; kS, vS, err3 = storages.NextDup() {
					if err3 != nil {
						return err3
					}
					i4++
					hexutil.DecompressNibbles(vS[:32], &l.kHexS)
					if keyIsBefore(ihKS, l.kHexS) { // read until next IH
						break
					}
					if err := l.receiver.Receive(StorageStreamItem, l.accAddrHashWithInc[:], l.kHexS, nil, vS[32:], nil, 0); err != nil {
						return err
					}
				}

			SkipStorage:
				if len(ihKS) == 0 || !bytes.HasPrefix(ihKS, l.ihSeek) { // Loop termination
					return nil
				}

				if err := l.receiver.Receive(SHashStreamItem, l.accAddrHashWithInc[:], ihKS, nil, nil, h.Bytes(), 0); err != nil {
					return err
				}
				prevIHKS = ihKS
				return nil
			}); err != nil {
				return false, err
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

		if err := l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV[:], 0); err != nil {
			return err
		}
		prevIHK = ihK
		return nil
	}); err != nil {
		return EmptyRoot, err
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, len(prefix)); err != nil {
		return EmptyRoot, err
	}
	fmt.Printf("%d,%d,%d,%d\n", i1, i2, i3, i4)
	return EmptyRoot, nil
}

func (l *FlatDBTrieLoader) CalcTrieRootOnCache(db ethdb.Database, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) (common.Hash, error) {
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

	accs, storages := NewStateCursor(tx.Cursor(dbutils.HashedAccountsBucket)), NewStateCursor(tx.Cursor(dbutils.HashedStorageBucket))
	ihAccC, ihStorageC := tx.Cursor(dbutils.IntermediateHashOfAccountBucket), tx.Cursor(dbutils.IntermediateHashOfStorageBucket)
	ss := tx.CursorDupSort(dbutils.HashedStorageBucket)
	var filter = func(prefix []byte) bool {
		if !l.rd.Retain(prefix) {
			return true
		}
		//if err := l.hc(prefix, nil); err != nil {
		//	panic(err)
		//}
		return false
	}
	ih := IH(filter, ihAccC)
	ihStorage := IH(filter, ihStorageC)
	_, _, _, _ = ih, ihStorage, accs, storages

	if err := l.prep(accs, prefix, cache, quit); err != nil {
		panic(err)
	}
	if _, err := l.post(ss, prefix, cache, quit); err != nil {
		panic(err)
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

func (l *FlatDBTrieLoader) CalcTrieRootOnCache2(cache *shards.StateCache) (common.Hash, error) {
	if err := cache.AccountHashes2([]byte{}, func(ihK []byte, ihV common.Hash) error {
		if len(ihK) == 0 { // Loop termination
			return nil
		}
		if err := l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV[:], 0); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return EmptyRoot, err
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, 0); err != nil {
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

func (r *RootHashAggregator) Reset(hc HashCollector, shc StorageHashCollector, trace bool) {
	r.hc = hc
	r.shc = shc
	r.curr.Reset()
	r.succ.Reset()
	r.value = nil
	r.groups = r.groups[:0]
	r.a.Reset()
	r.hb.Reset()
	r.wasIH = false
	r.currStorage.Reset()
	r.succStorage.Reset()
	r.valueStorage = nil
	r.wasIHStorage, r.wasStorage = false, false
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
	cutoff int,
) error {
	//fmt.Printf("1: %d, %x, %x, %x\n", itemType, accountKey, storageKey, hash)
	switch itemType {
	case StorageStreamItem:
		if len(r.currAccK) == 0 {
			r.currAccK = append(r.currAccK[:0], accountKey...)
		}
		r.advanceKeysStorage(storageKey, true /* terminator */)
		if r.wasStorage || r.wasIHStorage {
			if err := r.genStructStorage(); err != nil {
				return err
			}
		}
		r.saveValueStorage(false, storageValue, hash)
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
		if r.wasStorage || r.wasIHStorage {
			if err := r.genStructStorage(); err != nil {
				return err
			}
		}
		r.saveValueStorage(true, storageValue, hash)
	case AccountStreamItem:
		r.advanceKeysAccount(accountKey, true /* terminator */)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(0)
			if r.wasStorage || r.wasIHStorage {
				if err := r.genStructStorage(); err != nil {
					return err
				}
				r.currStorage.Reset()
				r.succStorage.Reset()
				r.wasIHStorage, r.wasStorage = false, false
				r.groupsStorage = r.groupsStorage[:0]
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
		if err := r.saveValueAccount(false, accountValue, hash); err != nil {
			return err
		}
	case AHashStreamItem:
		r.advanceKeysAccount(accountKey, false /* terminator */)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(0)
			if r.wasStorage || r.wasIHStorage {
				if err := r.genStructStorage(); err != nil {
					return err
				}
				r.currStorage.Reset()
				r.succStorage.Reset()
				r.wasIHStorage, r.wasStorage = false, false
				r.groupsStorage = r.groupsStorage[:0]
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
		if err := r.saveValueAccount(true, accountValue, hash); err != nil {
			return err
		}
	case CutoffStreamItem:
		if r.trace {
			fmt.Printf("storage cuttoff %d\n", cutoff)
		}
		r.cutoffKeysAccount(cutoff)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(0)
			if r.wasStorage || r.wasIHStorage {
				if err := r.genStructStorage(); err != nil {
					return err
				}
				r.currStorage.Reset()
				r.succStorage.Reset()
				r.wasIHStorage, r.wasStorage = false, false
				r.groupsStorage = r.groupsStorage[:0]
				// There are some storage items
				r.accData.FieldSet |= AccountFieldStorageOnly
			}
		}
		if r.curr.Len() > 0 {
			if err := r.genStructAccount(); err != nil {
				return err
			}
		}
		if r.hb.hasRoot() {
			r.root = r.hb.rootHash()
		} else {
			r.root = EmptyRoot
		}
		r.groups = r.groups[:0]
		r.hb.Reset()
		r.wasIH = false
		r.wasIHStorage = false
		r.wasStorage = false
		r.curr.Reset()
		r.succ.Reset()
		r.currStorage.Reset()
		r.succStorage.Reset()
	}
	return nil
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
	//	r.succStorage.Write(r.currStorage.Bytes()[:cutoff-1])
	//	r.succStorage.WriteByte(r.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
	//}
}

func (r *RootHashAggregator) genStructStorage() error {
	var err error
	var data GenStructStepData
	if r.wasIHStorage {
		r.hashData.Hash = r.hashStorage
		data = &r.hashData
	} else {
		r.leafData.Value = rlphacks.RlpSerializableBytes(r.valueStorage)
		data = &r.leafData
	}
	r.groupsStorage, err = GenStructStep(r.RetainNothing, r.currStorage.Bytes(), r.succStorage.Bytes(), r.hb, func(keyHex []byte, hash []byte) error {
		return r.shc(r.currAccK, keyHex, hash)
	}, data, r.groupsStorage, r.trace)
	if err != nil {
		return err
	}
	return nil
}

func (r *RootHashAggregator) saveValueStorage(isIH bool, v, h []byte) {
	// Remember the current value
	r.wasIHStorage = isIH
	r.wasStorage = !isIH
	r.valueStorage = nil
	if isIH {
		r.hashStorage.SetBytes(h)
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
		//copy(r.hashData.Hash[:], r.value)
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
	if r.groups, err = GenStructStep(r.RetainNothing, r.curr.Bytes(), r.succ.Bytes(), r.hb, r.hc, data, r.groups, r.trace); err != nil {
		return err
	}
	r.accData.FieldSet = 0
	return nil
}

func (r *RootHashAggregator) saveValueAccount(isIH bool, v *accounts.Account, h []byte) error {
	r.wasStorage = false
	r.wasIH = isIH
	if isIH {
		r.hashAccount.SetBytes(h)
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

type Filter func([]byte) bool // returns false - if element must be skipped

const IHDupKeyLen = 2 * (common.HashLength + common.IncarnationLength)

// IHCursor - holds logic related to iteration over IH bucket
type IHCursor struct {
	c                     ethdb.Cursor
	i                     int
	is                    int
	prev                  []byte
	seek                  []byte
	cur                   []byte
	next                  []byte
	firstNotCoveredPrefix []byte
	filter                func(prefix []byte) bool
}

func IH(filter func(prefix []byte) bool, c ethdb.Cursor) *IHCursor {
	ih := &IHCursor{c: c, filter: filter, firstNotCoveredPrefix: make([]byte, 0, 256), next: make([]byte, 256)}
	return ih
}

func (c *IHCursor) PrevKey() []byte {
	return c.prev
}

func (c *IHCursor) FirstNotCoveredPrefix() []byte {
	if len(c.prev) > 0 {
		_ = dbutils.NextNibblesSubtree(c.prev, &c.firstNotCoveredPrefix)
		if len(c.firstNotCoveredPrefix)%2 == 1 {
			c.firstNotCoveredPrefix = append(c.firstNotCoveredPrefix, 0)
		}
		return c.firstNotCoveredPrefix
	}
	if len(c.seek)%2 == 1 {
		c.seek = append(c.seek, 0)
	}
	return c.seek
}

func (c *IHCursor) First() (k, v []byte, err error) {
	k, v, err = c._first()
	if err != nil {
		return []byte{}, nil, err
	}

	if k == nil {
		return nil, nil, nil
	}
	c.cur = k
	return c.cur, v, nil
}

func (c *IHCursor) Seek(seek []byte) (k, v []byte, err error) {
	c.seek = seek
	k, v, err = c._seek(seek)
	if err != nil {
		return []byte{}, nil, err
	}

	c.prev = c.cur
	if k == nil {
		c.cur = nil
		return nil, nil, nil
	}
	c.cur = k
	return c.cur, v, nil
}

func (c *IHCursor) Next() (k, v []byte, err error) {
	ok := dbutils.NextNibblesSubtree(c.cur, &c.next)
	if !ok {
		c.prev = c.cur
		c.cur = nil
		return nil, nil, nil
	}
	k, v, err = c._seek(c.next)
	if err != nil {
		return []byte{}, nil, err
	}

	c.prev = c.cur
	if k == nil {
		c.cur = nil
		return nil, nil, nil
	}

	c.cur = k
	return c.cur, v, nil
}

func (c *IHCursor) _first() (k, v []byte, err error) {
	k, v, err = c.c.First()
	if err != nil {
		return []byte{}, nil, err
	}

	if k == nil {
		return nil, nil, nil
	}

	if c.filter(k) { // if rd allow us, return. otherwise delete and go ahead.
		return k, v, nil
	}

	return c._next()
}

func (c *IHCursor) _seek(seek []byte) (k, v []byte, err error) {
	c.is++
	k, v, err = c.c.Seek(seek)
	if err != nil {
		return []byte{}, nil, err
	}
	if k == nil {
		return nil, nil, nil
	}

	if c.filter(k) { // if rd allow us, return. otherwise delete and go ahead.
		return k, v, nil
	}

	return c._next()
}

func (c *IHCursor) _next() (k, v []byte, err error) {
	c.i++
	k, v, err = c.c.Next()
	if err != nil {
		return []byte{}, nil, err
	}
	for {
		if k == nil {
			return nil, nil, nil
		}

		if c.filter(k) { // if rd allow us, return. otherwise delete and go ahead.
			return k, v, nil
		}

		c.i++
		k, v, err = c.c.Next()
		if err != nil {
			return []byte{}, nil, err
		}
	}
}

// Storage - holds logic related to iteration over IH bucket
type StorageIHCursor struct {
	c                     ethdb.CursorDupSort
	i                     int
	prev                  []byte
	cur                   []byte
	next                  []byte
	firstNotCoveredPrefix []byte
	accWithInc            []byte
	filterBuf             []byte
	filter                func(prefix []byte) bool
}

func StorageIH(filter func(prefix []byte) bool, c ethdb.CursorDupSort) *StorageIHCursor {
	ih := &StorageIHCursor{c: c, filter: filter, i: 1, firstNotCoveredPrefix: make([]byte, 0, 256), next: make([]byte, 256), filterBuf: make([]byte, 256)}
	return ih
}

func (c *StorageIHCursor) PrevKey() []byte {
	return c.prev
}

func (c *StorageIHCursor) FirstNotCoveredPrefix() []byte {
	_ = dbutils.NextNibblesSubtree(c.prev, &c.firstNotCoveredPrefix)
	if len(c.firstNotCoveredPrefix)%2 == 1 {
		c.firstNotCoveredPrefix = append(c.firstNotCoveredPrefix, 0)
	}
	return c.firstNotCoveredPrefix
}

func (c *StorageIHCursor) First() (k, v []byte, err error) {
	k, v, err = c._first()
	if err != nil {
		return []byte{}, nil, err
	}

	if k == nil {
		return nil, nil, nil
	}
	c.cur = append(c.cur[:0], k...)
	return c.cur, v, nil
}

func (c *StorageIHCursor) SeekToAccount(seek []byte) (k, v []byte, err error) {
	c.accWithInc = seek
	k, v, err = c._seek([]byte{0})
	if err != nil {
		return []byte{}, nil, err
	}
	c.prev = c.prev[:0]

	if k == nil {
		return nil, nil, nil
	}

	c.cur = append(c.cur[:0], k...)
	return c.cur, v, nil
}

func (c *StorageIHCursor) Next() (k, v []byte, err error) {
	ok := dbutils.NextNibblesSubtree(c.cur, &c.next)
	if !ok {
		c.prev = append(c.prev[:0], c.cur...)
		c.cur = nil
		return nil, nil, nil
	}
	k, v, err = c._seek(c.next)
	if err != nil {
		return []byte{}, nil, err
	}

	c.prev = append(c.prev[:0], c.cur...)
	if k == nil {
		c.cur = nil
		return nil, nil, nil
	}
	c.cur = append(c.cur[:0], k...)
	return c.cur, v, nil
}

func (c *StorageIHCursor) _first() (k, v []byte, err error) {
	k, v, err = c.c.First()
	if err != nil {
		return []byte{}, nil, err
	}

	if k == nil {
		return nil, nil, nil
	}

	keyPart := len(v) - common.HashLength
	k = v[:keyPart]
	v = v[keyPart:]

	hexutil.DecompressNibbles(c.accWithInc, &c.filterBuf)
	c.filterBuf = append(c.filterBuf, k...)
	if c.filter(c.filterBuf) { // if rd allow us, return. otherwise delete and go ahead.
		return k, v, nil
	}

	err = c.c.DeleteCurrent()
	if err != nil {
		return []byte{}, nil, err
	}

	return c._next()
}

func (c *StorageIHCursor) _seek(seek []byte) (k, v []byte, err error) {
	k, v, err = c.c.SeekBothRange(c.accWithInc, seek)
	if err != nil {
		return []byte{}, nil, err
	}

	if k == nil {
		return nil, nil, nil
	}

	keyPart := len(v) - common.HashLength
	k = v[:keyPart]
	v = v[keyPart:]

	hexutil.DecompressNibbles(c.accWithInc, &c.filterBuf)
	c.filterBuf = append(c.filterBuf, k...)
	if c.filter(c.filterBuf) { // if rd allow us, return. otherwise delete and go ahead.
		return k, v, nil
	}

	err = c.c.DeleteCurrent()
	if err != nil {
		return []byte{}, nil, err
	}

	return c._next()
}

func (c *StorageIHCursor) _next() (k, v []byte, err error) {
	k, v, err = c.c.NextDup()
	if err != nil {
		return []byte{}, nil, err
	}
	for {
		if k == nil {
			return nil, nil, nil
		}

		keyPart := len(v) - common.HashLength
		k = v[:keyPart]
		v = v[keyPart:]

		hexutil.DecompressNibbles(c.accWithInc, &c.filterBuf)
		c.filterBuf = append(c.filterBuf, k...)
		if c.filter(c.filterBuf) { // if rd allow us, return. otherwise delete and go ahead.
			return k, v, nil
		}

		err = c.c.DeleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}

		k, v, err = c.c.NextDup()
		if err != nil {
			return []byte{}, nil, err
		}
	}
}

/*
	Dense Sequence - if between 2 IH records not possible insert any state record - then they form "dense sequence"
	If 2 IH records form Dense Sequence - then no reason to iterate over state - just use IH one after another
	Example1:
		1234
		1235
	Example2:
		12ff
		13
	Example3:
		12ff
		13000000
	If 2 IH records form "sequence" then it can be consumed without moving StateCursor
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

type StateCursor struct {
	c    ethdb.Cursor
	kHex []byte
}

func NewStateCursor(c ethdb.Cursor) *StateCursor {
	return &StateCursor{c: c}
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

func nextAccountHex(in, out []byte) bool {
	copy(out, in)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 15 {
			out[i]++
			return true
		}
		out[i] = 0
	}
	return false
}

// keyIsBefore - kind of bytes.Compare, but nil is the last key. And return
func keyIsBeforeOrEqual(k1, k2 []byte) (bool, []byte) {
	if k1 == nil {
		return false, k2
	}

	if k2 == nil {
		return true, k1
	}

	switch bytes.Compare(k1, k2) {
	case -1, 0:
		return true, k1
	default:
		return false, k2
	}
}

// keyIsBefore - kind of bytes.Compare, but nil is the last key. And return
func keyIsBefore(k1, k2 []byte) bool {
	if k1 == nil {
		return false
	}

	if k2 == nil {
		return true
	}

	switch bytes.Compare(k1, k2) {
	case -1:
		return true
	default:
		return false
	}
}

func tmpMakeIHPrefix(addrHash common.Hash, incarnation uint64, prefix []byte, buf []byte) []byte {
	hexutil.DecompressNibbles(addrHash.Bytes(), &buf)
	incBuf := buf[80:96]
	binary.BigEndian.PutUint64(incBuf, incarnation)
	to := buf[64:]
	hexutil.DecompressNibbles(incBuf, &to)
	l := 80 + len(prefix)
	buf = buf[:l]
	copy(buf[80:l], prefix)
	return common.CopyBytes(buf)
}
