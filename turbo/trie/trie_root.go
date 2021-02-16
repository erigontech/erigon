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
	hc              HashCollector2
	shc             StorageHashCollector2
}

// RootHashAggregator - calculates Merkle trie root hash from incoming data stream
type RootHashAggregator struct {
	trace            bool
	wasIH            bool
	wasIHStorage     bool
	root             common.Hash
	hc               HashCollector2
	shc              StorageHashCollector2
	currStorage      bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage      bytes.Buffer
	valueStorage     []byte // Current value to be used as the value tape for the hash builder
	hadBranchStorage bool
	hashAccount      common.Hash  // Current value to be used as the value tape for the hash builder
	hashStorage      common.Hash  // Current value to be used as the value tape for the hash builder
	curr             bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ             bytes.Buffer
	currAccK         []byte
	value            []byte // Current value to be used as the value tape for the hash builder
	hadBranchAcc     bool
	groups           []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
	hasBranch        []uint16
	hasHash          []uint16
	groupsStorage    []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
	hasBranchStorage []uint16
	hasHashStorage   []uint16
	hb               *HashBuilder
	hashData         GenStructStepHashData
	a                accounts.Account
	leafData         GenStructStepLeafData
	accData          GenStructStepAccountData
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
func (l *FlatDBTrieLoader) Reset(rd RetainDecider, hc HashCollector2, shc StorageHashCollector2, trace bool) error {
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

	accs := NewStateCursor(tx.Cursor(dbutils.HashedAccountsBucket), quit)
	ihAccC, ihStorageC := tx.Cursor(dbutils.TrieOfAccountsBucket), tx.CursorDupSort(dbutils.TrieOfStorageBucket)

	var canUse = func(prefix []byte) bool { return !l.rd.Retain(prefix) }
	ih := IH(canUse, l.hc, ihAccC, quit)
	ihStorage := IHStorage2(canUse, l.shc, ihStorageC, quit)

	ss := tx.CursorDupSort(dbutils.HashedStorageBucket)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	defer func(t time.Time) { fmt.Printf("trie_root.go:225: %s\n", time.Since(t)) }(time.Now())
	i2, i4 := 0, 0
	for ihK, ihV, hasBranch, err := ih.AtPrefix(prefix); ; ihK, ihV, hasBranch, err = ih.Next() { // no loop termination is at he end of loop
		if err != nil {
			return EmptyRoot, err
		}
		if ih.SkipState {
			goto SkipAccounts
		}

		i2++
		for k, kHex, v, err1 := accs.Seek(ih.FirstNotCoveredPrefix()); k != nil; k, kHex, v, err1 = accs.Next() {
			if err1 != nil {
				return EmptyRoot, err1
			}
			if keyIsBefore(ihK, kHex) || !bytes.HasPrefix(kHex, prefix) { // read all accounts until next IH
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
			for ihKS, ihVS, hasBranchS, err2 := ihStorage.SeekToAccount(accWithInc); ; ihKS, ihVS, hasBranchS, err2 = ihStorage.Next() {
				if err2 != nil {
					return EmptyRoot, err2
				}

				if ihStorage.skipState {
					goto SkipStorage
				}
				i4++
				for kS, vS, err3 := ss.SeekBothRange(accWithInc, ihStorage.FirstNotCoveredPrefix()); kS != nil; kS, vS, err3 = ss.NextDup() {
					if err3 != nil {
						return EmptyRoot, err3
					}
					hexutil.DecompressNibbles(vS[:32], &l.kHexS)
					if keyIsBefore(ihKS, l.kHexS) { // read until next IH
						break
					}
					if err = l.receiver.Receive(StorageStreamItem, accWithInc, common.CopyBytes(l.kHexS), nil, vS[32:], nil, false, 0); err != nil {
						return EmptyRoot, err
					}
				}

			SkipStorage:
				if ihKS == nil { // Loop termination
					break
				}

				if err = l.receiver.Receive(SHashStreamItem, accWithInc, common.CopyBytes(ihKS), nil, nil, ihVS, hasBranchS, 0); err != nil {
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

		if err = l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV, hasBranch, 0); err != nil {
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
	fmt.Printf("%d,%d,%d,%d\n", ih.is, i2, ihStorage.is, i4)
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
	r.hasBranch = r.hasBranch[:0]
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
	hasBranch bool,
	cutoff int,
) error {
	//r.traceIf("9c3dc2561d472d125d8f87dde8f2e3758386463ade768ae1a1546d34101968bb", "00")
	//if storageKey == nil {
	//	//if bytes.HasPrefix(accountKey, common.FromHex("08050d07")) {
	//	fmt.Printf("1: %d, %x, %x\n", itemType, accountKey, hash)
	//	//}
	//} else {
	//	//if bytes.HasPrefix(accountKey, common.FromHex("9c3dc2561d472d125d8f87dde8f2e3758386463ade768ae1a1546d34101968bb")) && bytes.HasPrefix(storageKey, common.FromHex("00")) {
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
		r.saveValueStorage(false, hasBranch, storageValue, hash)
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
		r.saveValueStorage(true, hasBranch, storageValue, hash)
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
				r.hasBranchStorage = r.hasBranchStorage[:0]
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
		if err := r.saveValueAccount(false, hasBranch, accountValue, hash); err != nil {
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
				r.hasBranchStorage = r.hasBranchStorage[:0]
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
		if err := r.saveValueAccount(true, hasBranch, accountValue, hash); err != nil {
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
				r.hasBranchStorage = r.hasBranchStorage[:0]
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
				r.hasBranch = r.hasBranch[:cutoff]
				r.hasHash = r.hasHash[:cutoff]
			}
		}
		if r.hb.hasRoot() {
			r.root = r.hb.rootHash()
		} else {
			r.root = EmptyRoot
		}
		r.groups = r.groups[:0]
		r.hasBranch = r.hasBranch[:0]
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
		r.hashData.IsBranch = r.hadBranchStorage
		data = &r.hashData
	} else {
		r.leafData.Value = rlphacks.RlpSerializableBytes(r.valueStorage)
		data = &r.leafData
	}
	r.groupsStorage, r.hasBranchStorage, r.hasHashStorage, err = GenStructStep(r.RetainNothing, r.currStorage.Bytes(), r.succStorage.Bytes(), r.hb, func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		if r.shc == nil {
			return nil
		}
		//if bytes.HasPrefix(r.currAccK, common.FromHex("9c3dc2561d472d125d8f87dde8f2e3758386463ade768ae1a1546d34101968bb0000000000000001")) && bytes.HasPrefix(keyHex, common.FromHex("00")) {
		//	fmt.Printf("collect: %x,%x,%016b,%016b, del:%t\n", r.currAccK, keyHex, hasHash, hasBranch, hashes == nil && rootHash == nil)
		//}
		return r.shc(r.currAccK, keyHex, hasState, hasBranch, hasHash, hashes, rootHash)
	}, data, r.groupsStorage, r.hasBranchStorage, r.hasHashStorage,
		//false,
		r.trace,
	)
	if err != nil {
		return err
	}
	return nil
}

func (r *RootHashAggregator) saveValueStorage(isIH, hasBranch bool, v, h []byte) {
	// Remember the current value
	r.wasIHStorage = isIH
	r.valueStorage = nil
	if isIH {
		r.hashStorage.SetBytes(h)
		r.hadBranchStorage = hasBranch
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
		r.hashData.IsBranch = r.hadBranchAcc
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
	if r.groups, r.hasBranch, r.hasHash, err = GenStructStep(r.RetainNothing, r.curr.Bytes(), r.succ.Bytes(), r.hb, func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		if r.hc == nil {
			return nil
		}

		//if bytes.HasPrefix(keyHex, common.FromHex("060e")) {
		//	fmt.Printf("collect: %x,%b,%b, del:%t\n", keyHex, hasHash, hasBranch, hashes == nil)
		//}
		return r.hc(keyHex, hasState, hasBranch, hasHash, hashes, rootHash)
	}, data, r.groups, r.hasBranch, r.hasHash,
		false,
		//r.trace,
	); err != nil {
		return err
	}
	r.accData.FieldSet = 0
	return nil
}

func (r *RootHashAggregator) saveValueAccount(isIH, hasBranch bool, v *accounts.Account, h []byte) error {
	r.wasIH = isIH
	if isIH {
		r.hashAccount.SetBytes(h)
		r.hadBranchAcc = hasBranch
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

type CanUse func([]byte) bool // returns false - if element must be skipped

const IHDupKeyLen = 2 * (common.HashLength + common.IncarnationLength)

// IHCursor - holds logic related to iteration over IH bucket
// has 2 basic operations:  _preOrderTraversalStep and _preOrderTraversalStepNoInDepth
type IHCursor struct {
	SkipState                    bool
	is, lvl                      int
	k, v                         [64][]byte // store up to 64 levels of key/value pairs in nibbles format
	hasState, hasBranch, hasHash [64]uint16 // branch hasState set, and any hasState set
	childID, hashID              [64]int16  // meta info: current child in .hasState[lvl] field, max child id, current hash in .v[lvl]
	deleted                      [64]bool   // helper to avoid multiple deletes of same key

	c                     ethdb.Cursor
	hc                    HashCollector2
	seek, prev, cur, next []byte
	prefix                []byte // global prefix - cursor will never return records without this prefix

	firstNotCoveredPrefix []byte
	canUse                func(prefix []byte) bool // if this function returns true - then this IH can be used as is and don't need continue PostorderTraversal, but switch to sibling instead

	kBuf []byte
	quit <-chan struct{}
}

func IH(canUse func(prefix []byte) bool, hc HashCollector2, c ethdb.Cursor, quit <-chan struct{}) *IHCursor {
	ih := &IHCursor{c: c, canUse: canUse,
		firstNotCoveredPrefix: make([]byte, 0, 64),
		next:                  make([]byte, 64),
		hc:                    hc,
		quit:                  quit,
	}
	return ih
}

// _preOrderTraversalStep - goToChild || nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *IHCursor) _preOrderTraversalStep() error {
	if c._hasBranch() {
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
func (c *IHCursor) _preOrderTraversalStepNoInDepth() error {
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

func (c *IHCursor) FirstNotCoveredPrefix() []byte {
	c.firstNotCoveredPrefix = firstNotCoveredPrefix(c.prev, c.seek, c.firstNotCoveredPrefix)
	return c.firstNotCoveredPrefix
}

func (c *IHCursor) AtPrefix(prefix []byte) (k, v []byte, hasBranch bool, err error) {
	c.SkipState = false
	c.prev = append(c.prev[:0], c.cur...)
	c.prefix = prefix
	c.seek = prefix
	ok, err := c._seek(prefix, []byte{})
	if err != nil {
		return []byte{}, nil, false, err
	}
	if !ok {
		c.cur = nil
		c.SkipState = isDenseSequence(c.prev, c.cur)
		return nil, nil, false, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if c.canUse(c.kBuf) {
			c.cur = append(c.cur[:0], c.kBuf...)
			c.SkipState = isDenseSequence(c.prev, c.cur)
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasBranch(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
	}

	return c._next()
}

func (c *IHCursor) Next() (k, v []byte, hasBranch bool, err error) {
	if err = common.Stopped(c.quit); err != nil {
		return []byte{}, nil, false, err
	}

	c.SkipState = false
	c.prev = append(c.prev[:0], c.cur...)
	err = c._preOrderTraversalStepNoInDepth()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if c.k[c.lvl] == nil {
		c.cur = nil
		c.SkipState = isDenseSequence(c.prev, c.cur)
		return nil, nil, false, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		//if bytes.HasPrefix(c.kBuf, common.FromHex("060e")) {
		//	fmt.Printf(".Next can use: %x ->%t\n", c.kBuf, c.canUse(c.kBuf))
		//}
		if c.canUse(c.kBuf) {
			c.cur = append(c.cur[:0], c.kBuf...)
			c.SkipState = isDenseSequence(c.prev, c.cur)
			//fmt.Printf("Next2: %x, %d,%b,%d\n", c.k, c.childID[c.lvl], c.hasHash[c.lvl], len(c.v))
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasBranch(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
	}

	return c._next()
}

func (c *IHCursor) _seek(seek []byte, withinPrefix []byte) (bool, error) {
	var k, v []byte
	var err error
	if len(seek) == 0 {
		k, v, err = c.c.First()
	} else {
		// optimistic .Next call, can use result in 2 cases:
		// - no child found, means: len(k) <= c.lvl
		// - looking for first child, means: c.childID[c.lvl] <= int16(bits.TrailingZeros16(c.hasBranch[c.lvl]))
		// otherwise do .Seek call
		//k, v, err = c.c.Next()
		//if err != nil {
		//	return false, err
		//}
		//if len(k) > c.lvl && c.childID[c.lvl] > int16(bits.TrailingZeros16(c.hasBranch[c.lvl])) {
		//	c.is++
		k, v, err = c.c.Seek(seek)
		//}
		//fmt.Printf("_seek1: %x ->%x,%b\n", seek, k, c.hasBranch[c.lvl])
	}
	if err != nil {
		return false, err
	}
	if k == nil || !bytes.HasPrefix(k, withinPrefix) || !bytes.HasPrefix(k, c.prefix) {
		//fmt.Printf("_seek2: %x ->%x\n", withinPrefix, k)
		return false, nil
	}
	c._unmarshal(k, v)
	c._nextSiblingInMem()
	//fmt.Printf("_seek3: %x ->%x, %x, %x\n", withinPrefix, k, c.k, c.childID)
	return true, nil
}

func (c *IHCursor) _nextSiblingInMem() bool {
	for c.childID[c.lvl]++; c.childID[c.lvl] < int16(bits.Len16(c.hasState[c.lvl])); c.childID[c.lvl]++ {
		// TODO: replace isDenseSequence() by next logic
		//c.SkipState = c.SkipState && ((1<<(c.childID[c.lvl]-1))&c.hasState[c.lvl]) == 0 // if prev child has state - then we skipped some state

		if ((1 << c.childID[c.lvl]) & c.hasHash[c.lvl]) != 0 {
			c.hashID[c.lvl]++
			return true
		}
		if ((1 << c.childID[c.lvl]) & c.hasBranch[c.lvl]) != 0 {
			return true
		}
	}
	return false
}

func (c *IHCursor) _nextSiblingOfParentInMem() bool {
	for c.lvl > 1 {
		if c.k[c.lvl-1] == nil {
			nonNilLvl := c.lvl - 1
			for ; c.k[nonNilLvl] == nil && nonNilLvl > 1; nonNilLvl-- {
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

func (c *IHCursor) _nextSiblingInDB() error {
	ok := dbutils.NextNibblesSubtree(c.k[c.lvl], &c.next)
	if !ok {
		c.k[c.lvl] = nil
		return nil
	}
	c.is++
	if _, err := c._seek(c.next, []byte{}); err != nil {
		return err
	}
	return nil
}

func (c *IHCursor) _unmarshal(k, v []byte) {
	from, to := c.lvl+1, len(k)
	if c.lvl >= len(k) {
		from, to = len(k)+1, c.lvl+2
	}
	for i := from; i < to; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
		c.k[i], c.hasState[i], c.hasBranch[i], c.hasHash[i], c.hashID[i], c.childID[i], c.deleted[i] = nil, 0, 0, 0, 0, 0, false
	}
	c.lvl = len(k)
	c.k[c.lvl] = k
	c.deleted[c.lvl] = false
	c.hasState[c.lvl], c.hasBranch[c.lvl], c.hasHash[c.lvl], c.v[c.lvl], _ = UnmarshalIH(v)
	c.hashID[c.lvl] = -1
	c.childID[c.lvl] = int16(bits.TrailingZeros16(c.hasState[c.lvl]) - 1)
}

func (c *IHCursor) _deleteCurrent() error {
	if c.deleted[c.lvl] {
		return nil
	}
	//if bytes.HasPrefix(c.k[c.lvl], common.FromHex("060e")) {
	//	fmt.Printf("delete: %x\n", c.k[c.lvl])
	//}

	if err := c.hc(c.k[c.lvl], 0, 0, 0, nil, nil); err != nil {
		return err
	}
	c.deleted[c.lvl] = true
	return nil
}

func (c *IHCursor) _hasBranch() bool { return (1<<c.childID[c.lvl])&c.hasBranch[c.lvl] != 0 }
func (c *IHCursor) _hasHash() bool   { return (1<<c.childID[c.lvl])&c.hasHash[c.lvl] != 0 }
func (c *IHCursor) _hash(i int16) []byte {
	return c.v[c.lvl][common.HashLength*i : common.HashLength*(i+1)]
}

func (c *IHCursor) _next() (k, v []byte, hasBranch bool, err error) {
	err = c._preOrderTraversalStep()
	if err != nil {
		return []byte{}, nil, false, err
	}

	for {
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.SkipState = isDenseSequence(c.prev, c.cur)
			return nil, nil, false, nil
		}

		if c._hasHash() {
			c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			//if bytes.HasPrefix(c.kBuf, common.FromHex("060e")) {
			//	fmt.Printf("_next can use: %x ->%t\n", c.kBuf, c.canUse(c.kBuf))
			//}
			if c.canUse(c.kBuf) {
				c.cur = append(c.cur[:0], c.kBuf...)
				c.SkipState = isDenseSequence(c.prev, c.cur)
				return c.cur, c._hash(c.hashID[c.lvl]), c._hasBranch(), nil
			}
		}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, false, err
		}

		err = c._preOrderTraversalStep()
		if err != nil {
			return []byte{}, nil, false, err
		}
	}
}

// IHCursor - holds logic related to iteration over IH bucket
type StorageIHCursor struct {
	is, lvl                      int
	k, v                         [64][]byte
	hasState, hasBranch, hasHash [64]uint16
	deleted                      [64]bool
	childID, hashID              [64]int16

	c         ethdb.Cursor
	shc       StorageHashCollector2
	prev, cur []byte
	seek      []byte
	root      []byte

	next                  []byte
	firstNotCoveredPrefix []byte
	canUse                func(prefix []byte) bool
	skipState             bool

	accWithInc []byte
	kBuf       []byte
	kBuf2      []byte
	quit       <-chan struct{}
}

func IHStorage2(canUse func(prefix []byte) bool, shc StorageHashCollector2, c ethdb.Cursor, quit <-chan struct{}) *StorageIHCursor {
	ih := &StorageIHCursor{c: c, canUse: canUse,
		firstNotCoveredPrefix: make([]byte, 0, 64), next: make([]byte, 64),
		shc:  shc,
		quit: quit,
	}
	return ih
}

func (c *StorageIHCursor) PrevKey() []byte {
	return c.prev
}

func (c *StorageIHCursor) FirstNotCoveredPrefix() []byte {
	c.firstNotCoveredPrefix = firstNotCoveredPrefix(c.prev, []byte{0, 0}, c.firstNotCoveredPrefix)
	return c.firstNotCoveredPrefix
}

func (c *StorageIHCursor) SeekToAccount(accWithInc []byte) (k, v []byte, hasBranch bool, err error) {
	c.accWithInc = accWithInc
	hexutil.DecompressNibbles(c.accWithInc, &c.kBuf)
	c.seek = append(c.seek[:0], c.accWithInc...)
	c.skipState = false
	c.prev = c.cur
	ok, err := c._seek(accWithInc, []byte{})
	if err != nil {
		return []byte{}, nil, false, err
	}
	if !ok {
		return nil, nil, false, nil
	}
	if c.root != nil { // check if acc.storageRoot can be used
		root := c.root
		c.root = nil
		if c.canUse(c.kBuf) { // if rd allow us, return. otherwise delete and go ahead.
			c.cur = c.k[c.lvl]
			c.skipState = true
			return c.cur, root, false, nil
		}
		err = c._preOrderTraversalStepNoInDepth()
		if err != nil {
			return []byte{}, nil, false, err
		}
	}

	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if c.canUse(c.kBuf) {
			c.cur = common.CopyBytes(c.kBuf[80:])
			c.skipState = isDenseSequence(c.prev, c.cur)
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasBranch(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
	}
	return c._next()
}

func (c *StorageIHCursor) Next() (k, v []byte, hasBranch bool, err error) {
	if err = common.Stopped(c.quit); err != nil {
		return []byte{}, nil, false, err
	}

	c.skipState = false
	c.prev = c.cur
	err = c._preOrderTraversalStepNoInDepth()
	if err != nil {
		return []byte{}, nil, false, err
	}
	if c.k[c.lvl] == nil {
		c.cur = nil
		c.skipState = isDenseSequence(c.prev, c.cur)
		return nil, nil, false, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if c.canUse(c.kBuf) {
			c.cur = common.CopyBytes(c.kBuf[80:])
			c.skipState = isDenseSequence(c.prev, c.cur)
			return c.cur, c._hash(c.hashID[c.lvl]), c._hasBranch(), nil
		}
	}
	err = c._deleteCurrent()
	if err != nil {
		return []byte{}, nil, false, err
	}

	return c._next()
}

func (c *StorageIHCursor) _seek(seek, prefix []byte) (bool, error) {
	var k, v []byte
	var err error
	if len(seek) == 40 {
		c.is++
		k, v, err = c.c.Seek(seek)
	} else {
		// optimistic .Next call, can use result in 2 cases:
		// - no child found, means: len(k) <= c.lvl
		// - looking for first child, means: c.childID[c.lvl] <= int16(bits.TrailingZeros16(c.hasBranch[c.lvl]))
		// otherwise do .Seek call
		//k, v, err = c.c.Next()
		//if err != nil {
		//	return false, err
		//}
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("71fe2579f4a5be157546549260f5539cc9445fa20674a8bb637049f43fc1eac20000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//fmt.Printf("_seek2: %x -> %x\n", prefix, k)
		//}
		//if len(k) > c.lvl && c.childID[c.lvl] > int16(bits.TrailingZeros16(c.hasBranch[c.lvl])) {
		c.is++
		k, v, err = c.c.Seek(seek)
		//}
	}
	if err != nil {
		return false, err
	}

	if k == nil || !bytes.HasPrefix(k, c.accWithInc) || !bytes.HasPrefix(k[40:], prefix) {
		return false, nil
	}
	c._unmarshal(k, v)
	if len(c.k[c.lvl]) > 0 { // root record, firstly storing root hash
		c._nextSiblingInMem()
	}
	return true, nil
}

// _preOrderTraversalStep - goToChild || nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *StorageIHCursor) _preOrderTraversalStep() error {
	if c._hasBranch() {
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
func (c *StorageIHCursor) _preOrderTraversalStepNoInDepth() error {
	ok := c._nextSiblingInMem() || c._nextSiblingOfParentInMem()
	if ok {
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("71fe2579f4a5be157546549260f5539cc9445fa20674a8bb637049f43fc1eac20000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("_nextSibling1: %d,%x,%b,%x\n", c.lvl, c.k[c.lvl], c.hasBranch[c.lvl], c.childID[c.lvl])
		//}
		return nil
	}
	err := c._nextSiblingInDB()
	if err != nil {
		return err
	}
	return nil
}

func (c *StorageIHCursor) _hasHash() bool   { return (1<<c.childID[c.lvl])&c.hasHash[c.lvl] != 0 }
func (c *StorageIHCursor) _hasBranch() bool { return (1<<c.childID[c.lvl])&c.hasBranch[c.lvl] != 0 }
func (c *StorageIHCursor) _hash(i int16) []byte {
	return c.v[c.lvl][common.HashLength*i : common.HashLength*(i+1)]
}

func (c *StorageIHCursor) _nextSiblingInMem() bool {
	for c.childID[c.lvl]++; c.childID[c.lvl] < 16; c.childID[c.lvl]++ {
		if ((1 << c.childID[c.lvl]) & c.hasHash[c.lvl]) != 0 {
			c.hashID[c.lvl]++
			return true
		}
		if ((1 << c.childID[c.lvl]) & c.hasBranch[c.lvl]) != 0 {
			return true
		}

		c.skipState = c.skipState && ((1<<c.childID[c.lvl])&c.hasState[c.lvl]) == 0
	}
	return false
}

func (c *StorageIHCursor) _nextSiblingOfParentInMem() bool {
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

func (c *StorageIHCursor) _nextSiblingInDB() error {
	ok := dbutils.NextNibblesSubtree(c.k[c.lvl], &c.next)
	if !ok {
		c.k[c.lvl] = nil
		return nil
	}
	c.seek = append(c.seek[:40], c.next...)
	if _, err := c._seek(c.seek, []byte{}); err != nil {
		return err
	}
	return nil
}

func (c *StorageIHCursor) _next() (k, v []byte, hasBranch bool, err error) {
	err = c._preOrderTraversalStep()
	if err != nil {
		return []byte{}, nil, false, err
	}

	for {
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.skipState = isDenseSequence(c.prev, c.cur)
			return nil, nil, false, nil
		}

		if c._hasHash() {
			c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			if c.canUse(c.kBuf) {
				c.cur = common.CopyBytes(c.kBuf[80:])
				c.skipState = isDenseSequence(c.prev, c.cur)
				return c.cur, c._hash(c.hashID[c.lvl]), c._hasBranch(), nil
			}
		}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, false, err
		}

		err = c._preOrderTraversalStep()
		if err != nil {
			return []byte{}, nil, false, err
		}
	}
}

func (c *StorageIHCursor) _unmarshal(k, v []byte) {
	from, to := c.lvl+1, len(k)
	if c.lvl >= len(k) {
		from, to = len(k)+1, c.lvl+2
	}
	for i := from; i < to; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
		c.k[i], c.hasState[i], c.hasBranch[i], c.hasHash[i], c.hashID[i], c.childID[i], c.deleted[i] = nil, 0, 0, 0, 0, 0, false
	}

	c.lvl = len(k) - 40
	c.k[c.lvl] = k[40:]
	c.deleted[c.lvl] = false
	c.hasState[c.lvl], c.hasBranch[c.lvl], c.hasHash[c.lvl], c.v[c.lvl], c.root = UnmarshalIH(v)
	c.hashID[c.lvl] = -1
	c.childID[c.lvl] = int16(bits.TrailingZeros16(c.hasState[c.lvl]) - 1)
}

func (c *StorageIHCursor) _deleteCurrent() error {
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

// keyIsBefore - kind of bytes.Compare, but nil is the last key. And return
func keyIsBeforeOrEqual(k1, k2 []byte) bool {
	if k1 == nil {
		return false
	}
	if k2 == nil {
		return true
	}
	return bytes.Compare(k1, k2) <= 0
}

func keyIsBeforeOrEqualDeprecated(k1, k2 []byte) (bool, []byte) {
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
	return bytes.Compare(k1, k2) < 0
}

func UnmarshalIHTyped(v []byte) (uint16, uint16, uint16, []common.Hash) {
	hasState, hasBranch, hasHash := binary.BigEndian.Uint16(v), binary.BigEndian.Uint16(v[2:]), binary.BigEndian.Uint16(v[4:])
	v = v[6:]
	newV := make([]common.Hash, len(v)/common.HashLength)
	for i := 0; i < len(newV); i++ {
		newV[i].SetBytes(common.CopyBytes(v[i*common.HashLength : (i+1)*common.HashLength]))
	}
	return hasState, hasBranch, hasHash, newV
}

func UnmarshalIH(v []byte) (hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) {
	hasState, hasBranch, hasHash, hashes = binary.BigEndian.Uint16(v), binary.BigEndian.Uint16(v[2:]), binary.BigEndian.Uint16(v[4:]), v[6:]
	if bits.OnesCount16(hasHash)+1 == len(hashes)/common.HashLength {
		rootHash = hashes[:32]
		hashes = hashes[32:]
	}
	return
}

func MarshalIH(hasState, hasBranch, hasHash uint16, h []common.Hash, buf []byte) []byte {
	buf = buf[:6+len(h)*common.HashLength]
	meta, hashes := buf[:6], buf[6:]
	binary.BigEndian.PutUint16(meta, hasState)
	binary.BigEndian.PutUint16(meta[2:], hasBranch)
	binary.BigEndian.PutUint16(meta[4:], hasHash)
	for i := 0; i < len(h); i++ {
		copy(hashes[i*common.HashLength:(i+1)*common.HashLength], h[i].Bytes())
	}
	return buf
}

func IHStorageKey(addressHash []byte, incarnation uint64, prefix []byte) []byte {
	return dbutils.GenerateCompositeStoragePrefix(addressHash, incarnation, prefix)
}

func IHValue(hasState, hasBranch, hasHash uint16, hashes []byte, rootHash []byte, buf []byte) []byte {
	buf = buf[:len(hashes)+len(rootHash)+6]
	meta, hashesList := buf[:6], buf[6:]
	binary.BigEndian.PutUint16(meta, hasState)
	binary.BigEndian.PutUint16(meta[2:], hasBranch)
	binary.BigEndian.PutUint16(meta[4:], hasHash)
	if len(rootHash) == 0 {
		copy(hashesList, hashes)
	} else {
		copy(hashesList, rootHash)
		copy(hashesList[32:], hashes)
	}
	return buf
}

func IHTypedValue(hashes []byte, rootHash []byte) []common.Hash {
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

func collectMissedAccIH(canUse func(prefix []byte) bool, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) ([][]byte, error) {
	var rangeFrom []byte
	var hasRange bool
	ranges := [][]byte{}
	var addToRange = func(cur []byte) {
		if hasRange {
			return
		}
		rangeFrom = append(rangeFrom[:0], cur...)
		hasRange = true
	}
	var endRange = func(cur []byte) {
		if !hasRange {
			return
		}
		ranges = append(ranges, common.CopyBytes(rangeFrom), common.CopyBytes(cur))
		hasRange = false
	}
	if err := walkIHAccounts(canUse, prefix, cache, func(k []byte, _ common.Hash, isBranch, _, canUse bool) error {
		if !isBranch {
			return nil
		}

		if canUse {
			endRange(k)
			return nil
		}

		inCache := cache.HasAccountHashWithPrefix(k)
		if inCache {
			endRange(k)
		} else {
			addToRange(k)
		}

		return common.Stopped(quit)
	}); err != nil {
		return nil, err
	}
	return ranges, nil
}

func loadAccIHToCache(ih ethdb.Cursor, prefix []byte, ranges [][]byte, cache *shards.StateCache, quit <-chan struct{}) error {
	for j := 0; j < len(ranges)/2; j++ {
		if err := common.Stopped(quit); err != nil {
			return err
		}
		from, to := ranges[j*2], ranges[j*2+1]
		for k, v, err := ih.Seek(from); k != nil; k, v, err = ih.Next() {
			if err != nil {
				return err
			}
			if keyIsBeforeOrEqual(to, k) || !bytes.HasPrefix(k, prefix) { // read all accounts until next IH
				break
			}
			hasState, hasBranch, hasHash, newV := UnmarshalIHTyped(v)
			cache.SetAccountHashesRead(k, hasState, hasBranch, hasHash, newV)
		}
	}
	return nil
}

func loadAccsToCache(accs ethdb.Cursor, ranges [][]byte, cache *shards.StateCache, quit <-chan struct{}) ([][]byte, error) {
	var storagePrefixes [][]byte
	for i := 0; i < len(ranges); i += 2 {
		if err := common.Stopped(quit); err != nil {
			return nil, err
		}
		from, to := ranges[i], ranges[i+1]
		if len(from)%2 == 1 {
			from = append(from, 0)
		}
		if len(to)%2 == 1 {
			to = append(to, 0)
		}
		hexutil.CompressNibbles(from, &from)
		hexutil.CompressNibbles(to, &to)
		for k, v, err := accs.Seek(from); k != nil; k, v, err = accs.Next() {
			if err != nil {
				return nil, err
			}
			if keyIsBefore(to, k) {
				break
			}

			var incarnation uint64
			b, ok := cache.GetAccountByHashedAddress(common.BytesToHash(k))
			if !ok {
				var a accounts.Account
				if err := a.DecodeForStorage(v); err != nil {
					return nil, err
				}
				incarnation = a.Incarnation
				cache.DeprecatedSetAccountRead(common.BytesToHash(k), &a)
			} else {
				incarnation = b.Incarnation
			}

			storagePrefixes = append(storagePrefixes, dbutils.GenerateCompositeStoragePrefix(k, incarnation, []byte{}))
		}
	}
	return storagePrefixes, nil
}

func collectMissedAccounts(canUse func(prefix []byte) bool, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) ([][]byte, error) {
	var rangeFrom []byte
	var hasRange bool
	ranges := [][]byte{}
	var addToRange = func(cur []byte) {
		if hasRange {
			return
		}
		rangeFrom = append(rangeFrom[:0], cur...)
		hasRange = true
	}
	var endRange = func(cur []byte) {
		if !hasRange {
			return
		}
		if isDenseSequence(rangeFrom, cur) {
			return
		}
		ranges = append(ranges, common.CopyBytes(rangeFrom), common.CopyBytes(cur))
		hasRange = false
	}
	if err := walkIHAccounts(canUse, prefix, cache, func(cur []byte, _ common.Hash, isBranch, _, canUse bool) error {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		if !isBranch {
			inCache := cache.HasAccountWithInPrefix(cur)
			if inCache {
				endRange(cur)
			} else {
				addToRange(cur)
			}
			return nil
		}

		if canUse {
			endRange(cur)
			return nil
		}

		addToRange(cur)
		return nil
	}); err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		ranges = append(ranges, []byte{}, nil)
	}
	return ranges, nil
}

func walkIHAccounts(canUse func(prefix []byte) bool, prefix []byte, sc *shards.StateCache, walker func(cur []byte, hash common.Hash, isBranch, isHash, canUse bool) error) error {
	var cur []byte
	seek := make([]byte, 0, 64)
	buf := make([]byte, 0, 64)
	next := make([]byte, 0, 64)
	seek = append(seek, prefix...)
	var k [64][]byte
	var hasBranch, hasState, hasHash [64]uint16
	var id, hashID [64]int16
	var deleted [64]bool
	var hashes [64][]common.Hash
	var lvl int
	var ok bool
	var (
		ihK                                      []byte
		hasStateItem, hasBranchItem, hasHashItem uint16
		hashItem                                 []common.Hash
	)
	var isChild = func() bool { return (1<<id[lvl])&hasState[lvl] != 0 }
	var isBranch = func() bool { return (1<<id[lvl])&hasBranch[lvl] != 0 }
	var isHash = func() bool { return (1<<id[lvl])&hasHash[lvl] != 0 }
	var _unmarshal = func() {
		from, to := lvl+1, len(k)
		if lvl >= len(k) {
			from, to = len(k)+1, lvl+2
		}
		for i := from; i < to; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
			k[i], hasState[i], hasBranch[i], hasHash[i], hashID[i], id[i], hashes[i], deleted[i] = nil, 0, 0, 0, 0, 0, nil, false
		}
		lvl = len(ihK)
		k[lvl], hasState[lvl], hasBranch[lvl], hasHash[lvl], hashes[lvl] = ihK, hasStateItem, hasBranchItem, hasHashItem, hashItem
		hashID[lvl], id[lvl], deleted[lvl] = -1, int16(bits.TrailingZeros16(hasStateItem))-1, false
	}
	var _nextSiblingInMem = func() bool {
		for id[lvl]++; id[lvl] < int16(bits.Len16(hasState[lvl])); id[lvl]++ { // go to sibling
			// TODO: replace isDenseSequence() by next logic
			//c.SkipState = c.SkipState && ((1<<(c.childID[c.lvl]-1))&c.hasState[c.lvl]) == 0 // if prev child has state - then we skipped some state
			if !isChild() {
				continue
			}

			if isHash() {
				hashID[lvl]++
				return true
			}
			if isBranch() {
				return true
			}
		}
		return false
	}
	var _seek = func(seek []byte, withinPrefix []byte) bool {
		ihK, hasStateItem, hasBranchItem, hasHashItem, hashItem = sc.AccountHashesSeek(seek)
		if ihK == nil || !bytes.HasPrefix(ihK, withinPrefix) || !bytes.HasPrefix(ihK, prefix) {
			k[lvl] = nil
			return false
		}
		_unmarshal()
		_nextSiblingInMem()
		return true
	}
	var _nextSiblingOfParentInMem = func() bool {
		for lvl > 1 { // go to parent sibling in mem
			if k[lvl-1] == nil {
				nonNilLvl := lvl - 1
				for ; k[nonNilLvl] == nil && nonNilLvl > 1; nonNilLvl-- {
				}
				next = append(append(next[:0], k[lvl]...), uint8(id[lvl]))
				buf = append(append(buf[:0], k[nonNilLvl]...), uint8(id[nonNilLvl]))
				if _seek(next, buf) {
					return true
				}
				lvl = nonNilLvl + 1
				continue
			}
			lvl--
			// END of _nextSiblingOfParentInMem
			if _nextSiblingInMem() {
				return true
			}
		}
		return false
	}
	var _nextSiblingInDB = func() bool {
		ok = dbutils.NextNibblesSubtree(k[lvl], &seek)
		if !ok {
			k[lvl] = nil
			return false
		}
		_seek(seek, []byte{})
		return k[lvl] != nil
	}
	var _preOrderTraversalStepNoInDepth = func() { _ = _nextSiblingInMem() || _nextSiblingOfParentInMem() || _nextSiblingInDB() }
	var _preOrderTraversalStep = func() {
		if isBranch() {
			cur = append(append(cur[:0], k[lvl]...), uint8(id[lvl]))
			ihK, hasStateItem, hasBranchItem, hasHashItem, hashItem, ok = sc.GetAccountHash(cur)
			if !ok {
				panic(fmt.Errorf("item %x hasBranch bit %x, but it not found in cache", k[lvl], id[lvl]))
			}
			_unmarshal()
			_nextSiblingInMem()
			return
		}
		_preOrderTraversalStepNoInDepth()
	}

	_seek(prefix, []byte{})

	for k[lvl] != nil { // go to sibling in cache
		if prefix != nil && !bytes.HasPrefix(k[lvl], prefix) {
			break
		}

		cur = append(append(cur[:0], k[lvl]...), uint8(id[lvl]))
		if isHash() {
			can := canUse(cur)
			if err := walker(cur, hashes[lvl][hashID[lvl]], isBranch(), true, can); err != nil {
				return err
			}
			if can {
				_preOrderTraversalStepNoInDepth()
				continue
			}
		} else {
			if err := walker(cur, common.Hash{}, isBranch(), false, false); err != nil {
				return err
			}
		}
		_preOrderTraversalStep()
	}

	if err := walker(nil, common.Hash{}, false, false, false); err != nil {
		return err
	}
	return nil
}

func (l *FlatDBTrieLoader) prep(accs, ihAcc ethdb.Cursor, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) error {
	defer func(t time.Time) { fmt.Printf("trie_root.go:338: %s\n", time.Since(t)) }(time.Now())
	canUse := func(prefix []byte) bool { return !l.rd.Retain(prefix) }
	accIHPrefixes, err := collectMissedAccIH(canUse, prefix, cache, quit)
	if err != nil {
		return err
	}
	err = loadAccIHToCache(ihAcc, prefix, accIHPrefixes, cache, quit)
	if err != nil {
		return err
	}
	accPrefixes, err := collectMissedAccounts(canUse, prefix, cache, quit)
	if err != nil {
		return err
	}
	storageIHPrefixes, err := loadAccsToCache(accs, accPrefixes, cache, quit)
	if err != nil {
		return err
	}
	_ = storageIHPrefixes
	return nil
}

func (l *FlatDBTrieLoader) post(storages ethdb.CursorDupSort, ihStorage *StorageIHCursor, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) (common.Hash, error) {
	var prevIHK []byte
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	l.accSeek = make([]byte, 64)
	defer func(t time.Time) { fmt.Printf("trie_root.go:375: %s\n", time.Since(t)) }(time.Now())
	i2, i4 := 0, 0
	canUse := func(prefix []byte) bool { return !l.rd.Retain(prefix) }

	if err := cache.AccountHashesTree(canUse, prefix, func(ihK []byte, ihV common.Hash, hasBranch, skipState bool) error {
		if skipState {
			goto SkipAccounts
		}

		l.accSeek = firstNotCoveredPrefix(prevIHK, prefix, l.accSeek)
		if err := cache.WalkAccounts(l.accSeek, func(addrHash common.Hash, acc *accounts.Account) (bool, error) {
			if err := common.Stopped(quit); err != nil {
				return false, err
			}
			i2++
			hexutil.DecompressNibbles(addrHash.Bytes(), &l.kHex)
			if keyIsBefore(ihK, l.kHex) || !bytes.HasPrefix(l.kHex, prefix) { // read all accounts until next IH
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
			l.accAddrHash.SetBytes(addrHash.Bytes())
			accWithInc := l.accAddrHashWithInc[:]
			for ihKS, ihVS, hasBranchS, err2 := ihStorage.SeekToAccount(accWithInc); ; ihKS, ihVS, hasBranchS, err2 = ihStorage.Next() {
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
					if keyIsBefore(ihKS, l.kHexS) { // read until next IH
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

				if err := l.receiver.Receive(SHashStreamItem, accWithInc, common.CopyBytes(ihKS), nil, nil, ihVS, hasBranchS, 0); err != nil {
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

		if err := l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV[:], hasBranch, 0); err != nil {
			return err
		}
		prevIHK = append(prevIHK[:0], ihK...)
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
	ihAccC, ihStorageC := tx.Cursor(dbutils.TrieOfAccountsBucket), tx.Cursor(dbutils.TrieOfStorageBucket)
	canUse := func(prefix []byte) bool { return !l.rd.Retain(prefix) }
	ihStorage := IHStorage2(canUse, l.shc, ihStorageC, quit)

	ss := tx.CursorDupSort(dbutils.HashedStorageBucket)

	_ = ihStorageC
	_ = stC

	if err := l.prep(accsC, ihAccC, prefix, cache, quit); err != nil {
		return EmptyRoot, err
	}
	if _, err := l.post(ss, ihStorage, prefix, cache, quit); err != nil {
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
	if err := cache.AccountHashesTree(func(_ []byte) bool { return true }, []byte{}, func(ihK []byte, ihV common.Hash, hasBranch, skipState bool) error {
		if len(ihK) == 0 { // Loop termination
			return nil
		}
		if err := l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV[:], hasBranch, 0); err != nil {
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
