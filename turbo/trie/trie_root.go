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
	kHex, kHexS, buf             []byte
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
	trace        bool
	wasIH        bool
	wasIHStorage bool
	root         common.Hash
	hc           HashCollector2
	shc          StorageHashCollector2
	currStorage  bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage  bytes.Buffer
	valueStorage []byte       // Current value to be used as the value tape for the hash builder
	hashAccount  common.Hash  // Current value to be used as the value tape for the hash builder
	hashStorage  common.Hash  // Current value to be used as the value tape for the hash builder
	curr         bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ         bytes.Buffer
	currAccK     []byte
	value        []byte   // Current value to be used as the value tape for the hash builder
	groups       []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
	hasBranch    []uint16
	hasHash      []uint16
	hb           *HashBuilder
	hashData     GenStructStepHashData
	a            accounts.Account
	leafData     GenStructStepLeafData
	accData      GenStructStepAccountData
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
	for ihK, ihV, err := ih.AtPrefix(prefix); ; ihK, ihV, err = ih.Next() { // no loop termination is at he end of loop
		if err != nil {
			return EmptyRoot, err
		}
		if ih.skipState {
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

				if ihStorage.skipState {
					goto SkipStorage
				}

				i4++
				for kS, vS, err3 := ss.SeekBothRange(accWithInc, ihStorage.FirstNotCoveredPrefix()); kS != nil; kS, vS, err3 = ss.NextDup() {
					if err3 != nil {
						return EmptyRoot, err3
					}
					hexutil.DecompressNibbles(vS[:32], &l.buf)
					if keyIsBefore(ihKS, l.buf) { // read until next IH
						break
					}
					hexutil.DecompressNibbles(accWithInc, &l.kHexS)
					l.kHexS = append(l.kHexS[:80], l.buf...)
					if err = l.receiver.Receive(StorageStreamItem, nil, common.CopyBytes(l.kHexS), nil, vS[32:], nil, 0); err != nil {
						return EmptyRoot, err
					}
				}

			SkipStorage:
				if ihKS == nil { // Loop termination
					break
				}

				hexutil.DecompressNibbles(accWithInc, &l.kHexS)
				l.kHexS = append(l.kHexS[:80], ihKS...)
				if err = l.receiver.Receive(SHashStreamItem, nil, common.CopyBytes(l.kHexS), nil, nil, ihVS, 0); err != nil {
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
	cutoff int,
) error {
	if storageKey == nil {
		if bytes.HasPrefix(accountKey, common.FromHex("05070202")) {
			//fmt.Printf("1: %d, %x, %x\n", itemType, accountKey, hash)
		}
	} else {
		hexutil.CompressNibbles(storageKey[:80], &r.currAccK)
		if bytes.HasPrefix(r.currAccK, common.FromHex("5722")) && bytes.HasPrefix(storageKey[80:], common.FromHex("")) {
			//fmt.Printf("%x\n", storageKey)
			//fmt.Printf("1: %d, %x, %x, %x\n", itemType, r.currAccK, storageKey[80:], hash)
		}
	}
	switch itemType {
	case StorageStreamItem:
		r.advanceKeysStorage(storageKey, true /* terminator */)
		if r.currStorage.Len() > 0 {
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
		r.advanceKeysStorage(storageKey, false /* terminator */)
		if r.currStorage.Len() > 0 {
			if err := r.genStructStorage(); err != nil {
				return err
			}
		}
		r.saveValueStorage(true, storageValue, hash)
	case AccountStreamItem:
		r.advanceKeysAccount(accountKey, true /* terminator */)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(2 * (common.HashLength + common.IncarnationLength))
			if r.currStorage.Len() > 0 {
				if err := r.genStructStorage(); err != nil {
					return err
				}
			}
			if r.currStorage.Len() > 0 {
				if len(r.groups) >= 2*common.HashLength {
					r.groups = r.groups[:2*common.HashLength-1]
					r.hasBranch = r.hasBranch[:2*common.HashLength-1]
					r.hasHash = r.hasHash[:2*common.HashLength-1]
				}
				for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
					r.groups = r.groups[:len(r.groups)-1]
					r.hasBranch = r.hasBranch[:len(r.hasBranch)-1]
					r.hasHash = r.hasHash[:len(r.hasHash)-1]
				}
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
		if err := r.saveValueAccount(false, accountValue, hash); err != nil {
			return err
		}
	case AHashStreamItem:
		r.advanceKeysAccount(accountKey, false /* terminator */)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(2 * (common.HashLength + common.IncarnationLength))
			if r.currStorage.Len() > 0 {
				if err := r.genStructStorage(); err != nil {
					return err
				}
			}
			if r.currStorage.Len() > 0 {
				if len(r.groups) >= 2*common.HashLength {
					r.groups = r.groups[:2*common.HashLength-1]
					r.hasBranch = r.hasBranch[:2*common.HashLength-1]
					r.hasHash = r.hasHash[:2*common.HashLength-1]
				}
				for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
					r.groups = r.groups[:len(r.groups)-1]
					r.hasBranch = r.hasBranch[:len(r.hasBranch)-1]
					r.hasHash = r.hasHash[:len(r.hasHash)-1]
				}
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
		if err := r.saveValueAccount(true, accountValue, hash); err != nil {
			return err
		}
	case CutoffStreamItem:
		if r.trace {
			fmt.Printf("storage cuttoff %d\n", cutoff)
		}
		r.cutoffKeysAccount(cutoff)
		if r.curr.Len() > 0 && !r.wasIH {
			r.cutoffKeysStorage(2 * (common.HashLength + common.IncarnationLength))
			if r.currStorage.Len() > 0 {
				if err := r.genStructStorage(); err != nil {
					return err
				}
			}
			if r.currStorage.Len() > 0 {
				if len(r.groups) >= 2*common.HashLength {
					r.groups = r.groups[:2*common.HashLength-1]
					r.hasBranch = r.hasBranch[:2*common.HashLength-1]
					r.hasHash = r.hasHash[:2*common.HashLength-1]
				}
				for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
					r.groups = r.groups[:len(r.groups)-1]
					r.hasBranch = r.hasBranch[:len(r.hasBranch)-1]
					r.hasHash = r.hasHash[:len(r.hasHash)-1]
				}
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
			for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
				r.groups = r.groups[:len(r.groups)-1]
				r.hasBranch = r.hasBranch[:len(r.hasBranch)-1]
				r.hasHash = r.hasHash[:len(r.hasHash)-1]
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
	if r.currStorage.Len() > 0 {
		r.succStorage.Write(r.currStorage.Bytes()[:cutoff-1])
		r.succStorage.WriteByte(r.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
	}
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
	r.groups, r.hasBranch, r.hasHash, err = GenStructStep(r.RetainNothing, r.currStorage.Bytes(), r.succStorage.Bytes(), r.hb, func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		if len(keyHex) >= 80 {
			if r.shc == nil {
				return nil
			}
			hexutil.CompressNibbles(keyHex[:80], &r.currAccK)
			//if bytes.HasPrefix(r.currAccK, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(keyHex[80:], common.FromHex("")) {
			//	fmt.Printf("collect: %x,%x,%016b, del:%t\n", r.currAccK, keyHex[80:], hasBranch, hashes == nil && rootHash == nil)
			//}
			return r.shc(r.currAccK, keyHex[80:], hasState, hasBranch, hasHash, hashes, rootHash)
		}
		if r.hc == nil {
			return nil
		}
		if bytes.HasPrefix(keyHex, common.FromHex("05070202")) {
			fmt.Printf("collect: %x,%016b,%016b, del:%t\n", keyHex, hasHash, hasBranch, hashes == nil)
		}
		return r.hc(keyHex, hasState, hasBranch, hasHash, hashes, rootHash)
	}, data, r.groups, r.hasBranch, r.hasHash, r.trace)
	if err != nil {
		return err
	}
	return nil
}

func (r *RootHashAggregator) saveValueStorage(isIH bool, v, h []byte) {
	// Remember the current value
	r.wasIHStorage = isIH
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
	if r.groups, r.hasBranch, r.hasHash, err = GenStructStep(r.RetainNothing, r.curr.Bytes(), r.succ.Bytes(), r.hb, func(keyHex []byte, hasState, hasBranch, hasHash uint16, hashes, rootHash []byte) error {
		if len(keyHex) >= 80 {
			if r.shc == nil {
				return nil
			}
			hexutil.CompressNibbles(keyHex[:80], &r.currAccK)
			//if bytes.HasPrefix(r.currAccK, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(keyHex[80:], common.FromHex("")) {
			//	fmt.Printf("collect: %x,%x,%016b, del:%t\n", r.currAccK, keyHex[80:], hasBranch, hashes == nil && rootHash == nil)
			//}
			return r.shc(r.currAccK, keyHex[80:], hasState, hasBranch, hasHash, hashes, rootHash)
		}
		if r.hc == nil {
			return nil
		}
		if bytes.HasPrefix(keyHex, common.FromHex("05070202")) {
			fmt.Printf("collect: %x,%016b,%016b, del:%t\n", keyHex, hasBranch, hasHash, hashes == nil)
		}
		return r.hc(keyHex, hasState, hasBranch, hasHash, hashes, rootHash)
	}, data, r.groups, r.hasBranch, r.hasHash, r.trace); err != nil {
		return err
	}
	r.accData.FieldSet = 0
	return nil
}

func (r *RootHashAggregator) saveValueAccount(isIH bool, v *accounts.Account, h []byte) error {
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

type CanUse func([]byte) bool // returns false - if element must be skipped

const IHDupKeyLen = 2 * (common.HashLength + common.IncarnationLength)

// IHCursor - holds logic related to iteration over IH bucket
// has 2 main operations: goToChild and goToSibling
// goToChild can be done only by DB operation (go to longer prefix)
// goToSibling can be done in memory or by DB operation: nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
type IHCursor struct {
	skipState                    bool
	is, lvl                      int
	k, v                         [64][]byte // store up to 64 levels of key/value pairs in nibbles format
	hasState, hasBranch, hasHash [64]uint16 // branch hasState set, and any hasState set
	childID, hashID              [64]int16  // meta info: current child in .hasState[lvl] field, max child id, current hash in .v[lvl]
	deleted                      [64]bool   // helper to avoid multiple deletes of same key

	c                     ethdb.Cursor
	hc                    HashCollector2
	seek, prev, cur, next []byte
	prefix                []byte

	firstNotCoveredPrefix []byte
	canUse                func(prefix []byte) bool

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

func (c *IHCursor) FirstNotCoveredPrefix() []byte {
	c.firstNotCoveredPrefix = firstNotCoveredPrefix(c.prev, c.seek, c.firstNotCoveredPrefix)
	return c.firstNotCoveredPrefix
}

func (c *IHCursor) AtPrefix(prefix []byte) (k, v []byte, err error) {
	c.skipState = false
	c.prev = append(c.prev[:0], c.cur...)
	c.prefix = prefix
	c.seek = prefix
	ok, err := c._seek(prefix)
	if err != nil {
		return []byte{}, nil, err
	}
	if !ok || c.k[c.lvl] == nil {
		c.cur = nil
		c.skipState = isDenseSequence(c.prev, c.cur)
		return nil, nil, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if c.canUse(c.kBuf) {
			c.cur = append(c.cur[:0], c.kBuf...)
			c.skipState = isDenseSequence(c.prev, c.cur)
			return c.cur, c._hash(c.hashID[c.lvl]), nil
		}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}
	}
	return c._next()
}

func (c *IHCursor) Next() (k, v []byte, err error) {
	if err = common.Stopped(c.quit); err != nil {
		return []byte{}, nil, err
	}

	c.skipState = false
	c.prev = append(c.prev[:0], c.cur...)
	err = c._nextSibling()
	if err != nil {
		return []byte{}, nil, err
	}

	if c.k[c.lvl] == nil {
		c.cur = nil
		c.skipState = isDenseSequence(c.prev, c.cur)
		return nil, nil, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if c.canUse(c.kBuf) {
			c.cur = append(c.cur[:0], c.kBuf...)
			c.skipState = isDenseSequence(c.prev, c.cur) || c._complexSkpState()
			return c.cur, c._hash(c.hashID[c.lvl]), nil
		}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}
	}

	return c._next()
}

func (c *IHCursor) _seek(prefix []byte) (bool, error) {
	var k, v []byte
	var err error
	if len(prefix) == 0 {
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
		k, v, err = c.c.Seek(prefix)
		//}
	}
	if err != nil {
		return false, err
	}
	if k == nil || !bytes.HasPrefix(k, prefix) {
		return false, nil
	}
	c._unmarshal(k, v)
	c._nextSiblingInMem()
	return true, nil
}

// goToChild || nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *IHCursor) _nextItem(prefix []byte) error {
	ok, err := c._seek(prefix)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	return c._nextSibling()
}

// nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *IHCursor) _nextSibling() error {
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

func (c *IHCursor) _nextSiblingInMem() bool {
	for c.childID[c.lvl]++; c.childID[c.lvl] < 16; c.childID[c.lvl]++ {
		if ((uint16(1) << c.childID[c.lvl]) & c.hasHash[c.lvl]) != 0 {
			c.hashID[c.lvl]++
			return true
		}
		if ((uint16(1) << c.childID[c.lvl]) & c.hasBranch[c.lvl]) != 0 {
			return true
		}

		c.skipState = c.skipState && ((uint16(1)<<c.childID[c.lvl])&c.hasState[c.lvl]) == 0
	}
	return false
}

func (c *IHCursor) _nextSiblingOfParentInMem() bool {
	for c.lvl > 1 {
		if c.k[c.lvl-1] == nil {
			return false
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
	k, v, err := c.c.Seek(c.next)
	if err != nil {
		return err
	}
	if k == nil || !bytes.HasPrefix(k, c.prefix) {
		c.k[c.lvl] = nil
		return nil
	}
	c._unmarshal(k, v)
	c._nextSiblingInMem()

	return nil
}

func (c *IHCursor) _unmarshal(k, v []byte) {
	if c.lvl < len(k) {
		for i := c.lvl + 1; i < len(k); i++ { // if first meet key is not 0 length, then nullify all shorter metadata
			c.k[i], c.hasBranch[i], c.hasState[i], c.hashID[i], c.childID[i] = nil, 0, 0, 0, 0
		}
	} else {
		for i := len(k) + 1; i <= c.lvl+1; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
			c.k[i], c.hasBranch[i], c.hasState[i], c.hashID[i], c.childID[i] = nil, 0, 0, 0, 0
		}
	}
	c.lvl = len(k)
	c.k[c.lvl] = k
	c.deleted[c.lvl] = false
	c.hasState[c.lvl] = binary.BigEndian.Uint16(v)
	c.hasBranch[c.lvl] = binary.BigEndian.Uint16(v[2:])
	c.hasHash[c.lvl] = binary.BigEndian.Uint16(v[4:])
	c.v[c.lvl] = v[6:]
	c.hashID[c.lvl] = -1
	c.childID[c.lvl] = int16(bits.TrailingZeros16(c.hasState[c.lvl]) - 1)
	if len(c.k[c.lvl]) == 0 { // root record, firstly storing root hash
		c.v[c.lvl] = c.v[c.lvl][32:]
	}
}

func (c *IHCursor) _hash(i int16) []byte {
	return c.v[c.lvl][common.HashLength*i : common.HashLength*(i+1)]
}

func (c *IHCursor) _deleteCurrent() error {
	if c.deleted[c.lvl] {
		return nil
	}
	//if bytes.HasPrefix(c.k[c.lvl], common.FromHex("0c0e")) {
	//	fmt.Printf("delete: %x\n", c.k[c.lvl])
	//}

	if err := c.hc(c.k[c.lvl], 0, 0, 0, nil, nil); err != nil {
		return err
	}
	c.deleted[c.lvl] = true
	return nil
}

func (c *IHCursor) _hasHash() bool {
	return (uint16(1)<<c.childID[c.lvl])&c.hasHash[c.lvl] != 0
}

func (c *IHCursor) _complexSkpState() bool {
	// experimental example of - how can skip state by looking to 'hasState' bitmap
	return false
	//if len(c.prev) == len(c.cur) && bytes.Equal(c.prev[:len(c.prev)-1], c.cur[:len(c.cur)-1]) {
	//	mask := uint16(0)
	//	foundThings := false
	//	for i := int16(c.prev[len(c.prev)-1]) + 1; i < c.childID[c.lvl]; i++ {
	//		c.kBuf[len(c.kBuf)-1] = uint8(i)
	//		if !c.canUse(c.kBuf) {
	//			foundThings = true
	//			break
	//		}
	//		mask |= uint16(1) << i
	//	}
	//	return !foundThings && c.hasState[c.lvl]&mask == 0
	//}
	//return false
}

func (c *IHCursor) _next() (k, v []byte, err error) {
	c.next = append(append(c.next[:0], c.k[c.lvl]...), byte(c.childID[c.lvl]))
	err = c._nextItem(c.next)
	if err != nil {
		return []byte{}, nil, err
	}

	for {
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.skipState = isDenseSequence(c.prev, c.cur)
			return nil, nil, nil
		}

		if c._hasHash() {
			c.kBuf = append(append(c.kBuf[:0], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			if c.canUse(c.kBuf) {
				c.cur = append(c.cur[:0], c.kBuf...)
				c.skipState = isDenseSequence(c.prev, c.cur) || c._complexSkpState()
				return c.cur, c._hash(c.hashID[c.lvl]), nil
			}
			err = c._deleteCurrent()
			if err != nil {
				return []byte{}, nil, err
			}
		}

		c.next = append(append(c.next[:0], c.k[c.lvl]...), byte(c.childID[c.lvl]))
		err = c._nextItem(c.next)
		if err != nil {
			return []byte{}, nil, err
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

func (c *StorageIHCursor) SeekToAccount(prefix []byte) (k, v []byte, err error) {
	c.accWithInc = prefix
	hexutil.DecompressNibbles(c.accWithInc, &c.kBuf)
	c.seek = append(c.seek[:0], c.accWithInc...)
	c.skipState = false
	c.prev = c.cur
	ok, err := c._seek(prefix)
	if err != nil {
		return []byte{}, nil, err
	}
	if !ok || c.k[c.lvl] == nil {
		err = c._nextSiblingInDB()
		if err != nil {
			return []byte{}, nil, err
		}
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.skipState = isDenseSequence(c.prev, c.cur)
			return nil, nil, nil
		}
	}
	if c.root != nil { // check if acc.storageRoot can be used
		root := c.root
		c.root = nil
		if c.canUse(c.kBuf) { // if rd allow us, return. otherwise delete and go ahead.
			c.cur = c.k[c.lvl]
			c.skipState = true
			return c.cur, root, nil
		}
		ok = c._nextSiblingInMem()
		if !ok {
			return nil, nil, nil
		}
	}

	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if c.canUse(c.kBuf) {
			c.cur = common.CopyBytes(c.kBuf[80:])
			c.skipState = isDenseSequence(c.prev, c.cur)
			return c.cur, c._hash(c.hashID[c.lvl]), nil
		}
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("no ih(seek): %x -> %x, %016b, %016b\n", prefix, c.kBuf, c.hasBranch[c.lvl], c.hasState[c.lvl])
		//}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}
	}
	return c._next()
}

func (c *StorageIHCursor) Next() (k, v []byte, err error) {
	if err = common.Stopped(c.quit); err != nil {
		return []byte{}, nil, err
	}

	c.skipState = false
	c.prev = c.cur
	err = c._nextSibling()
	if err != nil {
		return []byte{}, nil, err
	}

	if c.k[c.lvl] == nil {
		c.cur = nil
		c.skipState = isDenseSequence(c.prev, c.cur)
		return nil, nil, nil
	}
	if c._hasHash() {
		c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
		if c.canUse(c.kBuf) {
			//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
			//	fmt.Printf("can use ih(next): %x,%x\n", c.k[c.lvl], c.childID[c.lvl])
			//}
			c.cur = common.CopyBytes(c.kBuf[80:])
			c.skipState = isDenseSequence(c.prev, c.cur) || c._complexSkpState()
			return c.cur, c._hash(c.hashID[c.lvl]), nil
		}
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("no ih(next): %x\n", c.cur)
		//}
		err = c._deleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}
	}

	return c._next()
}

func (c *StorageIHCursor) _seek(prefix []byte) (bool, error) {
	var k, v []byte
	var err error
	if len(prefix) == 40 {
		c.is++
		k, v, err = c.c.Seek(prefix)
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("_seek1: %x -> %x\n", prefix, k)
		//}
	} else {
		// optimistic .Next call, can use result in 2 cases:
		// - no child found, means: len(k) <= c.lvl
		// - looking for first child, means: c.childID[c.lvl] <= int16(bits.TrailingZeros16(c.hasBranch[c.lvl]))
		// otherwise do .Seek call
		//k, v, err = c.c.Next()
		//if err != nil {
		//	return false, err
		//}
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//fmt.Printf("_seek2: %x -> %x\n", prefix, k)
		//}
		//if len(k) > c.lvl && c.childID[c.lvl] > int16(bits.TrailingZeros16(c.hasBranch[c.lvl])) {
		c.is++
		k, v, err = c.c.Seek(prefix)
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("_seek3: %x -> %x\n", prefix, k)
		//}
		//}
	}
	if err != nil {
		return false, err
	}
	if k == nil || !bytes.HasPrefix(k, prefix) {
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("_seek4: %x -> %x\n", prefix, k)
		//}
		return false, nil
	}
	c._unmarshal(k, v)
	if len(c.k[c.lvl]) > 0 { // root record, firstly storing root hash
		c._nextSiblingInMem()
	}
	return true, nil
}

// goToChild || nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *StorageIHCursor) _nextItem(prefix []byte) error {
	ok, err := c._seek(prefix)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	return c._nextSibling()
}

// nextSiblingInMem || nextSiblingOfParentInMem || nextSiblingInDB
func (c *StorageIHCursor) _nextSibling() error {
	ok := c._nextSiblingInMem() || c._nextSiblingOfParentInMem()
	if ok {
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("_nextSibling1: %d,%x,%b,%x\n", c.lvl, c.k[c.lvl], c.hasBranch[c.lvl], c.childID[c.lvl])
		//}
		return nil
	}
	//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
	//	fmt.Printf("_nextSibling2: %d,%x\n", c.lvl, c.k[c.lvl])
	//}
	err := c._nextSiblingInDB()
	if err != nil {
		return err
	}
	return nil
}

func (c *StorageIHCursor) _hasHash() bool {
	return (uint16(1)<<c.childID[c.lvl])&c.hasHash[c.lvl] != 0
}

func (c *StorageIHCursor) _nextSiblingInMem() bool {
	for c.childID[c.lvl]++; c.childID[c.lvl] < 16; c.childID[c.lvl]++ {
		if ((uint16(1) << c.childID[c.lvl]) & c.hasHash[c.lvl]) != 0 {
			c.hashID[c.lvl]++
			return true
		}
		if ((uint16(1) << c.childID[c.lvl]) & c.hasBranch[c.lvl]) != 0 {
			return true
		}

		c.skipState = c.skipState && ((uint16(1)<<c.childID[c.lvl])&c.hasState[c.lvl]) == 0
	}
	return false
}

func (c *StorageIHCursor) _nextSiblingOfParentInMem() bool {
	for c.lvl > 0 {
		if c.k[c.lvl-1] == nil {
			return false
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
		//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
		//	fmt.Printf("_nextSiblingInDB, not ok\n")
		//}
		c.k[c.lvl] = nil
		return nil
	}
	c.is++
	c.seek = append(c.seek[:40], c.next...)
	k, v, err := c.c.Seek(c.seek)
	if err != nil {
		return err
	}
	//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
	//	fmt.Printf("_nextSiblingInDB2: %x -> %x\n", c.seek, k)
	//}
	if k == nil || !bytes.HasPrefix(k, c.accWithInc) {
		c.k[c.lvl] = nil
		return nil
	}
	c._unmarshal(k, v)
	c._nextSiblingInMem()
	return nil
}

func (c *StorageIHCursor) _hash(i int16) []byte {
	return c.v[c.lvl][common.HashLength*i : common.HashLength*(i+1)]
}

func (c *StorageIHCursor) _next() (k, v []byte, err error) {
	c.seek = append(append(c.seek[:40], c.k[c.lvl]...), byte(c.childID[c.lvl]))
	err = c._nextItem(c.seek)
	if err != nil {
		return []byte{}, nil, err
	}

	for {
		if c.k[c.lvl] == nil {
			c.cur = nil
			c.skipState = isDenseSequence(c.prev, c.cur)
			return nil, nil, nil
		}

		if c._hasHash() {
			c.kBuf = append(append(c.kBuf[:80], c.k[c.lvl]...), uint8(c.childID[c.lvl]))
			if c.canUse(c.kBuf) {
				c.cur = common.CopyBytes(c.kBuf[80:])
				c.skipState = isDenseSequence(c.prev, c.cur) || c._complexSkpState()
				return c.cur, c._hash(c.hashID[c.lvl]), nil
			}
			//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
			//	fmt.Printf("no ih(next): %x,%x,%016b\n", c.k[c.lvl], c.childID[c.lvl], c.hasBranch[c.lvl])
			//}
			err = c._deleteCurrent()
			if err != nil {
				return []byte{}, nil, err
			}
		}

		c.seek = append(append(c.seek[:40], c.k[c.lvl]...), byte(c.childID[c.lvl]))
		err = c._nextItem(c.seek)
		if err != nil {
			return []byte{}, nil, err
		}
	}
}

func (c *StorageIHCursor) _unmarshal(k, v []byte) {
	if c.lvl < len(k) {
		for i := c.lvl + 1; i < len(k); i++ { // if first meet key is not 0 length, then nullify all shorter metadata
			c.k[i], c.hasBranch[i], c.hasState[i], c.hashID[i], c.childID[i] = nil, 0, 0, 0, 0
		}
	} else {
		for i := len(k) + 1; i <= c.lvl+1; i++ { // if first meet key is not 0 length, then nullify all shorter metadata
			c.k[i], c.hasBranch[i], c.hasState[i], c.hashID[i], c.childID[i] = nil, 0, 0, 0, 0
		}
	}

	c.lvl = len(k) - 40
	c.k[c.lvl] = k[40:]
	c.deleted[c.lvl] = false
	c.hasState[c.lvl] = binary.BigEndian.Uint16(v)
	c.hasBranch[c.lvl] = binary.BigEndian.Uint16(v[2:])
	c.hasHash[c.lvl] = binary.BigEndian.Uint16(v[4:])
	c.hashID[c.lvl] = -1
	c.childID[c.lvl] = int16(bits.TrailingZeros16(c.hasState[c.lvl]) - 1)
	c.v[c.lvl] = v[6:]
	if len(c.k[c.lvl]) == 0 { // root record, firstly storing root hash
		c.root = c.v[c.lvl][:32]
		c.v[c.lvl] = c.v[c.lvl][32:]
	}
}

func (c *StorageIHCursor) _complexSkpState() bool {
	return false
	//if len(c.prev) == len(c.cur) && bytes.Equal(c.prev[:len(c.prev)-1], c.cur[:len(c.cur)-1]) {
	//	mask := uint16(0)
	//	foundThings := false
	//	for i := int16(c.prev[len(c.prev)-1]) + 1; i < c.childID[c.lvl]; i++ {
	//		c.kBuf[len(c.kBuf)-1] = uint8(i)
	//		if !c.canUse(c.kBuf) {
	//			foundThings = true
	//			break
	//		}
	//		mask |= uint16(1) << i
	//	}
	//	return !foundThings && c.hasState[c.lvl]&mask == 0
	//}
	//return false
}

func (c *StorageIHCursor) _deleteCurrent() error {
	if c.deleted[c.lvl] {
		return nil
	}
	//if bytes.HasPrefix(c.accWithInc, common.FromHex("35b50e7621258059586f717ca0f0578f166f83c83115e9d79688035f46668da10000000000000001")) && bytes.HasPrefix(c.k[c.lvl], common.FromHex("")) {
	//	fmt.Printf("delete: %x,%x\n", c.accWithInc, c.k[c.lvl])
	//}

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

func UnmarshalIH(v []byte) (uint16, uint16, uint16, []common.Hash) {
	hasState, hasBranch, hasHash := binary.BigEndian.Uint16(v), binary.BigEndian.Uint16(v[2:]), binary.BigEndian.Uint16(v[4:])
	v = v[6:]
	newV := make([]common.Hash, len(v)/common.HashLength)
	for i := 0; i < len(newV); i++ {
		newV[i].SetBytes(v[i*common.HashLength : (i+1)*common.HashLength])
	}
	return hasState, hasBranch, hasHash, newV
}

func MarshalIH(hasState, hasBranch, hasHash uint16, h []common.Hash) []byte {
	v := make([]byte, 6+len(h)*common.HashLength)
	binary.BigEndian.PutUint16(v, hasState)
	binary.BigEndian.PutUint16(v[2:], hasBranch)
	binary.BigEndian.PutUint16(v[4:], hasHash)
	for i := 0; i < len(h); i++ {
		copy(v[6+i*common.HashLength:6+(i+1)*common.HashLength], h[i].Bytes())
	}
	return v
}

func IHStorageKey(addressHash []byte, incarnation uint64, prefix []byte) []byte {
	return dbutils.GenerateCompositeStoragePrefix(addressHash, incarnation, prefix)
}

func IHValue(hasState, hasBranch, hasHash uint16, hashes []byte, rootHash []byte, buf []byte) []byte {
	buf = buf[:len(hashes)+len(rootHash)+6]
	binary.BigEndian.PutUint16(buf, hasState)
	binary.BigEndian.PutUint16(buf[2:], hasBranch)
	binary.BigEndian.PutUint16(buf[4:], hasHash)
	if len(rootHash) == 0 {
		copy(buf[6:], hashes)
	} else {
		copy(buf[6:], rootHash)
		copy(buf[38:], hashes)
	}
	return buf
}

func IHTypedValue(hashes []byte, rootHash []byte) []common.Hash {
	to := make([]common.Hash, len(hashes)/common.HashLength+len(rootHash)/common.HashLength)
	i := 0
	if len(rootHash) > 0 {
		to[0].SetBytes(rootHash)
		i++
	}
	for j := 0; j < len(hashes)/common.HashLength; j++ {
		to[i].SetBytes(hashes[j*common.HashLength : (j+1)*common.HashLength])
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
	if err := walkIHAccounts(canUse, prefix, cache, func(isBranch, canUse bool, cur []byte, hash common.Hash) error {
		if err := common.Stopped(quit); err != nil {
			return err
		}

		if !isBranch {
			return nil
		}

		if canUse {
			endRange(cur)
			return nil
		}

		inCache := cache.HasAccountHashWithPrefix(cur)
		if inCache {
			endRange(cur)
		} else {
			addToRange(cur)
		}
		return nil
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
			hasState, hasBranch, hasHash, newV := UnmarshalIH(v)
			cache.SetAccountHashesRead(k, hasState, hasBranch, hasHash, newV)
		}
	}
	return nil
}

func loadAccsToCache(accs ethdb.Cursor, ranges [][]byte, cache *shards.StateCache, quit <-chan struct{}) ([][]byte, error) {
	var storageIHRanges [][]byte
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

			var a accounts.Account
			if err := a.DecodeForStorage(v); err != nil {
				return nil, err
			}
			if _, ok := cache.GetAccountByHashedAddress(common.BytesToHash(k)); !ok {
				cache.DeprecatedSetAccountRead(common.BytesToHash(k), &a)
			}

			accWithInc := make([]byte, 40)
			copy(accWithInc, k)
			binary.BigEndian.PutUint64(accWithInc[32:], a.Incarnation)
			storageIHRanges = append(storageIHRanges, accWithInc)
		}
	}
	return storageIHRanges, nil
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
	if err := walkIHAccounts(canUse, prefix, cache, func(isBranch, canUse bool, cur []byte, hash common.Hash) error {
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

	return ranges, nil
}

func walkIHAccounts(canUse func(prefix []byte) bool, prefix []byte, cache *shards.StateCache, walker func(isBranch, canUse bool, cur []byte, hash common.Hash) error) error {
	var cur []byte
	seek := make([]byte, 0, 64)
	seek = append(seek, prefix...)
	var k [64][]byte
	var hasState, hasBranch, hasHash [64]uint16
	var id, maxID, hashID [64]int8
	var hashes [64][]common.Hash
	var lvl int
	var ok bool
	var isChild = func() bool { return (uint16(1)<<id[lvl])&hasState[lvl] != 0 }
	var isBranch = func() bool { return (uint16(1)<<id[lvl])&hasBranch[lvl] != 0 }

	ihK, hasStateItem, hasBranchItem, hasHashItem, hashItem := cache.AccountHashesSeek(prefix)
GotItemFromCache:
	for ihK != nil { // go to sibling in cache
		lvl = len(ihK)
		k[lvl], hasState[lvl], hasBranch[lvl], hasHash[lvl], hashes[lvl] = ihK, hasStateItem, hasBranchItem, hasHashItem, hashItem
		hashID[lvl], id[lvl], maxID[lvl] = -1, int8(bits.TrailingZeros16(hasBranchItem))-1, int8(bits.Len16(hasBranchItem))

		if prefix != nil && !bytes.HasPrefix(k[lvl], prefix) {
			return nil
		}

		for ; lvl > 0; lvl-- { // go to parent sibling in mem
			cur = append(append(cur[:0], k[lvl]...), 0)
			for id[lvl]++; id[lvl] <= maxID[lvl]; id[lvl]++ { // go to sibling
				if !isChild() {
					continue
				}

				cur[len(cur)-1] = uint8(id[lvl])
				if !isBranch() {
					if err := walker(false, false, cur, common.Hash{}); err != nil {
						return err
					}
					continue
				}
				hashID[lvl]++
				if canUse(cur) {
					if err := walker(true, true, cur, hashes[lvl][hashID[lvl]]); err != nil {
						return err
					}
					continue // cache item can be used and exists in cache, then just go to next sibling
				}

				if err := walker(true, false, cur, hashes[lvl][hashID[lvl]]); err != nil {
					return err
				}

				ihK, hasStateItem, hasBranchItem, hasHashItem, _, ok = cache.GetAccountHash(cur)
				if ok {
					continue GotItemFromCache
				}
			}
		}

		ok = dbutils.NextNibblesSubtree(k[1], &seek)
		if !ok {
			break
		}
		ihK, hasStateItem, hasBranchItem, hasHashItem, hashItem = cache.AccountHashesSeek(seek)
		//fmt.Printf("sibling: %x -> %x, %d, %d, %d\n", seek, ihK, lvl, id[lvl], maxID[lvl])
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

func (l *FlatDBTrieLoader) post(storages ethdb.CursorDupSort, prefix []byte, cache *shards.StateCache, quit <-chan struct{}) (common.Hash, error) {
	var prevIHK []byte
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	l.accSeek = make([]byte, 64)
	defer func(t time.Time) { fmt.Printf("trie_root.go:375: %s\n", time.Since(t)) }(time.Now())
	canUse := func(prefix []byte) bool { return !l.rd.Retain(prefix) }
	i1, i2, i3, i4 := 0, 0, 0, 0

	if err := cache.AccountHashesTree(canUse, prefix, func(ihK []byte, ihV common.Hash, skipState bool) error {
		i1++
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
			accWithInc := l.accAddrHashWithInc[:]
			hexutil.DecompressNibbles(accWithInc, &l.kHexS)
			if err := cache.StorageHashes(l.accAddrHash, l.accountValue.Incarnation, func(ihKS []byte, h common.Hash) error {
				i3++
				if isDenseSequence(prevIHKS, ihKS) {
					goto SkipStorage
				}

				l.storageSeek = firstNotCoveredPrefix(prevIHK, prefix, l.storageSeek)
				for kS, vS, err3 := storages.SeekBothRange(accWithInc, l.storageSeek); kS != nil; kS, vS, err3 = storages.NextDup() {
					if err3 != nil {
						return err3
					}
					i4++
					hexutil.DecompressNibbles(vS[:32], &l.buf)
					if keyIsBefore(ihKS, l.buf) { // read until next IH
						break
					}
					l.kHexS = append(l.kHexS[80:], l.buf...)
					if err := l.receiver.Receive(StorageStreamItem, accWithInc, l.kHexS, nil, vS[32:], nil, 0); err != nil {
						return err
					}
				}

			SkipStorage:
				if len(ihKS) == 0 || !bytes.HasPrefix(ihKS, l.ihSeek) { // Loop termination
					return nil
				}

				l.kHexS = append(l.kHexS[80:], ihKS...)
				if err := l.receiver.Receive(SHashStreamItem, accWithInc, l.kHexS, nil, nil, h.Bytes(), 0); err != nil {
					return err
				}
				if len(ihKS) == 0 { // means we just sent acc.storageRoot
					return nil
				}
				prevIHKS = append(prevIHKS[:0], ihKS...)
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
		prevIHK = append(prevIHK[:0], ihK...)
		return nil
	}); err != nil {
		return EmptyRoot, err
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, len(prefix)); err != nil {
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
	ss := tx.CursorDupSort(dbutils.HashedStorageBucket)

	_ = ihStorageC
	_ = stC

	if err := l.prep(accsC, ihAccC, prefix, cache, quit); err != nil {
		return EmptyRoot, err
	}
	if _, err := l.post(ss, prefix, cache, quit); err != nil {
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
	if err := cache.AccountHashesTree(func(_ []byte) bool { return true }, []byte{}, func(ihK []byte, ihV common.Hash, skipState bool) error {
		if len(ihK) == 0 { // Loop termination
			return nil
		}
		//fmt.Printf("1:%x\n", ihK)
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
