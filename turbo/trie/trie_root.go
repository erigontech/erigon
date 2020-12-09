package trie

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
)

/*
**Theoretically:** "Merkle trie root calculation" starts from state, build from state keys - trie,
on each level of trie calculates intermediate hash of underlying data.

**Practically:** It can be implemented as "Preorder trie traversal" (Preorder - visit Root, visit Left, visit Right).
But, let's make couple observations to make traversal over huge state efficient.

**Observation 1:** `CurrentStateBucket` already stores state keys in sorted way.
Iteration over this bucket will retrieve keys in same order as "Preorder trie traversal".

**Observation 2:** each Eth block - changes not big part of state - it means most of Merkle trie intermediate hashes will not change.
It means we effectively can cache them. `IntermediateTrieHashBucket` stores "Intermediate hashes of all Merkle trie levels".
It also sorted and Iteration over `IntermediateTrieHashBucket` will retrieve keys in same order as "Preorder trie traversal".

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
	logPrefix                string
	trace                    bool
	stateBucket              string
	intermediateHashesBucket string
	rd                       RetainDecider
	accAddrHashWithInc       [40]byte // Concatenation of addrHash of the currently build account with its incarnation encoding

	ihSeek, accSeek, storageSeek []byte

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
	trace        bool
	wasIH        bool
	wasIHStorage bool
	root         common.Hash
	hc           HashCollector
	currStorage  bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage  bytes.Buffer
	valueStorage []byte       // Current value to be used as the value tape for the hash builder
	hashAccount  common.Hash  // Current value to be used as the value tape for the hash builder
	hashStorage  common.Hash  // Current value to be used as the value tape for the hash builder
	curr         bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ         bytes.Buffer
	value        []byte   // Current value to be used as the value tape for the hash builder
	groups       []uint16 // `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack. See `GenStructStep` docs
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

func NewFlatDBTrieLoader(logPrefix, stateBucket, intermediateHashesBucket string) *FlatDBTrieLoader {
	return &FlatDBTrieLoader{
		logPrefix:                logPrefix,
		defaultReceiver:          NewRootHashAggregator(),
		stateBucket:              stateBucket,
		intermediateHashesBucket: intermediateHashesBucket,
	}
}

// Reset prepares the loader for reuse
func (l *FlatDBTrieLoader) Reset(rd RetainDecider, hc HashCollector, trace bool) error {
	l.defaultReceiver.Reset(hc, trace)
	l.hc = hc
	l.receiver = l.defaultReceiver
	l.trace = trace
	l.ihSeek, l.accSeek, l.storageSeek = nil, nil, nil
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
//		if isSequence {
//			receive(IH)
//			continue
//		}
//
//		for iterateAccounts from prevIH to currentIH {
//			receive(account)
//			for iterateIHOfStorage within accountWithIncarnation{
//				if isSequence {
//					receive(ihStorage)
//					continue
//				}
//
//				for iterateStorage from prevIHOfStorage to currentIHOfStorage {
//					receive(storage)
//				}
//				receive(ihStorage)
//			}
//		}
//		receive(IH)
//	}
func (l *FlatDBTrieLoader) CalcTrieRoot(db ethdb.Database, quit <-chan struct{}) (common.Hash, error) {
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
	var filter = func(k []byte) bool {
		return !l.rd.Retain(k)
	}
	ih := IH(filter, ihAccC, l.hc)
	ihStorage := IH(filter, ihStorageC, l.hc)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for ihK, ihV, isIHSequence, err := ih.First(); ; ihK, ihV, isIHSequence, err = ih.Next() { // no loop termination is at he end of loop
		if err != nil {
			return EmptyRoot, err
		}
		if isIHSequence {
			if ihK == nil {
				break
			}
			if err = l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV, 0); err != nil {
				return EmptyRoot, err
			}
			continue
		}

		nextHex, _ := dbutils.NextSubtreeHex(ih.PrevKey())
		if len(nextHex)%2 == 1 {
			nextHex = append(nextHex, 0)
		}
		CompressNibbles(nextHex, &l.accSeek)
		if len(ih.PrevKey()) > 0 && len(l.accSeek) == 0 {
			break
		}

		for k, kHex, v, err1 := accs.Seek(l.accSeek); k != nil; k, kHex, v, err1 = accs.Next() {
			if err1 != nil {
				return EmptyRoot, err1
			}

			if err = common.Stopped(quit); err != nil {
				return EmptyRoot, err
			}

			if keyIsBefore(ihK, kHex) { // read all accounts until next IH
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
			DecompressNibbles(l.accAddrHashWithInc[:], &l.ihSeek)

			for ihKS, ihVS, isSeqS, err2 := ihStorage.SeekToAccount(l.ihSeek); ; ihKS, ihVS, isSeqS, err2 = ihStorage.Next() {
				if err2 != nil {
					return EmptyRoot, err2
				}

				if isSeqS {
					if ihKS == nil {
						break
					}
					if err = l.receiver.Receive(SHashStreamItem, nil, ihKS, nil, nil, ihVS, 0); err != nil {
						return EmptyRoot, err
					}
					continue
				}

				if len(ihStorage.PrevKey()) == 0 {
					l.storageSeek = append(l.storageSeek[:0], l.accAddrHashWithInc[:]...)
				} else {
					nextHex2, _ := dbutils.NextSubtreeHex(ihStorage.PrevKey())
					if len(nextHex2)%2 == 1 {
						nextHex2 = append(nextHex2, 0)
					}
					CompressNibbles(nextHex2, &l.storageSeek)
				}

				if len(ihStorage.PrevKey()) > 0 && len(l.storageSeek) == 0 {
					break
				}
				for kS, kHexS, vS, err3 := storages.Seek(l.storageSeek); kS != nil; kS, kHexS, vS, err3 = storages.Next() {
					if err3 != nil {
						return EmptyRoot, err3
					}

					if !bytes.HasPrefix(kS, l.accAddrHashWithInc[:]) || keyIsBefore(ihKS, kHexS) { // read all accounts until next IH
						break
					}

					if err = l.receiver.Receive(StorageStreamItem, nil, kHexS, nil, vS, nil, 0); err != nil {
						return EmptyRoot, err
					}
				}

				if len(ihKS) == 0 || !bytes.HasPrefix(ihKS, l.ihSeek) { // Loop termination
					break
				}

				if err = l.receiver.Receive(SHashStreamItem, nil, ihKS, nil, nil, ihVS, 0); err != nil {
					return EmptyRoot, err
				}
			}

			select {
			default:
			case <-logEvery.C:
				l.logProgress(k, ihK)
			}
		}

		if len(ihK) == 0 { // Loop termination
			break
		}

		if err = l.receiver.Receive(AHashStreamItem, ihK, nil, nil, nil, ihV, 0); err != nil {
			return EmptyRoot, err
		}
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, 0); err != nil {
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

func (r *RootHashAggregator) Reset(hc HashCollector, trace bool) {
	r.hc = hc
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
	//fmt.Printf("1: %d, %x, %x, %x, %x\n", itemType, accountKey, storageKey, hash, storageValue)
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
				}
				for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
					r.groups = r.groups[:len(r.groups)-1]
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
				}
				for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
					r.groups = r.groups[:len(r.groups)-1]
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
				}
				for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
					r.groups = r.groups[:len(r.groups)-1]
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
			}
			for len(r.groups) > 0 && r.groups[len(r.groups)-1] == 0 {
				r.groups = r.groups[:len(r.groups)-1]
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
	r.groups, err = GenStructStep(r.RetainNothing, r.currStorage.Bytes(), r.succStorage.Bytes(), r.hb, r.hc, data, r.groups, r.trace)
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
		r.hashStorage = common.BytesToHash(h)
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
	r.wasIH = isIH
	if isIH {
		r.hashAccount = common.BytesToHash(h)
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
	c      ethdb.Cursor
	hc     HashCollector
	filter Filter
	i      int
	prev   []byte
	cur    []byte
	next   []byte
}

func IH(f Filter, c ethdb.Cursor, hc HashCollector) *IHCursor {
	ih := &IHCursor{c: c, filter: f, i: 1, next: make([]byte, 256), hc: hc}
	return ih
}

func (c *IHCursor) PrevKey() []byte {
	return c.prev
}

func (c *IHCursor) First() (k, v []byte, isSeq bool, err error) {
	k, v, err = c._first()
	if err != nil {
		return []byte{}, nil, false, err
	}
	c.prev = []byte{}

	if k == nil {
		return nil, nil, false, nil
	}
	c.cur = common.CopyBytes(k)
	return c.cur, v, isSequence([]byte{0}, c.cur), nil
}

func (c *IHCursor) SeekToAccount(seek []byte) (k, v []byte, isSeq bool, err error) {
	k, v, err = c._seek(seek)
	if err != nil {
		return []byte{}, nil, false, err
	}
	c.prev = common.CopyBytes(seek)
	c.prev[len(c.prev)-1]--

	if k == nil {
		return nil, nil, false, nil
	}
	c.cur = common.CopyBytes(k)
	return c.cur, v, isSequence(c.prev, c.cur), nil
}

func (c *IHCursor) Next() (k, v []byte, isSeq bool, err error) {
	ok := dbutils.NextSubtreeHex2(c.cur, &c.next)
	if !ok {
		c.prev = c.cur
		c.cur = nil
		return nil, nil, true, nil
	}
	k, v, err = c._seek(c.next)
	if err != nil {
		return []byte{}, nil, false, err
	}
	c.prev = c.cur
	if k == nil {
		c.cur = nil
		return nil, nil, isSequence(c.prev, c.cur), nil
	}
	c.cur = common.CopyBytes(k)
	return c.cur, v, isSequence(c.prev, c.cur), nil
}

func (c *IHCursor) _first() (k, v []byte, err error) {
	k, v, err = c.c.First()
	if err != nil {
		return []byte{}, nil, err
	}

	if k == nil {
		return nil, nil, nil
	}

	if c.filter(k) { // if filter allow us, return. otherwise delete and go ahead.
		return k, v, nil
	}

	err = c.hc(k, nil)
	if err != nil {
		return []byte{}, nil, err
	}

	return c._next()
}

func (c *IHCursor) _seek(seek []byte) (k, v []byte, err error) {
	k, v, err = c.c.Seek(seek)
	if err != nil {
		return []byte{}, nil, err
	}
	if k == nil {
		return nil, nil, nil
	}

	if c.filter(k) { // if filter allow us, return. otherwise delete and go ahead.
		return k, v, nil
	}

	err = c.hc(k, nil)
	if err != nil {
		return []byte{}, nil, err
	}

	return c._next()
}

func (c *IHCursor) _next() (k, v []byte, err error) {
	k, v, err = c.c.Next()
	if err != nil {
		return []byte{}, nil, err
	}

	for {
		if k == nil {
			return nil, nil, nil
		}

		if c.filter(k) { // if filter allow us, return. otherwise delete and go ahead.
			return k, v, nil
		}

		err = c.hc(k, nil)
		if err != nil {
			return []byte{}, nil, err
		}

		k, v, err = c.c.Next()
		if err != nil {
			return []byte{}, nil, err
		}
	}
}

/*
	Sequence - if between 2 IH records not possible insert any state record - then they form "sequence"
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
func isSequence(prev []byte, next []byte) bool {
	isSequence := false
	prev, ok := dbutils.NextSubtreeHex(prev)
	if !ok {
		return true
	}
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

	DecompressNibbles(k, &c.kHex)
	return k, c.kHex, v, nil
}

func (c *StateCursor) Next() ([]byte, []byte, []byte, error) {
	k, v, err := c.c.Next()
	if err != nil {
		return []byte{}, nil, nil, err
	}

	DecompressNibbles(k, &c.kHex)
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
