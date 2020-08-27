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
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
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
 `{account_key}{incarnation}{storage_hash}`. And [trie/trie_root.go](../../trie/trie_root.go) has logic - every time
 when Account visited - we save it to `accAddrHashWithInc` variable and skip any Storage or IntermediateTrieHashes with another incarnation.
*/

// PreOrderTraverse reads state and intermediate trie hashes in order equal to "Preorder trie traversal"
// (Preorder - visit Root, visit Left, visit Right)
//
// It produces stream of values and send this stream to `defaultReceiver`
// It skips storage with incorrect incarnations
//
// Each intermediate hash key firstly pass to RetainDecider, only if it returns "false" - such IH can be used.
type PreOrderTraverse struct {
	stateBucket, intermediateHashesBucket string
	trace                                 bool
	rd                                    RetainDecider
	accAddrHashWithInc                    [40]byte // Concatenation of addrHash of the currently build account with its incarnation encoding
	tx                                    ethdb.Tx
	kv                                    ethdb.KV
	nextAccountKey                        [32]byte
	k, v                                  []byte
	ihK, ihV                              []byte

	itemPresent bool
	itemType    StreamItem

	// Storage item buffer
	storageKey   []byte
	storageValue []byte

	// Account item buffer
	accountKey   []byte
	accountValue accounts.Account
	hashValue    []byte
	streamCutoff int

	receiver        StreamReceiver
	defaultReceiver *RootCalculator
	hc              HashCollector
}

// RootCalculator - calculates Merkle trie root hash from incoming data stream
// it uses indivitual `RetainDecider`
type RootCalculator struct {
	trace        bool
	hc           HashCollector
	root         common.Hash
	currStorage  bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage  bytes.Buffer
	valueStorage bytes.Buffer // Current value to be used as the value tape for the hash builder
	curr         bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ         bytes.Buffer
	value        bytes.Buffer // Current value to be used as the value tape for the hash builder
	groups       []uint16
	hb           *HashBuilder
	wasIH        bool
	wasIHStorage bool
	hashData     GenStructStepHashData
	a            accounts.Account
	leafData     GenStructStepLeafData
	accData      GenStructStepAccountData
}

func NewRootCalculator() *RootCalculator {
	return &RootCalculator{
		hb: NewHashBuilder(false),
	}
}

func NewPreOrderTraverse(stateBucket, intermediateHashesBucket string) *PreOrderTraverse {
	return &PreOrderTraverse{
		defaultReceiver:          NewRootCalculator(),
		stateBucket:              stateBucket,
		intermediateHashesBucket: intermediateHashesBucket,
	}
}

// Reset prepares the loader for reuse
func (t *PreOrderTraverse) Reset(db ethdb.Database, rl RetainDecider, hc HashCollector, trace bool) error {
	t.defaultReceiver.Reset(hc, trace)
	t.hc = hc
	t.receiver = t.defaultReceiver

	t.trace = trace
	t.rd = rl
	t.itemPresent = false
	if t.trace {
		fmt.Printf("----------\n")
		fmt.Printf("RebuildTrie\n")
	}
	if t.trace {
		fmt.Printf("t.rd: %s\n", t.rd)
	}
	if hasTx, ok := db.(ethdb.HasTx); ok {
		t.tx = hasTx.Tx()
	} else {
		if hasKV, ok := db.(ethdb.HasKV); ok {
			t.kv = hasKV.KV()
		} else {
			return fmt.Errorf("database doest not implement KV: %T", db)
		}
	}

	return nil
}

func (t *PreOrderTraverse) SetStreamReceiver(receiver StreamReceiver) {
	t.receiver = receiver
}

// iteration moves through the database buckets and creates at most
// one stream item, which is indicated by setting the field fstl.itemPresent to true
func (t *PreOrderTraverse) iteration(c ethdb.Cursor, ih *IHCursor, first bool) error {
	var isIH, isIHSequence bool
	var err error
	if first {
		if t.ihK, t.ihV, isIHSequence, err = ih.Seek([]byte{}); err != nil {
			return err
		}
		if isIHSequence {
			t.k = common.CopyBytes(t.ihK)
			return nil
		}
		if t.k, t.v, err = c.Seek([]byte{}); err != nil {
			return err
		}

		// Skip wrong incarnation
		if len(t.k) > common.HashLength {
			if nextAccount(t.k, t.nextAccountKey[:]) {
				if t.k, t.v, err = c.Seek(t.nextAccountKey[:]); err != nil {
					return err
				}
			} else {
				t.k = nil
			}
		}
		return nil
	}

	isIH, _ = keyIsBeforeOrEqual(t.ihK, t.k)
	if t.ihK == nil && t.k == nil {
		t.itemPresent = true
		t.itemType = CutoffStreamItem
		t.streamCutoff = 0
		t.accountKey = nil
		t.storageKey = nil
		t.storageValue = nil
		t.hashValue = nil
		return nil
	}

	if !isIH {
		// skip wrong incarnation
		if len(t.k) > common.HashLength && !bytes.HasPrefix(t.k, t.accAddrHashWithInc[:]) {
			if bytes.Compare(t.k, t.accAddrHashWithInc[:]) < 0 {
				// Skip all the irrelevant storage in the middle
				if t.k, t.v, err = c.Seek(t.accAddrHashWithInc[:]); err != nil {
					return err
				}
			} else {
				if nextAccount(t.k, t.nextAccountKey[:]) {
					if t.k, t.v, err = c.Seek(t.nextAccountKey[:]); err != nil {
						return err
					}
				} else {
					t.k = nil
				}
			}
			return nil
		}
		t.itemPresent = true
		if len(t.k) > common.HashLength {
			t.itemType = StorageStreamItem
			t.accountKey = nil
			t.storageKey = append(t.storageKey[:0], t.k...)
			t.hashValue = nil
			t.storageValue = append(t.storageValue[:0], t.v...)
			if t.k, t.v, err = c.Next(); err != nil {
				return err
			}
			if t.trace {
				fmt.Printf("k after storageWalker and Next: %x\n", t.k)
			}
		} else if len(t.k) > 0 {
			t.itemType = AccountStreamItem
			t.accountKey = append(t.accountKey[:0], t.k...)
			t.storageKey = nil
			t.storageValue = nil
			t.hashValue = nil
			if err := t.accountValue.DecodeForStorage(t.v); err != nil {
				return fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			copy(t.accAddrHashWithInc[:], t.k)
			binary.BigEndian.PutUint64(t.accAddrHashWithInc[32:], t.accountValue.Incarnation)

			// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
			// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
			// skips over all storage items
			if t.k, t.v, err = c.Seek(t.accAddrHashWithInc[:]); err != nil {
				return err
			}
			if t.trace {
				fmt.Printf("k after accountWalker and Seek: %x\n", t.k)
			}
			if keyIsBefore(t.ihK, t.accAddrHashWithInc[:]) {
				if t.ihK, t.ihV, _, err = ih.Seek(t.accAddrHashWithInc[:]); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// ih part
	if t.trace {
		fmt.Printf("t.ihK %x, t.accAddrHashWithInc %x\n", t.ihK, t.accAddrHashWithInc[:])
	}

	// Skip IH with wrong incarnation
	if len(t.ihK) > common.HashLength && !bytes.HasPrefix(t.ihK, t.accAddrHashWithInc[:]) {
		if bytes.Compare(t.ihK, t.accAddrHashWithInc[:]) < 0 {
			// Skip all the irrelevant storage in the middle
			if t.ihK, t.ihV, _, err = ih.Seek(t.accAddrHashWithInc[:]); err != nil {
				return err
			}
		} else {
			if nextAccount(t.ihK, t.nextAccountKey[:]) {
				if t.ihK, t.ihV, _, err = ih.Seek(t.nextAccountKey[:]); err != nil {
					return err
				}
			} else {
				t.ihK = nil
			}
		}
		return nil
	}
	t.itemPresent = true
	if len(t.ihK) > common.HashLength {
		t.itemType = SHashStreamItem
		t.accountKey = nil
		t.storageKey = append(t.storageKey[:0], t.ihK...)
		t.hashValue = append(t.hashValue[:0], t.ihV...)
		t.storageValue = nil
	} else {
		t.itemType = AHashStreamItem
		t.accountKey = append(t.accountKey[:0], t.ihK...)
		t.storageKey = nil
		t.storageValue = nil
		t.hashValue = append(t.hashValue[:0], t.ihV...)
	}

	// skip subtree
	next, ok := dbutils.NextSubtree(t.ihK)
	if !ok { // no siblings left
		t.k, t.ihK, t.ihV = nil, nil, nil
		return nil
	}
	if t.trace {
		fmt.Printf("next: %x\n", next)
	}

	if t.ihK, t.ihV, isIHSequence, err = ih.Seek(next); err != nil {
		return err
	}
	if isIHSequence {
		t.k = common.CopyBytes(t.ihK)
		return nil
	}
	if t.k, t.v, err = c.Seek(next); err != nil {
		return err
	}
	// Skip wrong incarnation
	if len(next) <= common.HashLength && len(t.k) > common.HashLength {
		// Advance past the storage to the first account
		if nextAccount(t.k, t.nextAccountKey[:]) {
			if t.k, t.v, err = c.Seek(t.nextAccountKey[:]); err != nil {
				return err
			}
		} else {
			t.k = nil
		}
	}
	if t.trace {
		fmt.Printf("k after next: %x\n", t.k)
	}
	return nil
}

func (t *PreOrderTraverse) CalcTrieRoot() (common.Hash, error) {
	if t.tx == nil {
		var err error
		t.tx, err = t.kv.Begin(context.Background(), nil, false)
		if err != nil {
			return EmptyRoot, err
		}
		defer t.tx.Rollback()
	}
	tx := t.tx
	c := tx.Cursor(t.stateBucket)
	var filter = func(k []byte) (bool, error) {
		if t.rd.Retain(k) {
			if t.hc != nil {
				if err := t.hc(k, nil); err != nil {
					return false, err
				}
			}
			return false, nil
		}

		return true, nil
	}
	ih := IH(Filter(filter, tx.Cursor(t.intermediateHashesBucket)))
	if err := t.iteration(c, ih, true /* first */); err != nil {
		return EmptyRoot, err
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for t.itemType != CutoffStreamItem {
		for !t.itemPresent {
			if err := t.iteration(c, ih, false /* first */); err != nil {
				return EmptyRoot, err
			}

		}
		if t.itemPresent {
			if err := t.receiver.Receive(t.itemType, t.accountKey, t.storageKey, &t.accountValue, t.storageValue, t.hashValue, t.streamCutoff); err != nil {
				return EmptyRoot, err
			}
			t.itemPresent = false

			select {
			default:
			case <-logEvery.C:
				t.logProgress()
			}
		}
	}

	return t.receiver.Root(), nil
}

func (t *PreOrderTraverse) logProgress() {
	var k string
	if t.accountKey != nil {
		k = makeCurrentKeyStr(t.accountKey)
	} else {
		k = makeCurrentKeyStr(t.ihK)
	}
	log.Info("Calculating Merkle root", "current key", k)
}

func (dr *RootCalculator) RetainNothing(prefix []byte) bool {
	return false
}

func (dr *RootCalculator) Reset(hc HashCollector, trace bool) {
	dr.hc = hc
	dr.curr.Reset()
	dr.succ.Reset()
	dr.value.Reset()
	dr.groups = dr.groups[:0]
	dr.a.Reset()
	dr.hb.Reset()
	dr.wasIH = false
	dr.currStorage.Reset()
	dr.succStorage.Reset()
	dr.valueStorage.Reset()
	dr.wasIHStorage = false
	dr.root = common.Hash{}
	dr.trace = trace
	dr.hb.trace = trace
}

func (dr *RootCalculator) Receive(itemType StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	cutoff int,
) error {
	switch itemType {
	case StorageStreamItem:
		dr.advanceKeysStorage(storageKey, true /* terminator */)
		if dr.currStorage.Len() > 0 {
			if err := dr.genStructStorage(); err != nil {
				return err
			}
		}
		dr.saveValueStorage(false, storageValue, hash)
	case SHashStreamItem:
		dr.advanceKeysStorage(storageKey, false /* terminator */)
		if dr.currStorage.Len() > 0 {
			if err := dr.genStructStorage(); err != nil {
				return err
			}
		}
		dr.saveValueStorage(true, storageValue, hash)
	case AccountStreamItem:
		dr.advanceKeysAccount(accountKey, true /* terminator */)
		if dr.curr.Len() > 0 && !dr.wasIH {
			dr.cutoffKeysStorage(2 * (common.HashLength + common.IncarnationLength))
			if dr.currStorage.Len() > 0 {
				if err := dr.genStructStorage(); err != nil {
					return err
				}
			}
			if dr.currStorage.Len() > 0 {
				if len(dr.groups) >= 2*common.HashLength {
					dr.groups = dr.groups[:2*common.HashLength-1]
				}
				for len(dr.groups) > 0 && dr.groups[len(dr.groups)-1] == 0 {
					dr.groups = dr.groups[:len(dr.groups)-1]
				}
				dr.currStorage.Reset()
				dr.succStorage.Reset()
				dr.wasIHStorage = false
				// There are some storage items
				dr.accData.FieldSet |= AccountFieldStorageOnly
			}
		}
		if dr.curr.Len() > 0 {
			if err := dr.genStructAccount(); err != nil {
				return err
			}
		}
		if err := dr.saveValueAccount(false, accountValue, hash); err != nil {
			return err
		}
	case AHashStreamItem:
		dr.advanceKeysAccount(accountKey, false /* terminator */)
		if dr.curr.Len() > 0 && !dr.wasIH {
			dr.cutoffKeysStorage(2 * (common.HashLength + common.IncarnationLength))
			if dr.currStorage.Len() > 0 {
				if err := dr.genStructStorage(); err != nil {
					return err
				}
			}
			if dr.currStorage.Len() > 0 {
				if len(dr.groups) >= 2*common.HashLength {
					dr.groups = dr.groups[:2*common.HashLength-1]
				}
				for len(dr.groups) > 0 && dr.groups[len(dr.groups)-1] == 0 {
					dr.groups = dr.groups[:len(dr.groups)-1]
				}
				dr.currStorage.Reset()
				dr.succStorage.Reset()
				dr.wasIHStorage = false
				// There are some storage items
				dr.accData.FieldSet |= AccountFieldStorageOnly
			}
		}
		if dr.curr.Len() > 0 {
			if err := dr.genStructAccount(); err != nil {
				return err
			}
		}
		if err := dr.saveValueAccount(true, accountValue, hash); err != nil {
			return err
		}
	case CutoffStreamItem:
		if dr.trace {
			fmt.Printf("storage cuttoff %d\n", cutoff)
		}

		dr.cutoffKeysAccount(cutoff)
		if dr.curr.Len() > 0 && !dr.wasIH {
			dr.cutoffKeysStorage(2 * (common.HashLength + common.IncarnationLength))
			if dr.currStorage.Len() > 0 {
				if err := dr.genStructStorage(); err != nil {
					return err
				}
			}
			if dr.currStorage.Len() > 0 {
				if len(dr.groups) >= 2*common.HashLength {
					dr.groups = dr.groups[:2*common.HashLength-1]
				}
				for len(dr.groups) > 0 && dr.groups[len(dr.groups)-1] == 0 {
					dr.groups = dr.groups[:len(dr.groups)-1]
				}
				dr.currStorage.Reset()
				dr.succStorage.Reset()
				dr.wasIHStorage = false
				// There are some storage items
				dr.accData.FieldSet |= AccountFieldStorageOnly
			}
		}
		if dr.curr.Len() > 0 {
			if err := dr.genStructAccount(); err != nil {
				return err
			}
		}
		if dr.curr.Len() > 0 {
			if len(dr.groups) > cutoff {
				dr.groups = dr.groups[:cutoff]
			}
			for len(dr.groups) > 0 && dr.groups[len(dr.groups)-1] == 0 {
				dr.groups = dr.groups[:len(dr.groups)-1]
			}
		}
		if dr.hb.hasRoot() {
			dr.root = dr.hb.rootHash()
		} else {
			dr.root = EmptyRoot
		}
		dr.groups = dr.groups[:0]
		dr.hb.Reset()
		dr.wasIH = false
		dr.wasIHStorage = false
		dr.curr.Reset()
		dr.succ.Reset()
		dr.currStorage.Reset()
		dr.succStorage.Reset()
	}
	return nil
}

func (dr *RootCalculator) Result() SubTries {
	panic("don't call me")
}

func (dr *RootCalculator) Root() common.Hash {
	return dr.root
}

func (dr *RootCalculator) advanceKeysStorage(k []byte, terminator bool) {
	dr.currStorage.Reset()
	dr.currStorage.Write(dr.succStorage.Bytes())
	dr.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	keyToNibbles(k, &dr.succStorage)

	if terminator {
		dr.succStorage.WriteByte(16)
	}
}

func (dr *RootCalculator) cutoffKeysStorage(cutoff int) {
	dr.currStorage.Reset()
	dr.currStorage.Write(dr.succStorage.Bytes())
	dr.succStorage.Reset()
	if dr.currStorage.Len() > 0 {
		dr.succStorage.Write(dr.currStorage.Bytes()[:cutoff-1])
		dr.succStorage.WriteByte(dr.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
	}
}

func (dr *RootCalculator) genStructStorage() error {
	var err error
	var data GenStructStepData
	if dr.wasIHStorage {
		dr.hashData.Hash = common.BytesToHash(dr.valueStorage.Bytes())
		data = &dr.hashData
	} else {
		dr.leafData.Value = rlphacks.RlpSerializableBytes(dr.valueStorage.Bytes())
		data = &dr.leafData
	}
	dr.groups, err = GenStructStep(dr.RetainNothing, dr.currStorage.Bytes(), dr.succStorage.Bytes(), dr.hb, dr.hc, data, dr.groups, dr.trace)
	if err != nil {
		return err
	}
	return nil
}

func (dr *RootCalculator) saveValueStorage(isIH bool, v, h []byte) {
	// Remember the current value
	dr.wasIHStorage = isIH
	dr.valueStorage.Reset()
	if isIH {
		dr.valueStorage.Write(h)
	} else {
		dr.valueStorage.Write(v)
	}
}

func (dr *RootCalculator) advanceKeysAccount(k []byte, terminator bool) {
	dr.curr.Reset()
	dr.curr.Write(dr.succ.Bytes())
	dr.succ.Reset()
	for _, b := range k {
		dr.succ.WriteByte(b / 16)
		dr.succ.WriteByte(b % 16)
	}
	if terminator {
		dr.succ.WriteByte(16)
	}
}

func (dr *RootCalculator) cutoffKeysAccount(cutoff int) {
	dr.curr.Reset()
	dr.curr.Write(dr.succ.Bytes())
	dr.succ.Reset()
	if dr.curr.Len() > 0 && cutoff > 0 {
		dr.succ.Write(dr.curr.Bytes()[:cutoff-1])
		dr.succ.WriteByte(dr.curr.Bytes()[cutoff-1] + 1) // Modify last nibble before the cutoff point
	}
}

func (dr *RootCalculator) genStructAccount() error {
	var data GenStructStepData
	if dr.wasIH {
		copy(dr.hashData.Hash[:], dr.value.Bytes())
		data = &dr.hashData
	} else {
		dr.accData.Balance.Set(&dr.a.Balance)
		if dr.a.Balance.Sign() != 0 {
			dr.accData.FieldSet |= AccountFieldBalanceOnly
		}
		dr.accData.Nonce = dr.a.Nonce
		if dr.a.Nonce != 0 {
			dr.accData.FieldSet |= AccountFieldNonceOnly
		}
		dr.accData.Incarnation = dr.a.Incarnation
		data = &dr.accData
	}
	dr.wasIHStorage = false
	dr.currStorage.Reset()
	dr.succStorage.Reset()
	var err error
	if dr.groups, err = GenStructStep(dr.RetainNothing, dr.curr.Bytes(), dr.succ.Bytes(), dr.hb, dr.hc, data, dr.groups, dr.trace); err != nil {
		return err
	}
	dr.accData.FieldSet = 0
	return nil
}

func (dr *RootCalculator) saveValueAccount(isIH bool, v *accounts.Account, h []byte) error {
	dr.wasIH = isIH
	if isIH {
		dr.value.Reset()
		dr.value.Write(h)
		return nil
	}
	dr.a.Copy(v)
	// Place code on the stack first, the storage will follow
	if !dr.a.IsEmptyCodeHash() {
		// the first item ends up deepest on the stack, the second item - on the top
		dr.accData.FieldSet |= AccountFieldCodeOnly
		if err := dr.hb.hash(dr.a.CodeHash[:]); err != nil {
			return err
		}
	}
	return nil
}

// FilterCursor - call .filter() and if it returns false - skip element
type FilterCursor struct {
	c ethdb.Cursor

	k, kHex, v []byte
	filter     func(k []byte) (bool, error)
}

func Filter(filter func(k []byte) (bool, error), c ethdb.Cursor) *FilterCursor {
	return &FilterCursor{c: c, filter: filter}
}

func (c *FilterCursor) _seek(seek []byte) (err error) {
	c.k, c.v, err = c.c.Seek(seek)
	if err != nil {
		return err
	}
	if c.k == nil {
		return nil
	}

	DecompressNibbles(c.k, &c.kHex)
	if ok, err := c.filter(c.kHex); err != nil {
		return err
	} else if ok {
		return nil
	}

	return c._next()
}

func (c *FilterCursor) _next() (err error) {
	c.k, c.v, err = c.c.Next()
	if err != nil {
		return err
	}
	for {
		if c.k == nil {
			return nil
		}

		DecompressNibbles(c.k, &c.kHex)
		var ok bool
		ok, err = c.filter(c.kHex)
		if err != nil {
			return err
		} else if ok {
			return nil
		}

		c.k, c.v, err = c.c.Next()
		if err != nil {
			return err
		}
	}
}

func (c *FilterCursor) Seek(seek []byte) ([]byte, []byte, error) {
	if err := c._seek(seek); err != nil {
		return []byte{}, nil, err
	}

	return c.k, c.v, nil
}

// IHCursor - holds logic related to iteration over IH bucket
type IHCursor struct {
	c *FilterCursor
}

func IH(c *FilterCursor) *IHCursor {
	return &IHCursor{c: c}
}

func (c *IHCursor) Seek(seek []byte) ([]byte, []byte, bool, error) {
	k, v, err := c.c.Seek(seek)
	if err != nil {
		return []byte{}, nil, false, err
	}

	if k == nil {
		return k, v, false, nil
	}

	return k, v, isSequence(seek, k), nil
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
