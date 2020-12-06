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
	itemPresent              bool
	itemType                 StreamItem
	stateBucket              string
	intermediateHashesBucket string
	rd                       RetainDecider
	accAddrHashWithInc       [40]byte // Concatenation of addrHash of the currently build account with its incarnation encoding
	nextAccountKey           [32]byte
	k, v                     []byte
	kHex, vHex               []byte
	ihK, ihV, ihSeek         []byte
	ihKStorage, ihVStorage   []byte

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
	l.rd = rd
	l.kHex, l.vHex = nil, nil
	l.itemPresent = false
	if l.trace {
		fmt.Printf("----------\n")
		fmt.Printf("CalcTrieRoot\n")
	}
	return nil
}

func (l *FlatDBTrieLoader) SetStreamReceiver(receiver StreamReceiver) {
	l.receiver = receiver
}

/*
// iteration moves through the database buckets and creates at most
// one stream item, which is indicated by setting the field fstl.itemPresent to true
func (l *FlatDBTrieLoader) iteration(accs *StateCursor, storages *StateCursor, ih *IHCursor, first bool) error {
	var isIH, isIHSequence bool
	var err error
	if first {
		if l.ihK, l.ihV, isIHSequence, err = ih.First(); err != nil {
			return err
		}
		if isIHSequence {
			l.kHex = l.ihK
			if len(l.kHex)%2 == 1 {
				l.kHex = append(l.kHex, 0)
			}
			l.k = make([]byte, len(l.kHex)/2)
			CompressNibbles(l.kHex, &l.k)
			return nil
		}
		if l.k, l.kHex, l.v, err = accs.Seek([]byte{}); err != nil {
			return err
		}

		//// Skip wrong incarnation
		//if len(l.k) > common.HashLength {
		//	if nextAccount(l.k, l.nextAccountKey[:]) {
		//		if l.k, l.kHex, l.v, err = accs.Seek(l.nextAccountKey[:]); err != nil {
		//			return err
		//		}
		//	} else {
		//		l.k = nil
		//	}
		//}
		return nil
	}

	if l.ihK == nil && l.k == nil { // loop termination
		l.itemPresent = true
		l.itemType = CutoffStreamItem
		l.accountKey = nil
		l.storageKey = nil
		l.storageValue = nil
		l.hashValue = nil
		return nil
	}

	isIH, _ = keyIsBeforeOrEqual(l.ihK, l.kHex)
	if !isIH {
		// skip wrong incarnation
		if len(l.k) > common.HashLength && !bytes.HasPrefix(l.k, l.accAddrHashWithInc[:]) {
			if bytes.Compare(l.k, l.accAddrHashWithInc[:]) < 0 {
				// Skip all the irrelevant storage in the middle
				if l.k, l.kHex, l.v, err = storages.Seek(l.accAddrHashWithInc[:]); err != nil {
					return err
				}
				//} else {
				//	if l.k, l.kHex, l.v, err = accs.Next(); err != nil {
				//		return err
				//	}
			}
			return nil
		}
		l.itemPresent = true
		if len(l.k) > common.HashLength {
			l.itemType = StorageStreamItem
			l.accountKey = nil
			l.storageKey = common.CopyBytes(l.kHex) // no reason to copy, because this "pointer and data" will valid until end of transaction
			l.hashValue = nil
			l.storageValue = l.v
			if l.k, l.kHex, l.v, err = storages.Next(); err != nil {
				return err
			}
			if l.trace {
				fmt.Printf("k after storageWalker and Next: %x\n", l.k)
			}
		} else if len(l.k) > 0 {
			l.itemType = AccountStreamItem
			l.accountKey = common.CopyBytes(l.kHex)
			l.storageKey = nil
			l.storageValue = nil
			l.hashValue = nil
			if err = l.accountValue.DecodeForStorage(l.v); err != nil {
				return fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			copy(l.accAddrHashWithInc[:], l.k)
			binary.BigEndian.PutUint64(l.accAddrHashWithInc[32:], l.accountValue.Incarnation)

			DecompressNibbles(l.accAddrHashWithInc[:], &l.ihSeek)
			if l.accountValue.Incarnation > 0 {
				// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
				// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
				// skips over all storage items
				if l.k, l.kHex, l.v, err = storages.Seek(l.accAddrHashWithInc[:]); err != nil {
					return err
				}
				for keyIsBefore(l.ihK, l.ihSeek) {
					if l.ihK, l.ihV, _, err = ih.Next(); err != nil {
						return err
					}
				}
			} else {
				fmt.Printf("before: %x\n", l.k)
				if l.k, l.kHex, l.v, err = accs.Next(); err != nil {
					return err
				}
				if keyIsBefore(l.ihK, l.ihSeek) {
					if l.ihK, l.ihV, _, err = ih.Next(); err != nil {
						return err
					}
				}
			}

		}
		return nil
	}

	// ih part
	if l.trace {
		fmt.Printf("l.ihK %x, l.accAddrHashWithInc %x\n", l.ihK, l.accAddrHashWithInc[:])
	}

	// Skip IH with wrong incarnation
	DecompressNibbles(l.accAddrHashWithInc[:], &l.ihSeek)
	if len(l.ihK) > common.HashLength*2 && !bytes.HasPrefix(l.ihK, l.ihSeek) {
		if bytes.Compare(l.ihK, l.ihSeek) < 0 {
			// Skip all the irrelevant storage in the middle
			if l.ihK, l.ihV, _, err = ih.SeekToAccount(l.ihSeek); err != nil {
				return err
			}
		} else {
			if l.ihK, l.ihV, _, err = ih.Next(); err != nil {
				return err
			}
		}
		return nil
	}
	l.itemPresent = true
	if len(l.ihK) > common.HashLength*2 {
		l.itemType = SHashStreamItem
		l.accountKey = nil
		l.storageKey = l.ihK
		l.hashValue = l.ihV
		l.storageValue = nil
	} else {
		l.itemType = AHashStreamItem
		l.accountKey = l.ihK
		l.storageKey = nil
		l.storageValue = nil
		l.hashValue = l.ihV
	}

	// go to Next Sub-Tree
	next, ok := dbutils.NextSubtreeHex(l.ihK)
	if !ok { // no siblings left
		l.k, l.kHex, l.ihK, l.ihV = nil, nil, nil, nil
		return nil
	}
	if l.trace {
		fmt.Printf("next: %x\n", next)
	}

	if l.ihK, l.ihV, isIHSequence, err = ih.Next(); err != nil {
		return err
	}

	if isIHSequence {
		l.kHex = l.ihK
		if len(l.kHex)%2 == 1 {
			l.kHex = append(l.kHex, 0)
		}
		l.k = make([]byte, len(l.kHex)/2)
		CompressNibbles(l.kHex, &l.k)
		return nil
	}
	if len(next)%2 == 1 {
		next = append(next, 0)
	}
	next2 := make([]byte, len(next)/2)
	CompressNibbles(next, &next2)
	if len(next) <= common.HashLength {
		if l.k, l.kHex, l.v, err = accs.Seek(next2); err != nil {
			return err
		}
	} else {
		if l.k, l.kHex, l.v, err = storages.Seek(next2); err != nil {
			return err
		}
	}

	//// Skip wrong incarnation
	//if len(next2) <= common.HashLength && len(l.k) > common.HashLength {
	//	// Advance past the storage to the first account
	//	if l.k, l.kHex, l.v, err = accs.Next(); err != nil {
	//		return err
	//	}
	//}
	//if l.trace {
	//	fmt.Printf("k after next: %x\n", l.k)
	//}
	return nil
}
*/

/*
// CalcTrieRoot - spawn 2 cursors (IntermediateHashes and HashedState)
// Wrap IntermediateHashes cursor to IH class - this class will return only keys which passed RetainDecider check
// If RetainDecider check not passed, then such key must be deleted - HashCollector receiving nil for such key.
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

	accsC, storageC := tx.Cursor(dbutils.HashedAccountsBucket), tx.Cursor(dbutils.HashedAccountsBucket)
	accs, storages := NewStateCursor(accsC), NewStateCursor(storageC)
	cursors := [73]ethdb.Cursor{}
	for i := 0; i < 73; i++ {
		cursors[i] = tx.Cursor(dbutils.IntermediateTrieHashBucket3)
	}
	var filter = func(k []byte) bool {
		return !l.rd.Retain(k)
	}
	ih := IH(filter, cursors)
	rightCursor2 := func() ethdb.Cursor {
		if ih.i <= 32 {
			return accsC
		}
		return storageC
	}
	rightCursor := func() *StateCursor {
		if ih.i <= 32 {
			return accs
		}
		return storages
	}
	_, _ = rightCursor, rightCursor2
	if err := l.iteration(accs, storages, ih, true ); err != nil {
		return EmptyRoot, err
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for l.itemType != CutoffStreamItem {
		if err := common.Stopped(quit); err != nil {
			return EmptyRoot, err
		}

		for !l.itemPresent {
			if err := l.iteration(accs, storages, ih, false ); err != nil {
				return EmptyRoot, err
			}
		}

		if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
			return EmptyRoot, err
		}
		l.itemPresent = false

		select {
		default:
		case <-logEvery.C:
			l.logProgress()
		}
	}

	if !useExternalTx {
		_, err := txDB.Commit()
		if err != nil {
			return EmptyRoot, err
		}
	}

	return l.receiver.Root(), nil
}
*/

// CalcTrieRoot - spawn 2 cursors (IntermediateHashes and HashedState)
// Wrap IntermediateHashes cursor to IH class - this class will return only keys which passed RetainDecider check
// If RetainDecider check not passed, then such key must be deleted - HashCollector receiving nil for such key.
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

	accsC, storageC := tx.Cursor(dbutils.HashedAccountsBucket), tx.Cursor(dbutils.HashedStorageBucket)
	accs, storages := NewStateCursor(accsC), NewStateCursor(storageC)
	cursors := [161]ethdb.Cursor{}
	for i := 0; i < 161; i++ {
		cursors[i] = tx.Cursor(dbutils.IntermediateTrieHashBucket3)
	}
	var filter = func(k []byte) bool {
		return !l.rd.Retain(k)
	}
	ih := IH(filter, cursors)
	ihStorage := IHStorage(filter, cursors)
	rightCursor2 := func() ethdb.Cursor {
		if ih.i <= 32 {
			return accsC
		}
		return storageC
	}
	rightCursor := func() *StateCursor {
		if ih.i <= 32 {
			return accs
		}
		return storages
	}
	_, _ = rightCursor, rightCursor2

	var isIHSequence bool
	var err error
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	l.ihK, l.ihV, isIHSequence, err = ih.First()
	if err != nil {
		return EmptyRoot, err
	}

	for {
		if err := common.Stopped(quit); err != nil {
			return EmptyRoot, err
		}
		if isIHSequence {
			l.itemType = AHashStreamItem
			l.accountKey = l.ihK
			l.storageKey = nil
			l.storageValue = nil
			l.hashValue = l.ihV
			if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
				return EmptyRoot, err
			}
			l.ihK, l.ihV, isIHSequence, err = ih.Next()
			continue
		}

		nextHex, _ := dbutils.NextSubtreeHex(ih.PrevKey())
		cmpBits := len(nextHex) * 8
		if len(nextHex)%2 == 1 {
			nextHex = append(nextHex, 0)
		}
		next := make([]byte, len(nextHex)/2)
		CompressNibbles(nextHex, &next)

		if err = ethdb.Walk(accsC, next, cmpBits, func(k, v []byte) (bool, error) {
			//fmt.Printf("22: %x\n", k)
			l.k, l.v = k, v
			DecompressNibbles(l.k, &l.kHex)
			l.itemType = AccountStreamItem
			l.accountKey = common.CopyBytes(l.kHex)
			l.storageKey = nil
			l.storageValue = nil
			l.hashValue = nil
			if err = l.accountValue.DecodeForStorage(l.v); err != nil {
				return false, fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			copy(l.accAddrHashWithInc[:], l.k)
			binary.BigEndian.PutUint64(l.accAddrHashWithInc[32:], l.accountValue.Incarnation)
			if err = l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
				return false, err
			}
			l.ihKStorage, l.ihVStorage, isIHSequence, err = ihStorage.SeekToAccount(l.accAddrHashWithInc[:])
			if err != nil {
				return false, err
			}
			for {
				if err := common.Stopped(quit); err != nil {
					return false, err
				}
				if isIHSequence {
					l.itemType = SHashStreamItem
					l.accountKey = nil
					l.storageKey = l.ihKStorage
					l.hashValue = l.ihVStorage
					l.storageValue = nil
					if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
						return false, err
					}
					l.ihKStorage, l.ihVStorage, isIHSequence, err = ihStorage.Next()
					continue
				}

				nextHex2, _ := dbutils.NextSubtreeHex(ih.PrevKey())
				cmpBits2 := len(nextHex2) * 8
				if len(nextHex2)%2 == 1 {
					nextHex2 = append(nextHex2, 0)
				}
				next2 := make([]byte, len(nextHex2)/2)
				CompressNibbles(nextHex2, &next2)

				if err = ethdb.Walk(accsC, next2, cmpBits2, func(k, v []byte) (bool, error) {
					l.k, l.v = k, v
					DecompressNibbles(l.k, &l.kHex)
					l.itemType = StorageStreamItem
					l.accountKey = nil
					l.storageKey = common.CopyBytes(l.kHex) // no reason to copy, because this "pointer and data" will valid until end of transaction
					l.hashValue = nil
					l.storageValue = common.CopyBytes(l.v)
					if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
						return false, err
					}
					return true, nil
				}); err != nil {
					return false, err
				}

				l.itemType = SHashStreamItem
				l.accountKey = nil
				l.storageKey = l.ihKStorage
				l.hashValue = l.ihVStorage
				l.storageValue = nil
				if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
					return false, err
				}
				l.ihKStorage, l.ihVStorage, isIHSequence, err = ihStorage.Next()
				if err != nil {
					return false, err
				}

				if len(l.ihKStorage) == 0 { // Loop termination
					break
				}

			}
			return true, nil
			//fmt.Printf("Before storage loop: %x, %x\n", l.ihK, l.accAddrHashWithInc)
			//fmt.Printf("After storage loop: %x, %x\n", l.ihK, l.accAddrHashWithInc)
		}); err != nil {
			return EmptyRoot, err
		}
		if len(l.ihK) > 0 {
			l.itemType = AHashStreamItem
			l.accountKey = l.ihK
			l.storageKey = nil
			l.storageValue = nil
			l.hashValue = l.ihV
			if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
				return EmptyRoot, err
			}
			l.ihK, l.ihV, isIHSequence, err = ih.Next()
			if err != nil {
				return EmptyRoot, err
			}
		}

		if len(l.ihK) == 0 { // Loop termination
			break
		}

		select {
		default:
		case <-logEvery.C:
			l.logProgress()
		}
	}

	if err := l.receiver.Receive(CutoffStreamItem, nil, nil, nil, nil, nil, 0); err != nil {
		return EmptyRoot, err
	}

	//for l.itemType != CutoffStreamItem {
	//	if err := common.Stopped(quit); err != nil {
	//		return EmptyRoot, err
	//	}
	//	for !l.itemPresent {
	//		if err := l.iteration(c, ih, false); err != nil {
	//			return EmptyRoot, err
	//		}
	//	}
	//
	//	if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, 0); err != nil {
	//		return EmptyRoot, err
	//	}
	//	l.itemPresent = false
	//
	//	select {
	//	default:
	//	case <-logEvery.C:
	//		l.logProgress()
	//	}
	//}

	if !useExternalTx {
		_, err := txDB.Commit()
		if err != nil {
			return EmptyRoot, err
		}
	}

	return l.receiver.Root(), nil
}

func NextIH(in []byte) ([]byte, bool) {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 15 {
			r[i]++
			return r, true
		}
	}
	return nil, false
}

func (l *FlatDBTrieLoader) logProgress() {
	var k string
	if l.accountKey != nil {
		k = makeCurrentKeyStr(l.accountKey)
	} else if l.storageKey != nil {
		k = makeCurrentKeyStr(l.storageKey)
	} else if l.ihK != nil {
		k = makeCurrentKeyStr(l.ihK)
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
		r.hashData.Hash = common.BytesToHash(r.valueStorage)
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
		r.valueStorage = h
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
		copy(r.hashData.Hash[:], r.value)
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
		r.value = h
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
	c          [161]ethdb.Cursor
	filter     Filter
	i          int
	prev       []byte
	buf        []byte
	parents    [161][]byte
	accWithInc []byte
}

func IH(f Filter, c [161]ethdb.Cursor) *IHCursor {
	ih := &IHCursor{c: c, filter: f, i: 1}
	ih.parents[1] = []byte{}
	return ih
}

func (c *IHCursor) PrevKey() []byte {
	if len(c.prev) == 0 {
		return nil
	}
	return c.prev[1:]
}

func (c *IHCursor) First() (k, v []byte, isSeq bool, err error) {
	k, v, err = c._first()
	if err != nil {
		return []byte{}, nil, false, err
	}

	if k == nil {
		return nil, nil, false, nil
	}

	return common.CopyBytes(k[1:]), common.CopyBytes(v), isSequence(c.prev[1:], k[1:]), nil
}

func (c *IHCursor) Next() (k, v []byte, isSeq bool, err error) {
	k, v, err = c._next()
	if err != nil {
		return []byte{}, nil, false, err
	}

	if k == nil {
		return nil, nil, false, nil
	}

	return common.CopyBytes(k[1:]), common.CopyBytes(v), isSequence(c.prev[1:], k[1:]), nil
}

func (c *IHCursor) _first() (k, v []byte, err error) {
	cursor := c.c[c.i]
	k, v, err = cursor.First()
	if err != nil {
		return []byte{}, nil, err
	}

	for {
		if k == nil || len(k)-1 > c.i || !bytes.HasPrefix(k[1:], c.parents[c.i]) {
			if c.i == 1 {
				return nil, nil, nil
			}
			c.i--
			cursor = c.c[c.i]
			k, v, err = cursor.Next()
			if err != nil {
				return []byte{}, nil, err
			}
			continue
		}

		// if filter allow us, return. otherwise delete and go level-down
		if c.filter(k[1:]) {
			c.prev = common.CopyBytes(k)
			return k, v, nil
		}

		k = common.CopyBytes(k)
		err = cursor.DeleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}
		c.i++
		c.parents[c.i] = k[1:]
		cursor = c.c[c.i]
		c.buf = append(append(c.buf[:0], uint8(c.i)), c.parents[c.i]...)
		k, v, err = cursor.Seek(c.buf)
		if err != nil {
			return []byte{}, nil, err
		}
	}
}

func (c *IHCursor) _next() (k, v []byte, err error) {
	cursor := c.c[c.i]
	k, v, err = cursor.Next()
	if err != nil {
		return []byte{}, nil, err
	}

	for {
		if k == nil || len(k)-1 > c.i || !bytes.HasPrefix(k[1:], c.parents[c.i]) {
			if c.i == 1 {
				return nil, nil, nil
			}
			c.i--
			cursor = c.c[c.i]
			k, v, err = cursor.Next()
			if err != nil {
				return []byte{}, nil, err
			}
			continue
		}

		// if filter allow us, return. otherwise delete and go level-down
		if c.filter(k[1:]) {
			c.prev = common.CopyBytes(k)
			return k, v, nil
		}

		k = common.CopyBytes(k)
		err = cursor.DeleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}
		c.i++
		c.parents[c.i] = k[1:]
		cursor = c.c[c.i]
		c.buf = append(append(c.buf[:0], uint8(c.i)), c.parents[c.i]...)
		k, v, err = cursor.Seek(c.buf)
		if err != nil {
			return []byte{}, nil, err
		}
	}
}

// IHStorageCursor - holds logic related to iteration over IH bucket
type IHStorageCursor struct {
	c          [161]ethdb.Cursor
	filter     Filter
	i          int
	prev       []byte
	buf        []byte
	parents    [161][]byte
	accWithInc []byte
}

func IHStorage(f Filter, c [161]ethdb.Cursor) *IHStorageCursor {
	ih := &IHStorageCursor{c: c, filter: f, i: 1}
	ih.parents[1] = []byte{}
	return ih
}

func (c *IHStorageCursor) PrevKey() []byte {
	if len(c.prev) == 0 {
		return nil
	}
	return c.prev[1:]
}

func (c *IHStorageCursor) SeekToAccount(seek []byte) (k, v []byte, isSeq bool, err error) {
	c.i = 80
	c.parents[c.i] = seek
	k, v, err = c._seek(seek)
	if err != nil {
		return []byte{}, nil, false, err
	}

	if k == nil {
		return nil, nil, false, nil
	}

	return common.CopyBytes(k[1:]), common.CopyBytes(v), isSequence(c.prev[1:], k[1:]), nil
}

func (c *IHStorageCursor) _seek(seek []byte) (k, v []byte, err error) {
	cursor := c.c[c.i]
	k, v, err = cursor.Seek(append([]byte{uint8(c.i)}, seek...))
	if err != nil {
		return []byte{}, nil, err
	}

	for {
		if k == nil || len(k)-1 > c.i || !bytes.HasPrefix(k[1:], c.parents[c.i]) {
			if c.i == 80 {
				return nil, nil, nil
			}
			c.i--
			cursor = c.c[c.i]
			k, v, err = cursor.Next()
			if err != nil {
				return []byte{}, nil, err
			}
			continue
		}

		// if filter allow us, return. otherwise delete and go level-down
		if c.filter(k[1:]) {
			c.prev = common.CopyBytes(k)
			return k, v, nil
		}

		k = common.CopyBytes(k)
		err = cursor.DeleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}

		c.i++
		c.parents[c.i] = k[1:]
		cursor = c.c[c.i]
		c.buf = c.buf[:0]
		c.buf = append(append(c.buf, uint8(c.i)), c.parents[c.i]...)
		k, v, err = cursor.Seek(c.buf)
		if err != nil {
			return []byte{}, nil, err
		}
	}
}

func (c *IHStorageCursor) Next() (k, v []byte, isSeq bool, err error) {
	k, v, err = c._next()
	if err != nil {
		return []byte{}, nil, false, err
	}

	if k == nil {
		return nil, nil, false, nil
	}

	return common.CopyBytes(k[1:]), common.CopyBytes(v), isSequence(c.prev[1:], k[1:]), nil
}

func (c *IHStorageCursor) _next() (k, v []byte, err error) {
	cursor := c.c[c.i]
	k, v, err = cursor.Next()
	if err != nil {
		return []byte{}, nil, err
	}

	for {
		if k == nil || len(k)-1 > c.i || !bytes.HasPrefix(k[1:], c.parents[c.i]) {
			if c.i == 80 {
				return nil, nil, nil
			}
			c.i--
			cursor = c.c[c.i]
			k, v, err = cursor.Next()
			if err != nil {
				return []byte{}, nil, err
			}
			continue
		}

		// if filter allow us, return. otherwise delete and go level-down
		if c.filter(k[1:]) {
			c.prev = common.CopyBytes(k)
			return k, v, nil
		}

		k = common.CopyBytes(k)
		err = cursor.DeleteCurrent()
		if err != nil {
			return []byte{}, nil, err
		}
		c.i++
		c.parents[c.i] = k[1:]
		cursor = c.c[c.i]
		c.buf = append(append(c.buf[:0], uint8(c.i)), c.parents[c.i]...)
		k, v, err = cursor.Seek(c.buf)
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
