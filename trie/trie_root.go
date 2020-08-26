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

type RootLoader struct {
	trace              bool
	rl                 RetainDecider
	accAddrHashWithInc [40]byte // Concatenation of addrHash of the currently build account with its incarnation encoding
	tx                 ethdb.Tx
	kv                 ethdb.KV
	nextAccountKey     [32]byte
	k, v               []byte
	ihK, ihV           []byte

	itemPresent bool
	itemType    StreamItem

	// Storage item buffer
	storageKey   []byte
	storageValue []byte

	// Acount item buffer
	accountKey   []byte
	accountValue accounts.Account
	hashValue    []byte
	streamCutoff int

	receiver        StreamReceiver
	defaultReceiver *RootReceiver
	hc              HashCollector
}

type RootReceiver struct {
	trace        bool
	rl           RetainDecider
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

func NewTrieRootReceiver() *RootReceiver {
	return &RootReceiver{hb: NewHashBuilder(false)}
}

func NewTrieRootLoader() *RootLoader {
	fstl := &RootLoader{
		defaultReceiver: NewTrieRootReceiver(),
	}
	return fstl
}

// Reset prepares the loader for reuse
func (l *RootLoader) Reset(db ethdb.Database, rl RetainDecider, receiverDecider RetainDecider, hc HashCollector, trace bool) error {
	l.defaultReceiver.Reset(receiverDecider, hc, trace)
	l.hc = hc
	l.receiver = l.defaultReceiver

	l.trace = trace
	l.rl = rl
	l.itemPresent = false
	if l.trace {
		fmt.Printf("----------\n")
		fmt.Printf("RebuildTrie\n")
	}
	if l.trace {
		fmt.Printf("l.rl: %s\n", l.rl)
	}
	if hasTx, ok := db.(ethdb.HasTx); ok {
		l.tx = hasTx.Tx()
	} else {
		if hasKV, ok := db.(ethdb.HasKV); ok {
			l.kv = hasKV.KV()
		} else {
			return fmt.Errorf("database doest not implement KV: %T", db)
		}
	}

	return nil
}

func (l *RootLoader) SetStreamReceiver(receiver StreamReceiver) {
	l.receiver = receiver
}

// iteration moves through the database buckets and creates at most
// one stream item, which is indicated by setting the field fstl.itemPresent to true
func (l *RootLoader) iteration(c ethdb.Cursor, ih *IHCursor, first bool) error {
	var isIH, isIHSequence bool
	var minKey []byte
	var err error
	if first {
		if l.ihK, l.ihV, isIHSequence, err = ih.Seek([]byte{}); err != nil {
			return err
		}
		if isIHSequence {
			l.k = common.CopyBytes(l.ihK)
			return nil
		}
		if l.k, l.v, err = c.Seek([]byte{}); err != nil {
			return err
		}
		if len([]byte{}) <= common.HashLength && len(l.k) > common.HashLength {
			// Advance past the storage to the first account
			if nextAccount(l.k, l.nextAccountKey[:]) {
				if l.k, l.v, err = c.Seek(l.nextAccountKey[:]); err != nil {
					return err
				}
			} else {
				l.k = nil
			}
		}
		return nil
	}

	isIH, minKey = keyIsBeforeOrEqual(l.ihK, l.k)
	if minKey == nil {
		l.itemPresent = true
		l.itemType = CutoffStreamItem
		l.streamCutoff = 0
		l.accountKey = nil
		l.storageKey = nil
		l.storageValue = nil
		l.hashValue = nil
		return nil
	}

	if !isIH {
		if len(l.k) > common.HashLength && !bytes.HasPrefix(l.k, l.accAddrHashWithInc[:]) {
			if bytes.Compare(l.k, l.accAddrHashWithInc[:]) < 0 {
				// Skip all the irrelevant storage in the middle
				if l.k, l.v, err = c.Seek(l.accAddrHashWithInc[:]); err != nil {
					return err
				}
			} else {
				if nextAccount(l.k, l.nextAccountKey[:]) {
					if l.k, l.v, err = c.Seek(l.nextAccountKey[:]); err != nil {
						return err
					}
				} else {
					l.k = nil
				}
			}
			return nil
		}
		l.itemPresent = true
		if len(l.k) > common.HashLength {
			l.itemType = StorageStreamItem
			l.accountKey = nil
			l.storageKey = append(l.storageKey[:0], l.k...)
			l.hashValue = nil
			l.storageValue = append(l.storageValue[:0], l.v...)
			if l.k, l.v, err = c.Next(); err != nil {
				return err
			}
			if l.trace {
				fmt.Printf("k after storageWalker and Next: %x\n", l.k)
			}
		} else if len(l.k) > 0 {
			l.itemType = AccountStreamItem
			l.accountKey = append(l.accountKey[:0], l.k...)
			l.storageKey = nil
			l.storageValue = nil
			l.hashValue = nil
			if err := l.accountValue.DecodeForStorage(l.v); err != nil {
				return fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			copy(l.accAddrHashWithInc[:], l.k)
			binary.BigEndian.PutUint64(l.accAddrHashWithInc[32:], l.accountValue.Incarnation)

			// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
			// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
			// skips over all storage items
			if l.k, l.v, err = c.Seek(l.accAddrHashWithInc[:]); err != nil {
				return err
			}
			if l.trace {
				fmt.Printf("k after accountWalker and Seek: %x\n", l.k)
			}
			if keyIsBefore(l.ihK, l.accAddrHashWithInc[:]) {
				if l.ihK, l.ihV, _, err = ih.Seek(l.accAddrHashWithInc[:]); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// ih part
	if l.trace {
		fmt.Printf("l.ihK %x, l.accAddrHashWithInc %x\n", l.ihK, l.accAddrHashWithInc[:])
	}
	if len(l.ihK) > common.HashLength && !bytes.HasPrefix(l.ihK, l.accAddrHashWithInc[:]) {
		if bytes.Compare(l.ihK, l.accAddrHashWithInc[:]) < 0 {
			// Skip all the irrelevant storage in the middle
			if l.ihK, l.ihV, _, err = ih.Seek(l.accAddrHashWithInc[:]); err != nil {
				return err
			}
		} else {
			if nextAccount(l.ihK, l.nextAccountKey[:]) {
				if l.ihK, l.ihV, _, err = ih.Seek(l.nextAccountKey[:]); err != nil {
					return err
				}
			} else {
				l.ihK = nil
			}
		}
		return nil
	}
	l.itemPresent = true
	if len(l.ihK) > common.HashLength {
		l.itemType = SHashStreamItem
		l.accountKey = nil
		l.storageKey = append(l.storageKey[:0], l.ihK...)
		l.hashValue = append(l.hashValue[:0], l.ihV...)
		l.storageValue = nil
	} else {
		l.itemType = AHashStreamItem
		l.accountKey = append(l.accountKey[:0], l.ihK...)
		l.storageKey = nil
		l.storageValue = nil
		l.hashValue = append(l.hashValue[:0], l.ihV...)
	}

	// skip subtree
	next, ok := dbutils.NextSubtree(l.ihK)
	if !ok { // no siblings left
		l.k, l.ihK, l.ihV = nil, nil, nil
		return nil
	}
	if l.trace {
		fmt.Printf("next: %x\n", next)
	}

	if l.ihK, l.ihV, isIHSequence, err = ih.Seek(next); err != nil {
		return err
	}
	if isIHSequence {
		l.k = common.CopyBytes(l.ihK)
		return nil
	}
	if l.k, l.v, err = c.Seek(next); err != nil {
		return err
	}
	if len(next) <= common.HashLength && len(l.k) > common.HashLength {
		// Advance past the storage to the first account
		if nextAccount(l.k, l.nextAccountKey[:]) {
			if l.k, l.v, err = c.Seek(l.nextAccountKey[:]); err != nil {
				return err
			}
		} else {
			l.k = nil
		}
	}
	if l.trace {
		fmt.Printf("k after next: %x\n", l.k)
	}
	return nil
}

func (dr *RootReceiver) Reset(rl RetainDecider, hc HashCollector, trace bool) {
	dr.rl = rl
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

func (dr *RootReceiver) Receive(itemType StreamItem,
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

func (dr *RootReceiver) Result() SubTries {
	panic("don't call me")
}

func (dr *RootReceiver) Root() common.Hash {
	return dr.root
}

func (l *RootLoader) CalcTrieRoot() (common.Hash, error) {
	if l.tx == nil {
		var err error
		l.tx, err = l.kv.Begin(context.Background(), nil, false)
		if err != nil {
			return EmptyRoot, err
		}
		defer l.tx.Rollback()
	}
	tx := l.tx
	c := tx.Cursor(dbutils.CurrentStateBucket)
	var filter = func(k []byte) (bool, error) {
		if l.rl.Retain(k) {
			if l.hc != nil {
				if err := l.hc(k, nil); err != nil {
					return false, err
				}
			}
			return false, nil
		}

		return true, nil
	}
	ih := IH(Filter(filter, tx.Cursor(dbutils.IntermediateTrieHashBucket)))
	if err := l.iteration(c, ih, true /* first */); err != nil {
		return EmptyRoot, err
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for l.itemType != CutoffStreamItem {
		for !l.itemPresent {
			if err := l.iteration(c, ih, false /* first */); err != nil {
				return EmptyRoot, err
			}

		}
		if l.itemPresent {
			if err := l.receiver.Receive(l.itemType, l.accountKey, l.storageKey, &l.accountValue, l.storageValue, l.hashValue, l.streamCutoff); err != nil {
				return EmptyRoot, err
			}
			l.itemPresent = false

			select {
			default:
			case <-logEvery.C:
				l.logProgress()
			}
		}
	}

	return l.receiver.Root(), nil
}

func (l *RootLoader) logProgress() {
	var k string
	if l.accountKey != nil {
		k = makeCurrentKeyStr(l.accountKey)
	} else {
		k = makeCurrentKeyStr(l.ihK)
	}
	log.Info("Calculating Merkle root", "current key", k)
}

func (dr *RootReceiver) advanceKeysStorage(k []byte, terminator bool) {
	dr.currStorage.Reset()
	dr.currStorage.Write(dr.succStorage.Bytes())
	dr.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	keyToNibbles(k, &dr.succStorage)

	if terminator {
		dr.succStorage.WriteByte(16)
	}
}

func (dr *RootReceiver) cutoffKeysStorage(cutoff int) {
	dr.currStorage.Reset()
	dr.currStorage.Write(dr.succStorage.Bytes())
	dr.succStorage.Reset()
	if dr.currStorage.Len() > 0 {
		dr.succStorage.Write(dr.currStorage.Bytes()[:cutoff-1])
		dr.succStorage.WriteByte(dr.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
	}
}

func (dr *RootReceiver) genStructStorage() error {
	var err error
	var data GenStructStepData
	if dr.wasIHStorage {
		dr.hashData.Hash = common.BytesToHash(dr.valueStorage.Bytes())
		data = &dr.hashData
	} else {
		dr.leafData.Value = rlphacks.RlpSerializableBytes(dr.valueStorage.Bytes())
		data = &dr.leafData
	}
	dr.groups, err = GenStructStep(dr.rl.Retain, dr.currStorage.Bytes(), dr.succStorage.Bytes(), dr.hb, dr.hc, data, dr.groups, dr.trace)
	if err != nil {
		return err
	}
	return nil
}

func (dr *RootReceiver) saveValueStorage(isIH bool, v, h []byte) {
	// Remember the current value
	dr.wasIHStorage = isIH
	dr.valueStorage.Reset()
	if isIH {
		dr.valueStorage.Write(h)
	} else {
		dr.valueStorage.Write(v)
	}
}

func (dr *RootReceiver) advanceKeysAccount(k []byte, terminator bool) {
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

func (dr *RootReceiver) cutoffKeysAccount(cutoff int) {
	dr.curr.Reset()
	dr.curr.Write(dr.succ.Bytes())
	dr.succ.Reset()
	if dr.curr.Len() > 0 && cutoff > 0 {
		dr.succ.Write(dr.curr.Bytes()[:cutoff-1])
		dr.succ.WriteByte(dr.curr.Bytes()[cutoff-1] + 1) // Modify last nibble before the cutoff point
	}
}

func (dr *RootReceiver) genStructAccount() error {
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
	if dr.groups, err = GenStructStep(dr.rl.Retain, dr.curr.Bytes(), dr.succ.Bytes(), dr.hb, dr.hc, data, dr.groups, dr.trace); err != nil {
		return err
	}
	dr.accData.FieldSet = 0
	return nil
}

func (dr *RootReceiver) saveValueAccount(isIH bool, v *accounts.Account, h []byte) error {
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
