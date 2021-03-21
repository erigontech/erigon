package trie

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
)

var (
	trieFlatDbSubTrieLoaderTimer = metrics.NewRegisteredTimer("trie/subtrieloader/flatdb", nil)
)

type StreamReceiver interface {
	Receive(
		itemType StreamItem,
		accountKey []byte,
		storageKey []byte,
		accountValue *accounts.Account,
		storageValue []byte,
		hash []byte,
		hasTree bool,
		cutoff int,
	) error

	Result() SubTries
	Root() common.Hash
}

type DefaultReceiver struct {
	trace        bool
	rl           RetainDecider
	hc           HashCollector
	subTries     SubTries
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

func NewDefaultReceiver() *DefaultReceiver {
	return &DefaultReceiver{hb: NewHashBuilder(false)}
}

func (dr *DefaultReceiver) Reset(rl RetainDecider, hc HashCollector, trace bool) {
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
	dr.subTries = SubTries{}
	dr.trace = trace
	dr.hb.trace = trace
}

func (dr *DefaultReceiver) Receive(itemType StreamItem,
	accountKey []byte,
	storageKey []byte,
	accountValue *accounts.Account,
	storageValue []byte,
	hash []byte,
	hasTree bool,
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
		if cutoff >= 2*(common.HashLength+common.IncarnationLength) {
			dr.cutoffKeysStorage(cutoff)
			if dr.currStorage.Len() > 0 {
				if err := dr.genStructStorage(); err != nil {
					return err
				}
			}
			if dr.currStorage.Len() > 0 {
				if len(dr.groups) >= cutoff {
					dr.groups = dr.groups[:cutoff-1]
				}
				for len(dr.groups) > 0 && dr.groups[len(dr.groups)-1] == 0 {
					dr.groups = dr.groups[:len(dr.groups)-1]
				}
				dr.currStorage.Reset()
				dr.succStorage.Reset()
				dr.wasIHStorage = false
				dr.subTries.roots = append(dr.subTries.roots, dr.hb.root())
				dr.subTries.Hashes = append(dr.subTries.Hashes, dr.hb.rootHash())
			} else {
				dr.subTries.roots = append(dr.subTries.roots, nil)
				dr.subTries.Hashes = append(dr.subTries.Hashes, common.Hash{})
			}
		} else {
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
				dr.subTries.roots = append(dr.subTries.roots, dr.hb.root())
				dr.subTries.Hashes = append(dr.subTries.Hashes, dr.hb.rootHash())
			} else {
				dr.subTries.roots = append(dr.subTries.roots, nil)
				dr.subTries.Hashes = append(dr.subTries.Hashes, EmptyRoot)
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
	}
	return nil
}

func (dr *DefaultReceiver) Result() SubTries {
	return dr.subTries
}

func (fstl *FlatDbSubTrieLoader) LoadSubTries() (SubTries, error) {
	defer trieFlatDbSubTrieLoaderTimer.UpdateSince(time.Now())
	if len(fstl.dbPrefixes) == 0 {
		return SubTries{}, nil
	}
	if fstl.tx == nil {
		var err error
		fstl.tx, err = fstl.kv.Begin(context.Background())
		if err != nil {
			return SubTries{}, err
		}
		defer fstl.tx.Rollback()
	}
	tx := fstl.tx
	c := tx.Cursor(dbutils.CurrentStateBucketOld2)
	var filter = func(k []byte) (bool, error) {

		if fstl.rl.Retain(k) {
			if fstl.hc != nil {
				if err := fstl.hc(k, nil); err != nil {
					return false, err
				}
			}
			return false, nil
		}

		if len(k) < fstl.cutoffs[fstl.rangeIdx] {
			return false, nil
		}

		return true, nil
	}
	ih := NewIHCursor2(NewFilterCursor2(filter, tx.CursorDupSort(dbutils.IntermediateTrieHashBucketOld2)))
	if err := fstl.iteration(c, ih, true /* first */); err != nil {
		return SubTries{}, err
	}
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for fstl.rangeIdx < len(fstl.dbPrefixes) {
		for !fstl.itemPresent {
			if err := fstl.iteration(c, ih, false /* first */); err != nil {
				return SubTries{}, err
			}

		}
		if fstl.itemPresent {
			if err := fstl.receiver.Receive(fstl.itemType, fstl.accountKey, fstl.storageKey, &fstl.accountValue, fstl.storageValue, fstl.hashValue, false, fstl.streamCutoff); err != nil {
				return SubTries{}, err
			}
			fstl.itemPresent = false

			select {
			default:
			case <-logEvery.C:
				fstl.logProgress()
			}
		}
	}

	return fstl.receiver.Result(), nil
}

func (fstl *FlatDbSubTrieLoader) logProgress() {
	var k string
	if fstl.accountKey != nil {
		k = makeCurrentKeyStr(fstl.accountKey)
	} else {
		k = makeCurrentKeyStr(fstl.ihK)
	}
	log.Info("Calculating Merkle root", "current key", k)
}

func makeCurrentKeyStr(k []byte) string {
	var currentKeyStr string
	if k == nil {
		currentKeyStr = "final"
	} else if len(k) < 4 {
		currentKeyStr = fmt.Sprintf("%x", k)
	} else {
		currentKeyStr = fmt.Sprintf("%x...", k[:4])
	}
	return currentKeyStr
}

func keyToNibbles(k []byte, w io.ByteWriter) {
	for _, b := range k {
		//nolint:errcheck
		w.WriteByte(b / 16)
		//nolint:errcheck
		w.WriteByte(b % 16)
	}
}

func (dr *DefaultReceiver) advanceKeysStorage(k []byte, terminator bool) {
	dr.currStorage.Reset()
	dr.currStorage.Write(dr.succStorage.Bytes())
	dr.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	keyToNibbles(k, &dr.succStorage)

	if terminator {
		dr.succStorage.WriteByte(16)
	}
}

func (dr *DefaultReceiver) Root() common.Hash {
	panic("don't use me")
}

func (dr *DefaultReceiver) cutoffKeysStorage(cutoff int) {
	dr.currStorage.Reset()
	dr.currStorage.Write(dr.succStorage.Bytes())
	dr.succStorage.Reset()
	if dr.currStorage.Len() > 0 {
		dr.succStorage.Write(dr.currStorage.Bytes()[:cutoff-1])
		dr.succStorage.WriteByte(dr.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
	}
}

func (dr *DefaultReceiver) genStructStorage() error {
	var err error
	var data GenStructStepData
	if dr.wasIHStorage {
		dr.hashData.Hash = common.BytesToHash(dr.valueStorage.Bytes())
		data = &dr.hashData
	} else {
		dr.leafData.Value = rlphacks.RlpSerializableBytes(dr.valueStorage.Bytes())
		data = &dr.leafData
	}
	dr.groups, err = GenStructStepOld(dr.rl.Retain, dr.currStorage.Bytes(), dr.succStorage.Bytes(), dr.hb, dr.hc, data, dr.groups, dr.trace)
	if err != nil {
		return err
	}
	return nil
}

func (dr *DefaultReceiver) saveValueStorage(isIH bool, v, h []byte) {
	// Remember the current value
	dr.wasIHStorage = isIH
	dr.valueStorage.Reset()
	if isIH {
		dr.valueStorage.Write(h)
	} else {
		dr.valueStorage.Write(v)
	}
}

func (dr *DefaultReceiver) advanceKeysAccount(k []byte, terminator bool) {
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

func (dr *DefaultReceiver) cutoffKeysAccount(cutoff int) {
	dr.curr.Reset()
	dr.curr.Write(dr.succ.Bytes())
	dr.succ.Reset()
	if dr.curr.Len() > 0 && cutoff > 0 {
		dr.succ.Write(dr.curr.Bytes()[:cutoff-1])
		dr.succ.WriteByte(dr.curr.Bytes()[cutoff-1] + 1) // Modify last nibble before the cutoff point
	}
}

func (dr *DefaultReceiver) genStructAccount() error {
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
	if dr.groups, err = GenStructStepOld(dr.rl.Retain, dr.curr.Bytes(), dr.succ.Bytes(), dr.hb, nil, data, dr.groups, dr.trace); err != nil {
		return err
	}
	dr.accData.FieldSet = 0
	return nil
}

func (dr *DefaultReceiver) saveValueAccount(isIH bool, v *accounts.Account, h []byte) error {
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
type FilterCursor2 struct {
	c ethdb.Cursor

	k, kHex, v []byte
	filter     func(k []byte) (bool, error)
}

func NewFilterCursor2(filter func(k []byte) (bool, error), c ethdb.Cursor) *FilterCursor2 {
	return &FilterCursor2{c: c, filter: filter}
}

func (c *FilterCursor2) _seek(seek []byte) (err error) {
	c.k, c.v, err = c.c.Seek(seek)
	if err != nil {
		return err
	}
	if c.k == nil {
		return nil
	}

	hexutil.DecompressNibbles(c.k, &c.kHex)
	if ok, err := c.filter(c.kHex); err != nil {
		return err
	} else if ok {
		return nil
	}

	return c._next()
}

func (c *FilterCursor2) _next() (err error) {
	c.k, c.v, err = c.c.Next()
	if err != nil {
		return err
	}
	for {
		if c.k == nil {
			return nil
		}

		hexutil.DecompressNibbles(c.k, &c.kHex)
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

func (c *FilterCursor2) Seek(seek []byte) ([]byte, []byte, error) {
	if err := c._seek(seek); err != nil {
		return []byte{}, nil, err
	}

	return c.k, c.v, nil
}

// AccTrieCursor - holds logic related to iteration over AccTrie bucket
type IHCursor2 struct {
	c *FilterCursor2
}

func NewIHCursor2(c *FilterCursor2) *IHCursor2 {
	return &IHCursor2{c: c}
}

func (c *IHCursor2) Seek(seek []byte) ([]byte, []byte, bool, error) {
	k, v, err := c.c.Seek(seek)
	if err != nil {
		return []byte{}, nil, false, err
	}

	if k == nil {
		return k, v, false, nil
	}

	return k, v, isSequenceOld(seek, k), nil
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
