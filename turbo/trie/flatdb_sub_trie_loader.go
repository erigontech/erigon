package trie

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
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
