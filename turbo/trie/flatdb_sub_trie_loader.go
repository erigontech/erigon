package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/ethdb"
	"github.com/erigontech/erigon/turbo/rlphacks"
)

var (
	trieFlatDbSubTrieLoaderTimer = metrics.NewHistTimer("trie_subtrieloader_flatdb")
)

type StreamReceiver[T any] interface {
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

	Legacy() bool
	Result() T
	EmptyResult() T
}

type FlatDbSubTrieLoader struct {
	trace              bool
	rl                 RetainDecider
	rangeIdx           int
	accAddrHashWithInc [40]byte // Concatenation of addrHash of the currently build account with its incarnation encoding
	dbPrefixes         [][]byte
	fixedbytes         []int
	masks              []byte
	cutoffs            []int
	tx                 kv.Tx
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

	receiver        StreamReceiver[SubTries]
	defaultReceiver *DefaultReceiver
	hc              HashCollector
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

func NewFlatDbSubTrieLoader() *FlatDbSubTrieLoader {
	fstl := &FlatDbSubTrieLoader{
		defaultReceiver: NewDefaultReceiver(),
	}
	return fstl
}

// Reset prepares the loader for reuse
func (fstl *FlatDbSubTrieLoader) Reset(db kv.Tx, rl RetainDecider, receiverDecider RetainDecider, hc HashCollector, dbPrefixes [][]byte, fixedbits []int, trace bool) error {
	fstl.defaultReceiver.Reset(receiverDecider, hc, trace)
	fstl.hc = hc
	fstl.receiver = fstl.defaultReceiver
	fstl.rangeIdx = 0

	fstl.trace = trace
	fstl.rl = rl
	fstl.dbPrefixes = dbPrefixes
	fstl.itemPresent = false
	if fstl.trace {
		fmt.Printf("----------\n")
		fmt.Printf("RebuildTrie\n")
	}
	if fstl.trace {
		fmt.Printf("fstl.rl: %s\n", fstl.rl)
		fmt.Printf("fixedbits: %d\n", fixedbits)
		fmt.Printf("dbPrefixes(%d): %x\n", len(dbPrefixes), dbPrefixes)
	}
	if len(dbPrefixes) == 0 {
		return nil
	}
	fstl.tx = db
	fixedbytes := make([]int, len(fixedbits))
	masks := make([]byte, len(fixedbits))
	cutoffs := make([]int, len(fixedbits))
	for i, bits := range fixedbits {
		cutoffs[i] = bits / 4
		fixedbytes[i], masks[i] = ethdb.Bytesmask(bits)
	}
	fstl.fixedbytes = fixedbytes
	fstl.masks = masks
	fstl.cutoffs = cutoffs

	return nil
}

func (fstl *FlatDbSubTrieLoader) SetStreamReceiver(receiver StreamReceiver[SubTries]) {
	fstl.receiver = receiver
}

// iteration moves through the database buckets and creates at most
// one stream item, which is indicated by setting the field fstl.itemPresent to true
func (fstl *FlatDbSubTrieLoader) iteration(c kv.Cursor, ih *IHCursor2, first bool) error {
	var isIH, isIHSequence bool
	var minKey []byte
	var err error
	if !first {
		isIH, minKey = keyIsBeforeOrEqualDeprecated(fstl.ihK, fstl.k)
	}
	fixedbytes := fstl.fixedbytes[fstl.rangeIdx]
	cutoff := fstl.cutoffs[fstl.rangeIdx]
	dbPrefix := fstl.dbPrefixes[fstl.rangeIdx]
	mask := fstl.masks[fstl.rangeIdx]
	// Adjust rangeIdx if needed
	var cmp = -1
	for cmp != 0 {
		if minKey == nil {
			if !first {
				cmp = 1
			}
		} else if fixedbytes > 0 { // In the first iteration, we do not have valid minKey, so we skip this part
			if len(minKey) < fixedbytes {
				cmp = bytes.Compare(minKey, dbPrefix[:len(minKey)])
				if cmp == 0 {
					cmp = -1
				}
			} else {
				cmp = bytes.Compare(minKey[:fixedbytes-1], dbPrefix[:fixedbytes-1])
				if cmp == 0 {
					k1 := minKey[fixedbytes-1] & mask
					k2 := dbPrefix[fixedbytes-1] & mask
					if k1 < k2 {
						cmp = -1
					} else if k1 > k2 {
						cmp = 1
					}
				}
			}
		} else {
			cmp = 0
		}
		if fstl.trace {
			fmt.Printf("minKey %x, dbPrefix %x, cmp %d, fstl.rangeIdx %d, %x\n", minKey, dbPrefix, cmp, fstl.rangeIdx, fstl.dbPrefixes)
		}
		if cmp == 0 && fstl.itemPresent {
			return nil
		}
		if cmp < 0 {
			// This happens after we have just incremented rangeIdx or on the very first iteration
			if first && len(dbPrefix) > length.Hash {
				// Looking for storage sub-tree
				copy(fstl.accAddrHashWithInc[:], dbPrefix[:length.Hash+common.IncarnationLength])
			}

			if fstl.ihK, fstl.ihV, isIHSequence, err = ih.Seek(dbPrefix); err != nil {
				return err
			}
			if isIHSequence {
				fstl.k = libcommon.CopyBytes(fstl.ihK)
			} else {
				if fstl.k, fstl.v, err = c.Seek(dbPrefix); err != nil {
					return err
				}
				if len(dbPrefix) <= length.Hash && len(fstl.k) > length.Hash {
					// Advance past the storage to the first account
					if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
						if fstl.k, fstl.v, err = c.Seek(fstl.nextAccountKey[:]); err != nil {
							return err
						}
					} else {
						fstl.k = nil
					}
				}
			}

			isIH, minKey = keyIsBeforeOrEqualDeprecated(fstl.ihK, fstl.k)
			if fixedbytes == 0 {
				cmp = 0
			}
		} else if cmp > 0 {
			if !first {
				fstl.rangeIdx++
			}
			if !first {
				fstl.itemPresent = true
				fstl.itemType = CutoffStreamItem
				fstl.streamCutoff = cutoff
				fstl.accountKey = nil
				fstl.storageKey = nil
				fstl.storageValue = nil
				fstl.hashValue = nil
				if fstl.trace {
					fmt.Printf("Inserting cutoff %d\n", cutoff)
				}
			}
			if fstl.rangeIdx == len(fstl.dbPrefixes) {
				return nil
			}
			fixedbytes = fstl.fixedbytes[fstl.rangeIdx]
			mask = fstl.masks[fstl.rangeIdx]
			dbPrefix = fstl.dbPrefixes[fstl.rangeIdx]
			if len(dbPrefix) > length.Hash {
				// Looking for storage sub-tree
				copy(fstl.accAddrHashWithInc[:], dbPrefix[:length.Hash+common.IncarnationLength])
			}
			cutoff = fstl.cutoffs[fstl.rangeIdx]
		}
	}

	if !isIH {
		if len(fstl.k) > length.Hash && !bytes.HasPrefix(fstl.k, fstl.accAddrHashWithInc[:]) {
			if bytes.Compare(fstl.k, fstl.accAddrHashWithInc[:]) < 0 {
				// Skip all the irrelevant storage in the middle
				if fstl.k, fstl.v, err = c.Seek(fstl.accAddrHashWithInc[:]); err != nil {
					return err
				}
			} else {
				if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
					if fstl.k, fstl.v, err = c.Seek(fstl.nextAccountKey[:]); err != nil {
						return err
					}
				} else {
					fstl.k = nil
				}
			}
			return nil
		}
		fstl.itemPresent = true
		if len(fstl.k) > length.Hash {
			fstl.itemType = StorageStreamItem
			fstl.accountKey = nil
			fstl.storageKey = fstl.k // no reason to copy, because this "pointer and data" will valid until end of transaction
			fstl.hashValue = nil
			fstl.storageValue = fstl.v
			if fstl.k, fstl.v, err = c.Next(); err != nil {
				return err
			}
			if fstl.trace {
				fmt.Printf("k after storageWalker and Next: %x\n", fstl.k)
			}
		} else if len(fstl.k) > 0 {
			fstl.itemType = AccountStreamItem
			fstl.accountKey = fstl.k
			fstl.storageKey = nil
			fstl.storageValue = nil
			fstl.hashValue = nil
			if err := fstl.accountValue.DecodeForStorage(fstl.v); err != nil {
				return fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			copy(fstl.accAddrHashWithInc[:], fstl.k)
			binary.BigEndian.PutUint64(fstl.accAddrHashWithInc[32:], fstl.accountValue.Incarnation)

			// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
			// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
			// skips over all storage items
			if fstl.k, fstl.v, err = c.Seek(fstl.accAddrHashWithInc[:]); err != nil {
				return err
			}
			if fstl.trace {
				fmt.Printf("k after accountWalker and Seek: %x\n", fstl.k)
			}
			if keyIsBefore(fstl.ihK, fstl.accAddrHashWithInc[:]) {
				if fstl.ihK, fstl.ihV, _, err = ih.Seek(fstl.accAddrHashWithInc[:]); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// ih part
	if fstl.trace {
		fmt.Printf("fstl.ihK %x, fstl.accAddrHashWithInc %x\n", fstl.ihK, fstl.accAddrHashWithInc[:])
	}
	if len(fstl.ihK) > length.Hash && !bytes.HasPrefix(fstl.ihK, fstl.accAddrHashWithInc[:]) {
		if bytes.Compare(fstl.ihK, fstl.accAddrHashWithInc[:]) < 0 {
			// Skip all the irrelevant storage in the middle
			if fstl.ihK, fstl.ihV, _, err = ih.Seek(fstl.accAddrHashWithInc[:]); err != nil {
				return err
			}
		} else {
			if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
				if fstl.ihK, fstl.ihV, _, err = ih.Seek(fstl.nextAccountKey[:]); err != nil {
					return err
				}
			} else {
				fstl.ihK = nil
			}
		}
		return nil
	}
	fstl.itemPresent = true
	if len(fstl.ihK) > length.Hash {
		fstl.itemType = SHashStreamItem
		fstl.accountKey = nil
		fstl.storageKey = fstl.ihK
		fstl.hashValue = fstl.ihV
		fstl.storageValue = nil
	} else {
		fstl.itemType = AHashStreamItem
		fstl.accountKey = fstl.ihK
		fstl.storageKey = nil
		fstl.storageValue = nil
		fstl.hashValue = fstl.ihV
	}

	// skip subtree
	next, ok := kv.NextSubtree(fstl.ihK)
	if !ok { // no siblings left
		fstl.k, fstl.ihK, fstl.ihV = nil, nil, nil
		return nil
	}
	if fstl.trace {
		fmt.Printf("next: %x\n", next)
	}

	if fstl.ihK, fstl.ihV, isIHSequence, err = ih.Seek(next); err != nil {
		return err
	}
	if isIHSequence {
		fstl.k = libcommon.CopyBytes(fstl.ihK)
		return nil
	}
	if fstl.k, fstl.v, err = c.Seek(next); err != nil {
		return err
	}
	if len(next) <= length.Hash && len(fstl.k) > length.Hash {
		// Advance past the storage to the first account
		if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
			if fstl.k, fstl.v, err = c.Seek(fstl.nextAccountKey[:]); err != nil {
				return err
			}
		} else {
			fstl.k = nil
		}
	}
	if fstl.trace {
		fmt.Printf("k after next: %x\n", fstl.k)
	}
	return nil
}

func (dr *DefaultReceiver) EmptyResult() SubTries {
	return SubTries{}
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

func (dr *DefaultReceiver) Legacy() bool {
	return true
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
		if len(accountKey) == 0 {
			dr.advanceKeysStorage(storageKey, false /* terminator */)
		} else {
			if len(storageKey) == 0 {
				dr.advanceKeysStorage(accountKey, false /* terminator */)
			} else {
				key := make([]byte, len(accountKey)+len(storageKey))
				copy(key, accountKey)
				copy(key[len(accountKey):], storageKey)

				dr.advanceKeysStorage(key, false /* terminator */)
			}
		}
		if dr.currStorage.Len() > 0 {
			if err := dr.genStructStorage(); err != nil {
				return err
			}
		}
		dr.saveValueStorage(true, storageValue, hash)
	case AccountStreamItem:
		dr.advanceKeysAccount(accountKey, true /* terminator */)
		if dr.curr.Len() > 0 && !dr.wasIH {
			dr.cutoffKeysStorage(2 * (length.Hash + common.IncarnationLength))
			if dr.currStorage.Len() > 0 {
				if err := dr.genStructStorage(); err != nil {
					return err
				}
			}
			if dr.currStorage.Len() > 0 {
				if len(dr.groups) >= 2*length.Hash {
					dr.groups = dr.groups[:2*length.Hash-1]
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
			dr.cutoffKeysStorage(2 * (length.Hash + common.IncarnationLength))
			if dr.currStorage.Len() > 0 {
				if err := dr.genStructStorage(); err != nil {
					return err
				}
			}
			if dr.currStorage.Len() > 0 {
				if len(dr.groups) >= 2*length.Hash {
					dr.groups = dr.groups[:2*length.Hash-1]
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
		if cutoff >= 2*(length.Hash+common.IncarnationLength) {
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
				dr.subTries.Hashes = append(dr.subTries.Hashes, libcommon.Hash{})
			}
		} else {
			dr.cutoffKeysAccount(cutoff)
			if dr.curr.Len() > 0 && !dr.wasIH {
				dr.cutoffKeysStorage(2 * (length.Hash + common.IncarnationLength))
				if dr.currStorage.Len() > 0 {
					if err := dr.genStructStorage(); err != nil {
						return err
					}
				}
				if dr.currStorage.Len() > 0 {
					if len(dr.groups) >= 2*length.Hash {
						dr.groups = dr.groups[:2*length.Hash-1]
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
	defer trieFlatDbSubTrieLoaderTimer.ObserveDuration(time.Now())
	if len(fstl.dbPrefixes) == 0 {
		return SubTries{}, nil
	}
	c, err := fstl.tx.Cursor(kv.HashedAccounts)

	if err != nil {
		return SubTries{}, err
	}

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

	cursorDupSort, err := fstl.tx.CursorDupSort(kv.IntermediateTrieHash)

	if err != nil {
		return SubTries{}, err
	}

	ih := NewIHCursor2(NewFilterCursor2(filter, cursorDupSort))
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

func AttachRequestedCode(db kv.Getter, requests []*LoadRequestForCode) error {
	for _, req := range requests {
		codeHash := req.codeHash
		code, err := db.GetOne(kv.Code, codeHash[:])
		if err != nil {
			return err
		}
		if req.bytecode {
			if err := req.t.UpdateAccountCode(req.addrHash[:], CodeNode(code)); err != nil {
				return err
			}
		} else {
			if err := req.t.UpdateAccountCodeSize(req.addrHash[:], len(code)); err != nil {
				return err
			}
		}
	}
	return nil
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

func (dr *DefaultReceiver) Root() libcommon.Hash {
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
		dr.hashData.Hash = libcommon.BytesToHash(dr.valueStorage.Bytes())
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
	c kv.Cursor

	k, kHex, v []byte
	filter     func(k []byte) (bool, error)
}

func NewFilterCursor2(filter func(k []byte) (bool, error), c kv.Cursor) *FilterCursor2 {
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

type SubTrieAggregator struct {
	trace          bool
	wasIH          bool
	wasIHStorage   bool
	subTries       SubTries
	root           libcommon.Hash
	hc             HashCollector2
	shc            StorageHashCollector2
	currStorage    bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage    bytes.Buffer
	valueStorage   []byte // Current value to be used as the value tape for the hash builder
	hadTreeStorage bool
	hashAccount    libcommon.Hash // Current value to be used as the value tape for the hash builder
	hashStorage    libcommon.Hash // Current value to be used as the value tape for the hash builder
	curr           bytes.Buffer   // Current key for the structure generation algorithm, as well as the input tape for the hash builder
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

	// Used to construct an Account proof while calculating the tree root.
	proofRetainer ProofRetainer
	rl            *RetainList
	cutoff        bool
}

func NewSubTrieAggregator(hc HashCollector2, shc StorageHashCollector2, trace bool) *SubTrieAggregator {
	return &SubTrieAggregator{
		hb:    NewHashBuilder(false),
		hc:    hc,
		shc:   shc,
		trace: trace,
	}
}

func (r *SubTrieAggregator) SetProofRetainer(pr ProofRetainer) {
	r.proofRetainer = pr
}

func (r *SubTrieAggregator) SetRetainList(rl *RetainList) {
	r.rl = rl
}

func (r *SubTrieAggregator) RetainNothing(_ []byte) bool {
	return false
}

func (r *SubTrieAggregator) Retain(data []byte) bool {
	return r.rl.Retain(data)
}

func (r *SubTrieAggregator) EmptyResult() SubTries {
	return SubTries{}
}

func (r *SubTrieAggregator) Legacy() bool {
	return false
}

func (r *SubTrieAggregator) Receive(itemType StreamItem,
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
	//

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
			r.hb.hashStack = append(append(r.hb.hashStack, byte(80+length.Hash)), hash...)
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

				r.subTries.roots = append(r.subTries.roots, r.hb.root())
				r.subTries.Hashes = append(r.subTries.Hashes, r.hb.rootHash())
			} else {
				r.subTries.roots = append(r.subTries.roots, nil)
				r.subTries.Hashes = append(r.subTries.Hashes, libcommon.Hash{})
			}
		}

		// Used for optional GetProof calculation to trigger inclusion of the top-level node
		r.cutoff = true

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
			r.subTries.roots = append(r.subTries.roots, r.hb.root())
			r.subTries.Hashes = append(r.subTries.Hashes, r.hb.rootHash())
		} else {
			r.root = EmptyRoot
			r.subTries.roots = append(r.subTries.roots, nil)
			r.subTries.Hashes = append(r.subTries.Hashes, EmptyRoot)
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

func (r *SubTrieAggregator) Result() SubTries {
	return r.subTries
}

func (r *SubTrieAggregator) advanceKeysStorage(k []byte, terminator bool) {
	r.currStorage.Reset()
	r.currStorage.Write(r.succStorage.Bytes())
	r.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	r.succStorage.Write(k)

	if terminator {
		r.succStorage.WriteByte(16)
	}
}

func (r *SubTrieAggregator) cutoffKeysStorage(cutoff int) {
	r.currStorage.Reset()
	r.currStorage.Write(r.succStorage.Bytes())
	r.succStorage.Reset()
	//if r.currStorage.Len() > 0 {
	//r.succStorage.Write(r.currStorage.Bytes()[:cutoff-1])
	//r.succStorage.WriteByte(r.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
	//}
}

func (r *SubTrieAggregator) genStructStorage() error {
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
	var wantProof func(_ []byte) *proofElement
	if r.proofRetainer != nil {
		var fullKey [2 * (length.Hash + length.Incarnation + length.Hash)]byte
		for i, b := range r.currAccK {
			fullKey[i*2] = b / 16
			fullKey[i*2+1] = b % 16
		}
		for i, b := range binary.BigEndian.AppendUint64(nil, r.a.Incarnation) {
			fullKey[2*length.Hash+i*2] = b / 16
			fullKey[2*length.Hash+i*2+1] = b % 16
		}
		baseKeyLen := 2 * (length.Hash + length.Incarnation)
		wantProof = func(prefix []byte) *proofElement {
			copy(fullKey[baseKeyLen:], prefix)
			return r.proofRetainer.ProofElement(fullKey[:baseKeyLen+len(prefix)])
		}
	}
	r.groupsStorage, r.hasTreeStorage, r.hasHashStorage, err = GenStructStepEx(r.RetainNothing, r.currStorage.Bytes(), r.succStorage.Bytes(), r.hb, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if r.shc == nil {
			return nil
		}
		return r.shc(r.currAccK, keyHex, hasState, hasTree, hasHash, hashes, rootHash)
	}, data, r.groupsStorage, r.hasTreeStorage, r.hasHashStorage,
		r.trace,
		wantProof,
		r.cutoff,
	)
	if err != nil {
		return err
	}
	return nil
}

func (r *SubTrieAggregator) saveValueStorage(isIH, hasTree bool, v, h []byte) {
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

func (r *SubTrieAggregator) advanceKeysAccount(k []byte, terminator bool) {
	r.curr.Reset()
	r.curr.Write(r.succ.Bytes())
	r.succ.Reset()
	r.succ.Write(k)
	if terminator {
		r.succ.WriteByte(16)
	}
}

func (r *SubTrieAggregator) cutoffKeysAccount(cutoff int) {
	r.curr.Reset()
	r.curr.Write(r.succ.Bytes())
	r.succ.Reset()
	if r.curr.Len() > 0 && cutoff > 0 {
		r.succ.Write(r.curr.Bytes()[:cutoff-1])
		r.succ.WriteByte(r.curr.Bytes()[cutoff-1] + 1) // Modify last nibble before the cutoff point
	}
}

func (r *SubTrieAggregator) genStructAccount() error {
	var data GenStructStepData
	if r.wasIH {
		r.hashData.Hash = r.hashAccount
		r.hashData.HasTree = r.hadTreeAcc
		data = &r.hashData
	} else {
		r.accData.Balance.Set(&r.a.Balance)
		if !r.a.Balance.IsZero() {
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

	var wantProof func(_ []byte) *proofElement
	if r.proofRetainer != nil {
		wantProof = r.proofRetainer.ProofElement
	}
	if r.groups, r.hasTree, r.hasHash, err = GenStructStepEx(r.RetainNothing, r.curr.Bytes(), r.succ.Bytes(), r.hb, func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		if r.hc == nil {
			return nil
		}
		return r.hc(keyHex, hasState, hasTree, hasHash, hashes, rootHash)
	}, data, r.groups, r.hasTree, r.hasHash,
		//false,
		r.trace,
		wantProof,
		r.cutoff,
	); err != nil {
		return err
	}
	r.accData.FieldSet = 0
	return nil
}

func (r *SubTrieAggregator) saveValueAccount(isIH, hasTree bool, v *accounts.Account, h []byte) error {
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
