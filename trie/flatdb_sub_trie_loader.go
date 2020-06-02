package trie

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
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
		cutoff int,
		witnessSize uint64,
	) error

	Result() SubTries
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
	kv                 ethdb.KV
	nextAccountKey     [32]byte
	k, v               []byte
	ihK, ihV           []byte
	minKeyAsNibbles    bytes.Buffer

	itemPresent    bool
	itemType       StreamItem
	getWitnessSize func(prefix []byte) uint64

	// Storage item buffer
	storageKey   []byte
	storageValue []byte

	// Acount item buffer
	accountKey   []byte
	accountValue accounts.Account
	hashValue    []byte
	streamCutoff int
	witnessSize  uint64

	receiver        StreamReceiver
	defaultReceiver *DefaultReceiver
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
	witnessSize  uint64
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
func (fstl *FlatDbSubTrieLoader) Reset(db ethdb.Database, rl RetainDecider, receiverDecider RetainDecider, hc HashCollector, dbPrefixes [][]byte, fixedbits []int, trace bool) error {
	fstl.defaultReceiver.Reset(receiverDecider, hc, trace)
	fstl.receiver = fstl.defaultReceiver
	fstl.rangeIdx = 0

	fstl.minKeyAsNibbles.Reset()
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
	if hasKV, ok := db.(ethdb.HasAbstractKV); ok {
		fstl.kv = hasKV.AbstractKV()
	} else {
		return fmt.Errorf("database doest not implement AbstractKV: %T", db)
	}
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

func (fstl *FlatDbSubTrieLoader) SetStreamReceiver(receiver StreamReceiver) {
	fstl.receiver = receiver
}

// iteration moves through the database buckets and creates at most
// one stream item, which is indicated by setting the field fstl.itemPresent to true
func (fstl *FlatDbSubTrieLoader) iteration(c, ih ethdb.Cursor, first bool) error {
	var isIH bool
	var minKey []byte
	var err error
	if !first {
		isIH, minKey = keyIsBefore(fstl.ihK, fstl.k)
	}
	fixedbytes := fstl.fixedbytes[fstl.rangeIdx]
	cutoff := fstl.cutoffs[fstl.rangeIdx]
	dbPrefix := fstl.dbPrefixes[fstl.rangeIdx]
	mask := fstl.masks[fstl.rangeIdx]
	// Adjust rangeIdx if needed
	var cmp int = -1
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
			if first && len(dbPrefix) > common.HashLength {
				// Looking for storage sub-tree
				copy(fstl.accAddrHashWithInc[:], dbPrefix[:common.HashLength+common.IncarnationLength])
			}
			if fstl.k, fstl.v, err = c.SeekTo(dbPrefix); err != nil {
				return err
			}
			if len(dbPrefix) <= common.HashLength && len(fstl.k) > common.HashLength {
				// Advance past the storage to the first account
				if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
					if fstl.k, fstl.v, err = c.SeekTo(fstl.nextAccountKey[:]); err != nil {
						return err
					}
				} else {
					fstl.k = nil
				}
			}
			if fstl.ihK, fstl.ihV, err = ih.SeekTo(dbPrefix); err != nil {
				return err
			}
			if len(dbPrefix) <= common.HashLength && len(fstl.ihK) > common.HashLength {
				// Advance to the first account
				if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
					if fstl.ihK, fstl.ihV, err = ih.SeekTo(fstl.nextAccountKey[:]); err != nil {
						return err
					}
				} else {
					fstl.ihK = nil
				}
			}
			isIH, minKey = keyIsBefore(fstl.ihK, fstl.k)
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
			if len(dbPrefix) > common.HashLength {
				// Looking for storage sub-tree
				copy(fstl.accAddrHashWithInc[:], dbPrefix[:common.HashLength+common.IncarnationLength])
			}
			cutoff = fstl.cutoffs[fstl.rangeIdx]
		}
	}

	if !isIH {
		if len(fstl.k) > common.HashLength && !bytes.HasPrefix(fstl.k, fstl.accAddrHashWithInc[:]) {
			if bytes.Compare(fstl.k, fstl.accAddrHashWithInc[:]) < 0 {
				// Skip all the irrelevant storage in the middle
				if fstl.k, fstl.v, err = c.SeekTo(fstl.accAddrHashWithInc[:]); err != nil {
					return err
				}
			} else {
				if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
					if fstl.k, fstl.v, err = c.SeekTo(fstl.nextAccountKey[:]); err != nil {
						return err
					}
				} else {
					fstl.k = nil
				}
			}
			return nil
		}
		fstl.itemPresent = true
		if len(fstl.k) > common.HashLength {
			fstl.itemType = StorageStreamItem
			fstl.storageKey = fstl.k
			fstl.hashValue = nil
			fstl.storageValue = fstl.v
			if fstl.k, fstl.v, err = c.Next(); err != nil {
				return err
			}
			if fstl.trace {
				fmt.Printf("k after storageWalker and Next: %x\n", fstl.k)
			}
		} else {
			fstl.itemType = AccountStreamItem
			fstl.accountKey = fstl.k
			fstl.storageKey = nil
			fstl.hashValue = nil
			if err := fstl.accountValue.DecodeForStorage(fstl.v); err != nil {
				return fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			copy(fstl.accAddrHashWithInc[:], fstl.k)
			binary.BigEndian.PutUint64(fstl.accAddrHashWithInc[32:], ^fstl.accountValue.Incarnation)
			// Now we know the correct incarnation of the account, an
			//d we can skip all irrelevant storage records
			// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
			// skips over all storage items
			if fstl.k, fstl.v, err = c.SeekTo(fstl.accAddrHashWithInc[:]); err != nil {
				return err
			}
			if fstl.trace {
				fmt.Printf("k after accountWalker and SeekTo: %x\n", fstl.k)
			}
			if !bytes.HasPrefix(fstl.ihK, fstl.accAddrHashWithInc[:]) {
				if fstl.ihK, fstl.ihV, err = ih.SeekTo(fstl.accAddrHashWithInc[:]); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// ih part
	fstl.minKeyAsNibbles.Reset()
	keyToNibbles(minKey, &fstl.minKeyAsNibbles)

	if fstl.minKeyAsNibbles.Len() < cutoff {
		if fstl.ihK, fstl.ihV, err = ih.Next(); err != nil {
			return err
		} // go to children, not to sibling
		return nil
	}

	retain := fstl.rl.Retain(fstl.minKeyAsNibbles.Bytes())
	if fstl.trace {
		fmt.Printf("fstl.rl.Retain(%x)=%t\n", fstl.minKeyAsNibbles.Bytes(), retain)
	}
	retain = true

	if retain { // can't use ih as is, need go to children
		if fstl.ihK, fstl.ihV, err = ih.Next(); err != nil {
			return err
		} // go to children, not to sibling
		return nil
	}

	if fstl.trace {
		fmt.Printf("fstl.ihK %x, fstl.accAddrHashWithInc %x\n", fstl.ihK, fstl.accAddrHashWithInc[:])
	}
	if len(fstl.ihK) > common.HashLength && !bytes.HasPrefix(fstl.ihK, fstl.accAddrHashWithInc[:]) {
		if bytes.Compare(fstl.ihK, fstl.accAddrHashWithInc[:]) < 0 {
			// Skip all the irrelevant storage in the middle
			if fstl.ihK, fstl.ihV, err = ih.SeekTo(fstl.accAddrHashWithInc[:]); err != nil {
				return err
			}
		} else {
			if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
				if fstl.ihK, fstl.ihV, err = ih.SeekTo(fstl.nextAccountKey[:]); err != nil {
					return err
				}
			} else {
				fstl.ihK = nil
			}
		}
		return nil
	}
	fstl.itemPresent = true
	if len(fstl.ihK) > common.HashLength {
		fstl.itemType = SHashStreamItem
		fstl.storageKey = fstl.ihK
		fstl.hashValue = fstl.ihV
		fstl.storageValue = nil
		fstl.witnessSize = fstl.getWitnessSize(fstl.ihK)
	} else {
		fstl.itemType = AHashStreamItem
		fstl.accountKey = fstl.ihK
		fstl.storageKey = nil
		fstl.hashValue = fstl.ihV
		fstl.witnessSize = fstl.getWitnessSize(fstl.ihK)
	}

	// skip subtree
	next, ok := dbutils.NextSubtree(fstl.ihK)
	if !ok { // no siblings left
		if !retain { // last sub-tree was taken from IH, then nothing to look in the main bucket. Can stop.
			fstl.k = nil
			fstl.ihK = nil
			return nil
		}
		fstl.ihK, fstl.ihV = nil, nil
		return nil
	}
	if fstl.trace {
		fmt.Printf("next: %x\n", next)
	}

	if !bytes.HasPrefix(fstl.k, next) {
		if fstl.k, fstl.v, err = c.SeekTo(next); err != nil {
			return err
		}
	}
	if len(next) <= common.HashLength && len(fstl.k) > common.HashLength {
		// Advance past the storage to the first account
		if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
			if fstl.k, fstl.v, err = c.SeekTo(fstl.nextAccountKey[:]); err != nil {
				return err
			}
		} else {
			fstl.k = nil
		}
	}
	if fstl.trace {
		fmt.Printf("k after next: %x\n", fstl.k)
	}
	if !bytes.HasPrefix(fstl.ihK, next) {
		if fstl.ihK, fstl.ihV, err = ih.SeekTo(next); err != nil {
			return err
		}
	}
	if len(next) <= common.HashLength && len(fstl.ihK) > common.HashLength {
		// Advance past the storage to the first account
		if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
			if fstl.ihK, fstl.ihV, err = ih.SeekTo(fstl.nextAccountKey[:]); err != nil {
				return err
			}
		} else {
			fstl.ihK = nil
		}
	}
	if fstl.trace {
		fmt.Printf("ihK after next: %x\n", fstl.ihK)
	}
	return nil
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
	cutoff int,
	witnessSize uint64,
) error {
	switch itemType {
	case StorageStreamItem:
		dr.advanceKeysStorage(storageKey, true /* terminator */)
		if dr.currStorage.Len() > 0 {
			if err := dr.genStructStorage(); err != nil {
				return err
			}
		}
		dr.saveValueStorage(false, storageValue, hash, witnessSize)
	case SHashStreamItem:
		dr.advanceKeysStorage(storageKey, false /* terminator */)
		if dr.currStorage.Len() > 0 {
			if err := dr.genStructStorage(); err != nil {
				return err
			}
		}
		dr.saveValueStorage(true, storageValue, hash, witnessSize)
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
		if err := dr.saveValueAccount(false, accountValue, hash, witnessSize); err != nil {
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
		if err := dr.saveValueAccount(true, accountValue, hash, witnessSize); err != nil {
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
			dr.subTries.roots = append(dr.subTries.roots, dr.hb.root())
			dr.subTries.Hashes = append(dr.subTries.Hashes, dr.hb.rootHash())
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
	if err := fstl.kv.View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Bucket(dbutils.CurrentStateBucket).Cursor()
		ih := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor()
		iwl := tx.Bucket(dbutils.IntermediateWitnessSizeBucket).Cursor()
		fstl.getWitnessSize = func(prefix []byte) uint64 {
			if !debug.IsTrackWitnessSizeEnabled() {
				return 0
			}
			k, v, err := iwl.SeekTo(prefix)
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(k, prefix) {
				panic(fmt.Sprintf("IH and WitnessSize buckets must have same keys set: %x, %x", k, prefix))
			}
			return binary.BigEndian.Uint64(v)
		}
		if err := fstl.iteration(c, ih, true /* first */); err != nil {
			return err
		}
		for fstl.rangeIdx < len(fstl.dbPrefixes) {
			for !fstl.itemPresent {
				if err := fstl.iteration(c, ih, false /* first */); err != nil {
					return err
				}
			}
			if fstl.itemPresent {
				if err := fstl.receiver.Receive(fstl.itemType, fstl.accountKey, fstl.storageKey, &fstl.accountValue, fstl.storageValue, fstl.hashValue, fstl.streamCutoff, fstl.witnessSize); err != nil {
					return err
				}
				fstl.itemPresent = false
			}
		}
		return nil
	}); err != nil {
		return SubTries{}, err
	}
	return fstl.receiver.Result(), nil
}

func (fstl *FlatDbSubTrieLoader) AttachRequestedCode(db ethdb.Getter, requests []*LoadRequestForCode) error {
	for _, req := range requests {
		codeHash := req.codeHash
		code, err := db.Get(dbutils.CodeBucket, codeHash[:])
		if err != nil {
			return err
		}
		if req.bytecode {
			if err := req.t.UpdateAccountCode(req.addrHash[:], codeNode(code)); err != nil {
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
		dr.hashData.WitnessSize = dr.witnessSize
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

func (dr *DefaultReceiver) saveValueStorage(isIH bool, v, h []byte, witnessSize uint64) {
	// Remember the current value
	dr.wasIHStorage = isIH
	dr.valueStorage.Reset()
	if isIH {
		dr.valueStorage.Write(h)
		dr.witnessSize = witnessSize
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
		dr.hashData.WitnessSize = dr.witnessSize
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

func (dr *DefaultReceiver) saveValueAccount(isIH bool, v *accounts.Account, h []byte, witnessSize uint64) error {
	dr.wasIH = isIH
	if isIH {
		dr.value.Reset()
		dr.value.Write(h)
		dr.witnessSize = witnessSize
		return nil
	}
	dr.a.Copy(v)
	// Place code on the stack first, the storage will follow
	if !dr.a.IsEmptyCodeHash() {
		// the first item ends up deepest on the stack, the second item - on the top
		dr.accData.FieldSet |= AccountFieldCodeOnly
		if err := dr.hb.hash(dr.a.CodeHash[:], 0); err != nil {
			return err
		}
	}
	return nil
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
func keyIsBefore(k1, k2 []byte) (bool, []byte) {
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
