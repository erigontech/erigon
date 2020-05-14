package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

var (
	trieFlatDbSubTrieLoaderTimer = metrics.NewRegisteredTimer("trie/subtrieloader/flatdb", nil)
)

type FlatDbSubTrieLoader struct {
	rl       *RetainList
	curr     bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ     bytes.Buffer
	value    bytes.Buffer // Current value to be used as the value tape for the hash builder
	groups   []uint16
	hb       *HashBuilder
	rangeIdx   int
	a        accounts.Account
	leafData GenStructStepLeafData
	accData  GenStructStepAccountData

	subTries SubTries

	wasIH        bool
	wasIHStorage bool
	hashData     GenStructStepHashData
	trace        bool

	currStorage   bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage   bytes.Buffer
	valueStorage  bytes.Buffer // Current value to be used as the value tape for the hash builder
	groupsStorage []uint16

	accAddrHashWithInc [40]byte // Concatenation of addrHash of the currently build account with its incarnation encoding
	dbPrefixes [][]byte
	fixedbytes []int
	masks []byte
	cutoffs []int
	tx *bolt.Tx // Manually managed read-only transaction (rolled back when iterator is exhausted)
	c *bolt.Cursor
	ih *bolt.Cursor
	nextAccountKey [32]byte
	k, v []byte
	ihK, ihV []byte
	minKeyAsNibbles bytes.Buffer

	// Storage item buffer
	storageItemPresent bool
	storageIsHash bool
	storageKeyPart1 []byte
	storageKeyPart2 []byte
	storageHash []byte
	storageValue []byte

	// Acount item buffer
	accountItemPresent bool
	accountIsHash bool
	accountKey []byte
	accountHash []byte
	accountValue []byte
}

func NewFlatDbSubTrieLoader() *FlatDbSubTrieLoader {
	fstl := &FlatDbSubTrieLoader{
		hb:                 NewHashBuilder(false),
	}
	return fstl
}

// Reset prepares the loader for reuse
func (fstl *FlatDbSubTrieLoader) Reset(db ethdb.Database, rl *RetainList, dbPrefixes [][]byte, fixedbits []int, trace bool) error {
	fstl.rangeIdx = 0
	fstl.rl = NewRetainList(0)
	fstl.curr.Reset()
	fstl.succ.Reset()
	fstl.value.Reset()
	fstl.groups = fstl.groups[:0]
	fstl.a.Reset()
	fstl.hb.Reset()
	fstl.wasIH = false

	fstl.currStorage.Reset()
	fstl.succStorage.Reset()
	fstl.valueStorage.Reset()
	fstl.minKeyAsNibbles.Reset()
	fstl.groupsStorage = fstl.groupsStorage[:0]
	fstl.wasIHStorage = false
	fstl.subTries = SubTries{}
	fstl.trace = trace
	fstl.hb.trace = trace
	fstl.rl = rl
	fstl.dbPrefixes = dbPrefixes
	fstl.storageItemPresent = false
	fstl.accountItemPresent = false
	if fstl.trace {
		fmt.Printf("----------\n")
		fmt.Printf("RebuildTrie\n")
	}
	if fstl.trace {
		fmt.Printf("fstl.rl: %x\n", fstl.rl.hexes)
		fmt.Printf("fixedbits: %d\n", fixedbits)
		fmt.Printf("dbPrefixes(%d): %x\n", len(dbPrefixes), dbPrefixes)
	}
	if len(dbPrefixes) == 0 {
		return nil
	}
	var boltDB *bolt.DB
	if hasBolt, ok := db.(ethdb.HasKV); ok {
		boltDB = hasBolt.KV()
	}
	if boltDB == nil {
		return fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	// Create manually managed read transaction
	tx, err := boltDB.Begin(false)
	if err != nil {
		return fmt.Errorf("opening bolt tx: %v", err)
	}
	fstl.tx = tx
	fixedbytes := make([]int, len(fixedbits))
	masks := make([]byte, len(fixedbits))
	cutoffs := make([]int, len(fixedbits))
	for i, bits := range fixedbits {
		if bits >= 256 /* addrHash */ +64 /* incarnation */ {
			cutoffs[i] = bits/4 - 16 // Remove incarnation
		} else {
			cutoffs[i] = bits / 4
		}
		fixedbytes[i], masks[i] = ethdb.Bytesmask(bits)
	}
	fstl.fixedbytes = fixedbytes
	fstl.masks = masks
	fstl.cutoffs = cutoffs
	dbPrefix :=  dbPrefixes[0]
	if len(dbPrefix) > common.HashLength {
		// Looking for storage sub-tree
		copy(fstl.accAddrHashWithInc[:], dbPrefix[:common.HashLength+common.IncarnationLength])
	}

	ihBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
	if ihBucket != nil {
		fstl.ih = ihBucket.Cursor()
	} else {
		fstl.ih = nil
	}
	fstl.c = tx.Bucket(dbutils.CurrentStateBucket).Cursor()

	fstl.k, fstl.v = fstl.c.Seek(dbPrefix)
	if len(dbPrefix) <= common.HashLength && len(fstl.k) > common.HashLength {
		// Advance past the storage to the first account
		if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
			fstl.k, fstl.v = fstl.c.SeekTo(fstl.nextAccountKey[:])
		} else {
			fstl.k = nil
		}
	}
	if fstl.trace {
		fmt.Printf("c.Seek(%x) = %x\n", dbPrefix, fstl.k)
	}
	if fstl.ih != nil {
		fstl.ihK, fstl.ihV = fstl.ih.Seek(dbPrefix)
		if len(dbPrefix) <= common.HashLength && len(fstl.ihK) > common.HashLength {
			// Advance past the storage to the first account
			if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
				fstl.ihK, fstl.ihV = fstl.ih.SeekTo(fstl.nextAccountKey[:])
			} else {
				fstl.ihK = nil
			}
		}
	}
	return nil
}

func (fstl *FlatDbSubTrieLoader) iteration() error {
	isIH, minKey := keyIsBefore(fstl.ihK, fstl.k)
	if fstl.trace {
		fmt.Printf("keyIsBefore(%x,%x)=%t,%x\n", fstl.ihK, fstl.k, isIH, minKey)
	}
	fixedbytes := fstl.fixedbytes[fstl.rangeIdx]
	cutoff := fstl.cutoffs[fstl.rangeIdx]
	if fixedbytes > 0 {
		dbPrefix := fstl.dbPrefixes[fstl.rangeIdx]
		mask := fstl.masks[fstl.rangeIdx]
		// Adjust rangeIdx if needed
		cmp := int(-1)
		for cmp != 0 {
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
			if cmp < 0 {
				// This happens after we have just incremented rangeIdx
				fstl.k, fstl.v = fstl.c.SeekTo(dbPrefix)
				if len(dbPrefix) <= common.HashLength && len(fstl.k) > common.HashLength {
					// Advance past the storage to the first account
					if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
						fstl.k, fstl.v = fstl.c.SeekTo(fstl.nextAccountKey[:])
					} else {
						fstl.k = nil
					}
				}
				if fstl.ih != nil {
					fstl.ihK, fstl.ihV = fstl.ih.SeekTo(dbPrefix)
					if len(dbPrefix) <= common.HashLength && len(fstl.ihK) > common.HashLength {
						// Advance to the first account
						if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
							fstl.ihK, fstl.ihV = fstl.ih.SeekTo(fstl.nextAccountKey[:])
						} else {
							fstl.ihK = nil
						}
					}
				}
				if fstl.k == nil && fstl.ihK == nil {
					return nil
				}
				isIH, minKey = keyIsBefore(fstl.ihK, fstl.k)
			} else if cmp > 0 {
				fstl.rangeIdx++
				if fstl.rangeIdx == len(fstl.dbPrefixes) {
					fstl.k = nil
					fstl.ihK = nil
					return nil
				}
				if err := fstl.finaliseRoot(cutoff); err != nil {
					return err
				}
				fixedbytes = fstl.fixedbytes[fstl.rangeIdx]
				mask = fstl.masks[fstl.rangeIdx]
				dbPrefix = fstl.dbPrefixes[fstl.rangeIdx]
				if len(dbPrefix) > common.HashLength {
					// Looking for storage sub-tree
					copy(fstl.accAddrHashWithInc[:], dbPrefix[:common.HashLength+common.IncarnationLength])
				}
				cutoff = fstl.cutoffs[fstl.rangeIdx]
				fstl.hb.Reset()
				fstl.wasIH = false
				fstl.wasIHStorage = false
				fstl.groups = fstl.groups[:0]
				fstl.groupsStorage = fstl.groupsStorage[:0]
				fstl.curr.Reset()
				fstl.succ.Reset()
				fstl.currStorage.Reset()
				fstl.succStorage.Reset()
			}
		}
	}

	if !isIH {
		if len(fstl.k) > common.HashLength && !bytes.HasPrefix(fstl.k, fstl.accAddrHashWithInc[:]) {
			if bytes.Compare(fstl.k, fstl.accAddrHashWithInc[:]) < 0 {
				// Skip all the irrelevant storage in the middle
				fstl.k, fstl.v = fstl.c.SeekTo(fstl.accAddrHashWithInc[:])
			} else {
				if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
					fstl.k, fstl.v = fstl.c.SeekTo(fstl.nextAccountKey[:])
				} else {
					fstl.k = nil
				}
			}
			return nil
		}
		if len(fstl.k) > common.HashLength {
			fstl.storageItemPresent = true
			fstl.storageIsHash = false
			if len(fstl.k) >= common.HashLength {
				fstl.storageKeyPart1 = fstl.k[:common.HashLength]
				if len(fstl.k) >= common.HashLength + common.IncarnationLength {
					fstl.storageKeyPart2 = fstl.k[common.HashLength + common.IncarnationLength:]
				} else {
					fstl.storageKeyPart2 = nil
				}
			} else {
				fstl.storageKeyPart1 = fstl.k
				fstl.storageKeyPart2 = nil
			}
			fstl.storageHash = nil
			fstl.storageValue = fstl.v
			//if err := fstl.WalkerStorage(false, fstl.rangeIdx, fstl.k, fstl.v); err != nil {
			//	return err
			//}
			fstl.k, fstl.v = fstl.c.Next()
			if fstl.trace {
				fmt.Printf("k after storageWalker and Next: %x\n", fstl.k)
			}
		} else {
			fstl.accountItemPresent = true
			fstl.accountIsHash = false
			fstl.accountKey = fstl.k
			fstl.accountValue = fstl.v
			if err := fstl.a.DecodeForStorage(fstl.v); err != nil {
				return fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			copy(fstl.accAddrHashWithInc[:], fstl.k)
			binary.BigEndian.PutUint64(fstl.accAddrHashWithInc[32:], ^fstl.a.Incarnation)
			//if err := fstl.WalkerAccount(false, fstl.rangeIdx, fstl.k, fstl.v); err != nil {
			//	return err
			//}
			// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
			// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
			// skips over all storage items
			fstl.k, fstl.v = fstl.c.SeekTo(fstl.accAddrHashWithInc[:])
			if fstl.trace {
				fmt.Printf("k after accountWalker and SeekTo: %x\n", fstl.k)
			}
			if fstl.ih != nil {
				if !bytes.HasPrefix(fstl.ihK, fstl.accAddrHashWithInc[:]) {
					fstl.ihK, fstl.ihV = fstl.ih.SeekTo(fstl.accAddrHashWithInc[:])
				}
			}
		}
		return nil
	}

	// ih part
	fstl.minKeyAsNibbles.Reset()
	keyToNibblesWithoutInc(minKey, &fstl.minKeyAsNibbles)

	if fstl.minKeyAsNibbles.Len() < cutoff {
		fstl.ihK, fstl.ihV = fstl.ih.Next() // go to children, not to sibling
		return nil
	}

	retain := fstl.rl.Retain(fstl.minKeyAsNibbles.Bytes())
	if fstl.trace {
		fmt.Printf("fstl.rl.Retain(%x)=%t\n", fstl.minKeyAsNibbles.Bytes(), retain)
	}

	if retain { // can't use ih as is, need go to children
		fstl.ihK, fstl.ihV = fstl.ih.Next() // go to children, not to sibling
		return nil
	}

	if len(fstl.ihK) > common.HashLength && !bytes.HasPrefix(fstl.ihK, fstl.accAddrHashWithInc[:]) {
		if bytes.Compare(fstl.ihK, fstl.accAddrHashWithInc[:]) < 0 {
			// Skip all the irrelevant storage in the middle
			fstl.ihK, fstl.ihV = fstl.ih.SeekTo(fstl.accAddrHashWithInc[:])
		} else {
			if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
				fstl.ihK, fstl.ihV = fstl.ih.SeekTo(fstl.nextAccountKey[:])
			} else {
				fstl.ihK = nil
			}
		}
		return nil
	}
	if len(fstl.ihK) > common.HashLength {
		fstl.storageItemPresent = true
		fstl.storageIsHash = true
		if len(fstl.ihK) >= common.HashLength {
			fstl.storageKeyPart1 = fstl.ihK[:common.HashLength]
			if len(fstl.ihK) >= common.HashLength + common.IncarnationLength {
				fstl.storageKeyPart2 = fstl.ihK[common.HashLength + common.IncarnationLength:]
			} else {
				fstl.storageKeyPart2 = nil
			}
		} else {
			fstl.storageKeyPart1 = fstl.ihK
			fstl.storageKeyPart2 = nil
		}
		fstl.storageHash = nil
		fstl.storageValue = fstl.ihV
		//if err := fstl.WalkerStorage(true, fstl.rangeIdx, fstl.ihK, fstl.ihV); err != nil {
		//	return fmt.Errorf("storageWalker err: %w", err)
		//}
	} else {
		fstl.accountItemPresent = true
		fstl.accountIsHash = true
		fstl.accountKey = fstl.ihK
		fstl.accountValue = fstl.ihV
		//if err := fstl.WalkerAccount(true, fstl.rangeIdx, fstl.ihK, fstl.ihV); err != nil {
		//	return fmt.Errorf("accWalker err: %w", err)
		//}
	}

	// skip subtree
	next, ok := nextSubtree(fstl.ihK)
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
		fstl.k, fstl.v = fstl.c.SeekTo(next)
	}
	if len(next) <= common.HashLength && len(fstl.k) > common.HashLength {
		// Advance past the storage to the first account
		if nextAccount(fstl.k, fstl.nextAccountKey[:]) {
			fstl.k, fstl.v = fstl.c.SeekTo(fstl.nextAccountKey[:])
		} else {
			fstl.k = nil
		}
	}
	if fstl.trace {
		fmt.Printf("k after next: %x\n", fstl.k)
	}
	if !bytes.HasPrefix(fstl.ihK, next) {
		fstl.ihK, fstl.ihV = fstl.ih.SeekTo(next)
	}
	if len(next) <= common.HashLength && len(fstl.ihK) > common.HashLength {
		// Advance past the storage to the first account
		if nextAccount(fstl.ihK, fstl.nextAccountKey[:]) {
			fstl.ihK, fstl.ihV = fstl.ih.SeekTo(fstl.nextAccountKey[:])
		} else {
			fstl.ihK = nil
		}
	}
	if fstl.trace {
		fmt.Printf("ihK after next: %x\n", fstl.ihK)
	}
	return nil
}

// cutoff specifies how many nibbles have to be cut from the beginning of the storage keys
// to fit the insertion point.
// We create a fake branch node at the cutoff point by modifying the last
// nibble of the `succ` key
func (fstl *FlatDbSubTrieLoader) finaliseRoot(cutoff int) error {
	if fstl.trace {
		fmt.Printf("finaliseRoot(%d)\n", cutoff)
	}
	if cutoff >= 2*common.HashLength {
		// if only storage resolution required, then no account records
		if ok, err := fstl.finaliseStorageRoot(cutoff); err == nil {
			if ok {
				fstl.subTries.roots = append(fstl.subTries.roots, fstl.hb.root())
				fstl.subTries.Hashes = append(fstl.subTries.Hashes, fstl.hb.rootHash())
			} else {
				fstl.subTries.roots = append(fstl.subTries.roots, nil)
				fstl.subTries.Hashes = append(fstl.subTries.Hashes, common.Hash{})
			}
		} else {
			return err
		}
		return nil
	}
	fstl.curr.Reset()
	fstl.curr.Write(fstl.succ.Bytes())
	fstl.succ.Reset()
	if fstl.curr.Len() > 0 {
		if cutoff > 0 {
			fstl.succ.Write(fstl.curr.Bytes()[:cutoff-1])
			fstl.succ.WriteByte(fstl.curr.Bytes()[cutoff-1] + 1) // Modify last nibble before the cutoff point
		}
		var data GenStructStepData
		if fstl.wasIH {
			fstl.hashData.Hash = common.BytesToHash(fstl.value.Bytes())
			data = &fstl.hashData
		} else {
			if ok, err := fstl.finaliseStorageRoot(2 * common.HashLength); err == nil {
				if ok {
					// There are some storage items
					fstl.accData.FieldSet |= AccountFieldStorageOnly
				}
			} else {
				return err
			}
			fstl.wasIHStorage = false
			fstl.accData.Balance.Set(&fstl.a.Balance)
			if fstl.a.Balance.Sign() != 0 {
				fstl.accData.FieldSet |= AccountFieldBalanceOnly
			}
			fstl.accData.Nonce = fstl.a.Nonce
			if fstl.a.Nonce != 0 {
				fstl.accData.FieldSet |= AccountFieldNonceOnly
			}
			fstl.accData.Incarnation = fstl.a.Incarnation
			data = &fstl.accData
		}
		var err error
		if fstl.groups, err = GenStructStep(fstl.rl.Retain, fstl.curr.Bytes(), fstl.succ.Bytes(), fstl.hb, data, fstl.groups, false); err != nil {
			return err
		}
		fstl.accData.FieldSet = 0
	}
	fstl.groups = fstl.groups[:0]
	fstl.subTries.roots = append(fstl.subTries.roots, fstl.hb.root())
	fstl.subTries.Hashes = append(fstl.subTries.Hashes, fstl.hb.rootHash())
	return nil
}

// cutoff specifies how many nibbles have to be cut from the beginning of the storage keys
// to fit the insertion point. For entire storage subtrie of an account, the cutoff
// would be the the length (in nibbles) of adress Hash.
// For resolver requests that asks for a part of some
// contract's storage, cutoff point will be deeper than that.
// We create a fake branch node at the cutoff point by modifying the last
// nibble of the `succ` key
func (fstl *FlatDbSubTrieLoader) finaliseStorageRoot(cutoff int) (bool, error) {
	if fstl.trace {
		fmt.Printf("finaliseStorageRoot(%d), succStorage.Len() = %d\n", cutoff, fstl.succStorage.Len())
	}
	fstl.currStorage.Reset()
	fstl.currStorage.Write(fstl.succStorage.Bytes())
	fstl.succStorage.Reset()
	if fstl.currStorage.Len() > 0 {
		fstl.succStorage.Write(fstl.currStorage.Bytes()[:cutoff-1])
		fstl.succStorage.WriteByte(fstl.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
		var data GenStructStepData
		if fstl.wasIHStorage {
			fstl.hashData.Hash = common.BytesToHash(fstl.valueStorage.Bytes())
			data = &fstl.hashData
		} else {
			fstl.leafData.Value = rlphacks.RlpSerializableBytes(fstl.valueStorage.Bytes())
			data = &fstl.leafData
		}
		var err error
		fstl.groupsStorage, err = GenStructStep(fstl.rl.Retain, fstl.currStorage.Bytes(), fstl.succStorage.Bytes(), fstl.hb, data, fstl.groupsStorage, false)
		if err != nil {
			return false, err
		}
		fstl.groupsStorage = fstl.groupsStorage[:0]
		fstl.currStorage.Reset()
		fstl.succStorage.Reset()
		fstl.wasIHStorage = false
		if fstl.trace {
			fmt.Printf("storage root: %x\n", fstl.hb.rootHash())
		}
		return true, nil
	}
	return false, nil
}

func (fstl *FlatDbSubTrieLoader) LoadSubTries() (SubTries, error) {
	if len(fstl.dbPrefixes) == 0 {
		return SubTries{}, nil
	}
	for fstl.k != nil || fstl.ihK != nil {
		for (fstl.k != nil || fstl.ihK != nil) && !fstl.accountItemPresent && !fstl.storageItemPresent {
			if err := fstl.iteration(); err != nil {
				return fstl.subTries, err
			}
		}
		if fstl.storageItemPresent {
			if err := fstl.WalkerStorage(fstl.storageIsHash, fstl.rangeIdx, fstl.storageKeyPart1, fstl.storageKeyPart2, fstl.storageValue); err != nil {
				return fstl.subTries, err
			}
			fstl.storageItemPresent = false
		}
		if fstl.accountItemPresent {
			if err := fstl.WalkerAccount(fstl.accountIsHash, fstl.rangeIdx, fstl.accountKey, fstl.accountValue); err != nil {
				return fstl.subTries, err
			}
			fstl.accountItemPresent = false
		}
	}
	if fstl.tx != nil {
		fstl.tx.Rollback()
	}
	if err := fstl.finaliseRoot(fstl.cutoffs[len(fstl.cutoffs)-1]); err != nil {
		fmt.Println("Err in finalize root, writing down resolve params")
		fmt.Printf("fstl.rs: %x\n", fstl.rl.hexes)
		fmt.Printf("fixedbytes: %d\n", fstl.fixedbytes)
		fmt.Printf("masks: %b\n", fstl.masks)
		fmt.Printf("dbPrefixes: %x\n", fstl.dbPrefixes)
		return fstl.subTries, fmt.Errorf("error in finaliseRoot %w", err)
	}
	return fstl.subTries, nil
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

type walker func(isIH bool, rangeIdx int, k, v []byte) error

func keyToNibbles(k []byte, w io.ByteWriter) {
	for _, b := range k {
		//nolint:errcheck
		w.WriteByte(b / 16)
		//nolint:errcheck
		w.WriteByte(b % 16)
	}
}

func keyToNibblesWithoutInc(k []byte, w io.ByteWriter) {
	// Transform k to nibbles, but skip the incarnation part in the middle
	for i, b := range k {
		if i == common.HashLength {
			break
		}
		//nolint:errcheck
		w.WriteByte(b / 16)
		//nolint:errcheck
		w.WriteByte(b % 16)
	}
	if len(k) > common.HashLength+common.IncarnationLength {
		for _, b := range k[common.HashLength+common.IncarnationLength:] {
			//nolint:errcheck
			w.WriteByte(b / 16)
			//nolint:errcheck
			w.WriteByte(b % 16)
		}
	}
}

func (fstl *FlatDbSubTrieLoader) WalkerStorage(isIH bool, rangeIdx int, kPart1, kPart2, v []byte) error {
	if fstl.trace {
		fmt.Printf("WalkerStorage: isIH=%v rangeIdx=%d keyPart1=%x keyPart2=%x value=%x\n", isIH, rangeIdx, kPart1, kPart2, v)
	}

	fstl.currStorage.Reset()
	fstl.currStorage.Write(fstl.succStorage.Bytes())
	fstl.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	keyToNibbles(kPart1, &fstl.succStorage)
	keyToNibbles(kPart2, &fstl.succStorage)

	if !isIH {
		fstl.succStorage.WriteByte(16)
	}

	if fstl.currStorage.Len() > 0 {
		var err error
		var data GenStructStepData
		if fstl.trace {
			fmt.Printf("fstl.wasIHStorage=%t\n", fstl.wasIHStorage)
		}
		if fstl.wasIHStorage {
			fstl.hashData.Hash = common.BytesToHash(fstl.valueStorage.Bytes())
			data = &fstl.hashData
		} else {
			fstl.leafData.Value = rlphacks.RlpSerializableBytes(fstl.valueStorage.Bytes())
			data = &fstl.leafData
		}
		fstl.groupsStorage, err = GenStructStep(fstl.rl.Retain, fstl.currStorage.Bytes(), fstl.succStorage.Bytes(), fstl.hb, data, fstl.groupsStorage, false)
		if err != nil {
			return err
		}
	}
	// Remember the current key and value
	fstl.wasIHStorage = isIH
	fstl.valueStorage.Reset()
	fstl.valueStorage.Write(v)

	return nil
}

// Walker - k, v - shouldn't be reused in the caller's code
func (fstl *FlatDbSubTrieLoader) WalkerAccount(isIH bool, rangeIdx int, k, v []byte) error {
	if fstl.trace {
		fmt.Printf("WalkerAccount: isIH=%v rangeIdx=%d key=%x value=%x\n", isIH, rangeIdx, k, v)
	}
	fstl.curr.Reset()
	fstl.curr.Write(fstl.succ.Bytes())
	fstl.succ.Reset()
	for _, b := range k {
		fstl.succ.WriteByte(b / 16)
		fstl.succ.WriteByte(b % 16)
	}
	if !isIH {
		fstl.succ.WriteByte(16)
	}

	if fstl.curr.Len() > 0 {
		var data GenStructStepData
		if fstl.wasIH {
			fstl.hashData.Hash = common.BytesToHash(fstl.value.Bytes())
			data = &fstl.hashData
		} else {
			if ok, err := fstl.finaliseStorageRoot(2 * common.HashLength); err == nil {
				if ok {
					// There are some storage items
					fstl.accData.FieldSet |= AccountFieldStorageOnly
				}
			} else {
				return err
			}
			fstl.accData.Balance.Set(&fstl.a.Balance)
			if fstl.a.Balance.Sign() != 0 {
				fstl.accData.FieldSet |= AccountFieldBalanceOnly
			}
			fstl.accData.Nonce = fstl.a.Nonce
			if fstl.a.Nonce != 0 {
				fstl.accData.FieldSet |= AccountFieldNonceOnly
			}
			fstl.accData.Incarnation = fstl.a.Incarnation
			data = &fstl.accData
		}
		fstl.wasIHStorage = false
		fstl.currStorage.Reset()
		fstl.succStorage.Reset()
		fstl.groupsStorage = fstl.groupsStorage[:0]
		var err error
		if fstl.groups, err = GenStructStep(fstl.rl.Retain, fstl.curr.Bytes(), fstl.succ.Bytes(), fstl.hb, data, fstl.groups, false); err != nil {
			return err
		}
		fstl.accData.FieldSet = 0
	}
	// Remember the current key and value
	fstl.wasIH = isIH

	if isIH {
		fstl.value.Reset()
		fstl.value.Write(v)
		return nil
	}

	// Place code on the stack first, the storage will follow
	if !fstl.a.IsEmptyCodeHash() {
		// the first item ends up deepest on the stack, the second item - on the top
		fstl.accData.FieldSet |= AccountFieldCodeOnly
		if err := fstl.hb.hash(fstl.a.CodeHash[:]); err != nil {
			return err
		}
	}
	return nil
}

// nextSubtree does []byte++. Returns false if overflow.
func nextSubtree(in []byte) ([]byte, bool) {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 255 {
			r[i]++
			return r, true
		}

		r[i] = 0
	}
	return nil, false
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
