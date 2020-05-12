package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

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
	keyIdx   int
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

	accAddrHashWithInc []byte // Concatenation of addrHash of the currently build account with its incarnation encoding
}

func NewFlatDbSubTrieLoader() *FlatDbSubTrieLoader {
	return &FlatDbSubTrieLoader{
		hb:                 NewHashBuilder(false),
		accAddrHashWithInc: make([]byte, 40),
	}
}

// Reset prepares the loader for reuse
func (fstl *FlatDbSubTrieLoader) Reset() {
	fstl.keyIdx = 0
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
	fstl.groupsStorage = fstl.groupsStorage[:0]
	fstl.wasIHStorage = false
	fstl.subTries = SubTries{}
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

func (fstl *FlatDbSubTrieLoader) LoadSubTries(db ethdb.Database, rl *RetainList, dbPrefixes [][]byte, fixedbits []int, trace bool) (SubTries, error) {
	if len(dbPrefixes) == 0 {
		return SubTries{}, nil
	}
	defer trieFlatDbSubTrieLoaderTimer.UpdateSince(time.Now())
	fstl.trace = trace
	fstl.hb.trace = trace
	fstl.rl = rl

	if fstl.trace {
		fmt.Printf("----------\n")
		fmt.Printf("RebuildTrie\n")
	}

	if fstl.trace {
		fmt.Printf("fstl.rl: %x\n", fstl.rl.hexes)
		fmt.Printf("fixedbits: %d\n", fixedbits)
		fmt.Printf("dbPrefixes(%d): %x\n", len(dbPrefixes), dbPrefixes)
	}

	var boltDB *bolt.DB
	if hasBolt, ok := db.(ethdb.HasKV); ok {
		boltDB = hasBolt.KV()
	}

	if boltDB == nil {
		return SubTries{}, fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	cutoffs := make([]int, len(fixedbits))
	for i, bits := range fixedbits {
		if bits >= 256 /* addrHash */ +64 /* incarnation */ {
			cutoffs[i] = bits/4 - 16 // Remove incarnation
		} else {
			cutoffs[i] = bits / 4
		}
	}

	if err := fstl.MultiWalk2(boltDB, dbPrefixes, fixedbits, cutoffs, fstl.WalkerAccount, fstl.WalkerStorage); err != nil {
		return fstl.subTries, err
	}
	if err := fstl.finaliseRoot(cutoffs[len(cutoffs)-1]); err != nil {
		fmt.Println("Err in finalize root, writing down resolve params")
		fmt.Printf("fstl.rs: %x\n", fstl.rl.hexes)
		fmt.Printf("fixedbits: %d\n", fixedbits)
		fmt.Printf("dbPrefixes: %x\n", dbPrefixes)
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

type walker func(isIH bool, keyIdx int, k, v []byte) error

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

func (fstl *FlatDbSubTrieLoader) WalkerStorage(isIH bool, keyIdx int, k, v []byte) error {
	if fstl.trace {
		fmt.Printf("WalkerStorage: isIH=%v keyIdx=%d key=%x value=%x\n", isIH, keyIdx, k, v)
	}

	fstl.currStorage.Reset()
	fstl.currStorage.Write(fstl.succStorage.Bytes())
	fstl.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	keyToNibblesWithoutInc(k, &fstl.succStorage)

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
func (fstl *FlatDbSubTrieLoader) WalkerAccount(isIH bool, keyIdx int, k, v []byte) error {
	if fstl.trace {
		fmt.Printf("WalkerAccount: isIH=%v keyIdx=%d key=%x value=%x\n", isIH, keyIdx, k, v)
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

	if err := fstl.a.DecodeForStorage(v); err != nil {
		return fmt.Errorf("fail DecodeForStorage: %w", err)
	}
	copy(fstl.accAddrHashWithInc, k)
	binary.BigEndian.PutUint64(fstl.accAddrHashWithInc[32:40], ^fstl.a.Incarnation)
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

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieHashBucket
func (fstl *FlatDbSubTrieLoader) MultiWalk2(db *bolt.DB, startkeys [][]byte, fixedbits []int, cutoffs []int, accWalker walker, storageWalker walker) error {
	if len(startkeys) == 0 {
		return nil
	}

	var minKeyAsNibbles bytes.Buffer
	// Key used to advance to the next account (skip remaining storage)
	nextAccountKey := make([]byte, 32)

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := ethdb.Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	cutoff := cutoffs[rangeIdx]
	if len(startkey) > common.HashLength {
		// Looking for storage sub-tree
		copy(fstl.accAddrHashWithInc, startkey[:common.HashLength+common.IncarnationLength])
	}

	err := db.View(func(tx *bolt.Tx) error {
		ihBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		var ih *bolt.Cursor
		if ihBucket != nil {
			ih = ihBucket.Cursor()
		}
		c := tx.Bucket(dbutils.CurrentStateBucket).Cursor()

		k, v := c.Seek(startkey)
		if len(startkey) <= common.HashLength && len(k) > common.HashLength {
			// Advance past the storage to the first account
			if nextAccount(k, nextAccountKey) {
				k, v = c.SeekTo(nextAccountKey)
			} else {
				k = nil
			}
		}
		if fstl.trace {
			fmt.Printf("c.Seek(%x) = %x\n", startkey, k)
		}

		var ihK, ihV []byte
		if ih != nil {
			ihK, ihV = ih.Seek(startkey)
			if len(startkey) <= common.HashLength && len(ihK) > common.HashLength {
				// Advance past the storage to the first account
				if nextAccount(ihK, nextAccountKey) {
					ihK, ihV = ih.SeekTo(nextAccountKey)
				} else {
					ihK = nil
				}
			}
		}

		var minKey []byte
		var isIH bool
		for k != nil || ihK != nil {
			isIH, minKey = keyIsBefore(ihK, k)
			if fixedbytes > 0 {
				// Adjust rangeIdx if needed
				cmp := int(-1)
				for cmp != 0 {
					if len(minKey) < fixedbytes {
						cmp = bytes.Compare(minKey, startkey[:len(minKey)])
						if cmp == 0 {
							cmp = -1
						}
					} else {
						cmp = bytes.Compare(minKey[:fixedbytes-1], startkey[:fixedbytes-1])
						if cmp == 0 {
							k1 := minKey[fixedbytes-1] & mask
							k2 := startkey[fixedbytes-1] & mask
							if k1 < k2 {
								cmp = -1
							} else if k1 > k2 {
								cmp = 1
							}
						}
					}
					if cmp < 0 {
						// This happens after we have just incremented rangeIdx
						k, v = c.SeekTo(startkey)
						if len(startkey) <= common.HashLength && len(k) > common.HashLength {
							// Advance past the storage to the first account
							if nextAccount(k, nextAccountKey) {
								k, v = c.SeekTo(nextAccountKey)
							} else {
								k = nil
							}
						}
						if ih != nil {
							ihK, ihV = ih.SeekTo(startkey)
							if len(startkey) <= common.HashLength && len(ihK) > common.HashLength {
								// Advance to the first account
								if nextAccount(ihK, nextAccountKey) {
									ihK, ihV = ih.SeekTo(nextAccountKey)
								} else {
									ihK = nil
								}
							}
						}
						if k == nil && ihK == nil {
							return nil
						}
						isIH, minKey = keyIsBefore(ihK, k)
					} else if cmp > 0 {
						rangeIdx++
						if rangeIdx == len(startkeys) {
							return nil
						}
						if err := fstl.finaliseRoot(cutoff); err != nil {
							return err
						}
						fixedbytes, mask = ethdb.Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
						if len(startkey) > common.HashLength {
							// Looking for storage sub-tree
							copy(fstl.accAddrHashWithInc, startkey[:common.HashLength+common.IncarnationLength])
						}
						cutoff = cutoffs[rangeIdx]
						fstl.hb.Reset()
						fstl.wasIH = false
						fstl.wasIHStorage = false
						fstl.groups = fstl.groups[:0]
						fstl.groupsStorage = fstl.groupsStorage[:0]
						fstl.keyIdx = rangeIdx
						fstl.curr.Reset()
						fstl.succ.Reset()
						fstl.currStorage.Reset()
						fstl.succStorage.Reset()
					}
				}
			}

			if !isIH {
				if len(k) > common.HashLength && !bytes.HasPrefix(k, fstl.accAddrHashWithInc) {
					if bytes.Compare(k, fstl.accAddrHashWithInc) < 0 {
						// Skip all the irrelevant storage in the middle
						k, v = c.SeekTo(fstl.accAddrHashWithInc)
					} else {
						if nextAccount(k, nextAccountKey) {
							k, v = c.SeekTo(nextAccountKey)
						} else {
							k = nil
						}
					}
					continue
				}
				if len(k) > common.HashLength {
					if err := storageWalker(false, rangeIdx, k, v); err != nil {
						return err
					}
					k, v = c.Next()
					if fstl.trace {
						fmt.Printf("k after storageWalker and Next: %x\n", k)
					}
				} else {
					if err := accWalker(false, rangeIdx, k, v); err != nil {
						return err
					}
					// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
					// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
					// skips over all storage items
					k, v = c.SeekTo(fstl.accAddrHashWithInc)
					if ih != nil {
						if !bytes.HasPrefix(ihK, fstl.accAddrHashWithInc) {
							ihK, ihV = ih.SeekTo(fstl.accAddrHashWithInc)
						}
					}
				}
				continue
			}

			// ih part
			minKeyAsNibbles.Reset()
			keyToNibblesWithoutInc(minKey, &minKeyAsNibbles)

			if minKeyAsNibbles.Len() < cutoff {
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			retain := fstl.rl.Retain(minKeyAsNibbles.Bytes())
			if fstl.trace {
				fmt.Printf("fstl.rl.Retain(%x)=%t\n", minKeyAsNibbles.Bytes(), retain)
			}

			if retain { // can't use ih as is, need go to children
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			if len(ihK) > common.HashLength && !bytes.HasPrefix(ihK, fstl.accAddrHashWithInc) {
				if bytes.Compare(ihK, fstl.accAddrHashWithInc) < 0 {
					// Skip all the irrelevant storage in the middle
					ihK, ihV = ih.SeekTo(fstl.accAddrHashWithInc)
				} else {
					if nextAccount(ihK, nextAccountKey) {
						ihK, ihV = ih.SeekTo(nextAccountKey)
					} else {
						ihK = nil
					}
				}
				continue
			}
			if len(ihK) > common.HashLength {
				if err := storageWalker(true, rangeIdx, ihK, ihV); err != nil {
					return fmt.Errorf("storageWalker err: %w", err)
				}
			} else {
				if err := accWalker(true, rangeIdx, ihK, ihV); err != nil {
					return fmt.Errorf("accWalker err: %w", err)
				}
			}

			// skip subtree
			next, ok := nextSubtree(ihK)
			if !ok { // no siblings left
				if !retain { // last sub-tree was taken from IH, then nothing to look in the main bucket. Can stop.
					break
				}
				ihK, ihV = nil, nil
				continue
			}
			if fstl.trace {
				fmt.Printf("next: %x\n", next)
			}

			if !bytes.HasPrefix(k, next) {
				k, v = c.SeekTo(next)
			}
			if len(next) <= common.HashLength && len(k) > common.HashLength {
				// Advance past the storage to the first account
				if nextAccount(k, nextAccountKey) {
					k, v = c.SeekTo(nextAccountKey)
				} else {
					k = nil
				}
			}
			if fstl.trace {
				fmt.Printf("k after next: %x\n", k)
			}
			if !bytes.HasPrefix(ihK, next) {
				ihK, ihV = ih.SeekTo(next)
			}
			if len(next) <= common.HashLength && len(ihK) > common.HashLength {
				// Advance past the storage to the first account
				if nextAccount(ihK, nextAccountKey) {
					ihK, ihV = ih.SeekTo(nextAccountKey)
				} else {
					ihK = nil
				}
			}
			if fstl.trace {
				fmt.Printf("ihK after next: %x\n", ihK)
			}
		}
		return nil
	})
	return err
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
