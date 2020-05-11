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
	trieResolveStatefulTimer = metrics.NewRegisteredTimer("trie/resolve/stateful", nil)
)

type ResolverStateful struct {
	rs       *ResolveSet
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

func NewResolverStateful() *ResolverStateful {
	return &ResolverStateful{
		hb:                 NewHashBuilder(false),
		accAddrHashWithInc: make([]byte, 40),
	}
}

// Reset prepares the Resolver for reuse
func (tr *ResolverStateful) Reset() {
	tr.keyIdx = 0
	tr.rs = NewResolveSet(0)
	tr.curr.Reset()
	tr.succ.Reset()
	tr.value.Reset()
	tr.groups = tr.groups[:0]
	tr.a.Reset()
	tr.hb.Reset()
	tr.wasIH = false

	tr.currStorage.Reset()
	tr.succStorage.Reset()
	tr.valueStorage.Reset()
	tr.groupsStorage = tr.groupsStorage[:0]
	tr.wasIHStorage = false
	tr.subTries = SubTries{}
}

// cutoff specifies how many nibbles have to be cut from the beginning of the storage keys
// to fit the insertion point.
// We create a fake branch node at the cutoff point by modifying the last
// nibble of the `succ` key
func (tr *ResolverStateful) finaliseRoot(cutoff int) error {
	if tr.trace {
		fmt.Printf("finaliseRoot(%d)\n", cutoff)
	}
	if cutoff >= 2*common.HashLength {
		// if only storage resolution required, then no account records
		if ok, err := tr.finaliseStorageRoot(cutoff); err == nil {
			if ok {
				tr.subTries.roots = append(tr.subTries.roots, tr.hb.root())
				tr.subTries.Hashes = append(tr.subTries.Hashes, tr.hb.rootHash())
			} else {
				tr.subTries.roots = append(tr.subTries.roots, nil)
				tr.subTries.Hashes = append(tr.subTries.Hashes, common.Hash{})				
			}
		} else {
			return err
		}
		return nil
	}
	tr.curr.Reset()
	tr.curr.Write(tr.succ.Bytes())
	tr.succ.Reset()
	if tr.curr.Len() > 0 {
		if cutoff > 0 {
			tr.succ.Write(tr.curr.Bytes()[:cutoff-1])
			tr.succ.WriteByte(tr.curr.Bytes()[cutoff-1] + 1) // Modify last nibble before the cutoff point
		}
		var data GenStructStepData
		if tr.wasIH {
			tr.hashData.Hash = common.BytesToHash(tr.value.Bytes())
			data = &tr.hashData
		} else {
			if ok, err := tr.finaliseStorageRoot(2 * common.HashLength); err == nil {
				if ok {
					// There are some storage items
					tr.accData.FieldSet |= AccountFieldStorageOnly
				}
			} else {
				return err
			}
			tr.wasIHStorage = false
			tr.accData.Balance.Set(&tr.a.Balance)
			if tr.a.Balance.Sign() != 0 {
				tr.accData.FieldSet |= AccountFieldBalanceOnly
			}
			tr.accData.Nonce = tr.a.Nonce
			if tr.a.Nonce != 0 {
				tr.accData.FieldSet |= AccountFieldNonceOnly
			}
			tr.accData.Incarnation = tr.a.Incarnation
			data = &tr.accData
		}
		var err error
		if tr.groups, err = GenStructStep(tr.rs.HashOnly, tr.curr.Bytes(), tr.succ.Bytes(), tr.hb, data, tr.groups, false); err != nil {
			return err
		}
		tr.accData.FieldSet = 0
	}
	tr.groups = tr.groups[:0]
	tr.subTries.roots = append(tr.subTries.roots, tr.hb.root())
	tr.subTries.Hashes = append(tr.subTries.Hashes, tr.hb.rootHash())
	return nil
}

// cutoff specifies how many nibbles have to be cut from the beginning of the storage keys
// to fit the insertion point. For entire storage subtrie of an account, the cutoff
// would be the the length (in nibbles) of adress Hash.
// For resolver requests that asks for a part of some
// contract's storage, cutoff point will be deeper than that.
// We create a fake branch node at the cutoff point by modifying the last
// nibble of the `succ` key
func (tr *ResolverStateful) finaliseStorageRoot(cutoff int) (bool, error) {
	if tr.trace {
		fmt.Printf("finaliseStorageRoot(%d), succStorage.Len() = %d\n", cutoff, tr.succStorage.Len())
	}
	tr.currStorage.Reset()
	tr.currStorage.Write(tr.succStorage.Bytes())
	tr.succStorage.Reset()
	if tr.currStorage.Len() > 0 {
		tr.succStorage.Write(tr.currStorage.Bytes()[:cutoff-1])
		tr.succStorage.WriteByte(tr.currStorage.Bytes()[cutoff-1] + 1) // Modify last nibble in the incarnation part of the `currStorage`
		var data GenStructStepData
		if tr.wasIHStorage {
			tr.hashData.Hash = common.BytesToHash(tr.valueStorage.Bytes())
			data = &tr.hashData
		} else {
			tr.leafData.Value = rlphacks.RlpSerializableBytes(tr.valueStorage.Bytes())
			data = &tr.leafData
		}
		var err error
		tr.groupsStorage, err = GenStructStep(tr.rs.HashOnly, tr.currStorage.Bytes(), tr.succStorage.Bytes(), tr.hb, data, tr.groupsStorage, false)
		if err != nil {
			return false, err
		}
		tr.groupsStorage = tr.groupsStorage[:0]
		tr.currStorage.Reset()
		tr.succStorage.Reset()
		tr.wasIHStorage = false
		if tr.trace {
			fmt.Printf("storage root: %x\n", tr.hb.rootHash())
		}
		return true, nil
	}
	return false, nil
}

func (tr *ResolverStateful) RebuildTrie(db ethdb.Database, rs *ResolveSet, dbPrefixes [][]byte, fixedbits []int, trace bool) (SubTries, error) {
	if len(dbPrefixes) == 0 {
		return SubTries{}, nil
	}
	defer trieResolveStatefulTimer.UpdateSince(time.Now())
	tr.trace = trace
	tr.hb.trace = trace
	tr.rs = rs

	if tr.trace {
		fmt.Printf("----------\n")
		fmt.Printf("RebuildTrie\n")
	}

	if tr.trace {
		fmt.Printf("tr.rs: %x\n", tr.rs.hexes)
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
		if bits >= 256 /* addrHash */ + 64 /* incarnation */ {
			cutoffs[i] = bits/4 - 16 // Remove incarnation
		} else {
			cutoffs[i] = bits/4
		}
	}

	if err := tr.MultiWalk2(boltDB, dbPrefixes, fixedbits, cutoffs, tr.WalkerAccount, tr.WalkerStorage); err != nil {
		return tr.subTries, err
	}
	if err := tr.finaliseRoot(cutoffs[len(cutoffs)-1]); err != nil {
		fmt.Println("Err in finalize root, writing down resolve params")
		fmt.Printf("tr.rs: %x\n", tr.rs.hexes)
		fmt.Printf("fixedbits: %d\n", fixedbits)
		fmt.Printf("dbPrefixes: %x\n", dbPrefixes)
		return tr.subTries, fmt.Errorf("error in finaliseRoot %w", err)
	}
	return tr.subTries, nil
}

func (tr *ResolverStateful) AttachRequestedCode(db ethdb.Getter, requests []*ResolveRequestForCode) error {
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

func (tr *ResolverStateful) WalkerStorage(isIH bool, keyIdx int, k, v []byte) error {
	if tr.trace {
		fmt.Printf("WalkerStorage: isIH=%v keyIdx=%d key=%x value=%x\n", isIH, keyIdx, k, v)
	}

	tr.currStorage.Reset()
	tr.currStorage.Write(tr.succStorage.Bytes())
	tr.succStorage.Reset()
	// Transform k to nibbles, but skip the incarnation part in the middle
	keyToNibblesWithoutInc(k, &tr.succStorage)

	if !isIH {
		tr.succStorage.WriteByte(16)
	}

	if tr.currStorage.Len() > 0 {
		var err error
		var data GenStructStepData
		if tr.trace {
			fmt.Printf("tr.wasIHStorage=%t\n", tr.wasIHStorage)
		}
		if tr.wasIHStorage {
			tr.hashData.Hash = common.BytesToHash(tr.valueStorage.Bytes())
			data = &tr.hashData
		} else {
			tr.leafData.Value = rlphacks.RlpSerializableBytes(tr.valueStorage.Bytes())
			data = &tr.leafData
		}
		tr.groupsStorage, err = GenStructStep(tr.rs.HashOnly, tr.currStorage.Bytes(), tr.succStorage.Bytes(), tr.hb, data, tr.groupsStorage, false)
		if err != nil {
			return err
		}
	}
	// Remember the current key and value
	tr.wasIHStorage = isIH
	tr.valueStorage.Reset()
	tr.valueStorage.Write(v)

	return nil
}

// Walker - k, v - shouldn't be reused in the caller's code
func (tr *ResolverStateful) WalkerAccount(isIH bool, keyIdx int, k, v []byte) error {
	if tr.trace {
		fmt.Printf("WalkerAccount: isIH=%v keyIdx=%d key=%x value=%x\n", isIH, keyIdx, k, v)
	}
	tr.curr.Reset()
	tr.curr.Write(tr.succ.Bytes())
	tr.succ.Reset()
	for _, b := range k {
		tr.succ.WriteByte(b / 16)
		tr.succ.WriteByte(b % 16)
	}
	if !isIH {
		tr.succ.WriteByte(16)
	}

	if tr.curr.Len() > 0 {
		var data GenStructStepData
		if tr.wasIH {
			tr.hashData.Hash = common.BytesToHash(tr.value.Bytes())
			data = &tr.hashData
		} else {
			if ok, err := tr.finaliseStorageRoot(2 * common.HashLength); err == nil {
				if ok {
					// There are some storage items
					tr.accData.FieldSet |= AccountFieldStorageOnly
				}
			} else {
				return err
			}
			tr.accData.Balance.Set(&tr.a.Balance)
			if tr.a.Balance.Sign() != 0 {
				tr.accData.FieldSet |= AccountFieldBalanceOnly
			}
			tr.accData.Nonce = tr.a.Nonce
			if tr.a.Nonce != 0 {
				tr.accData.FieldSet |= AccountFieldNonceOnly
			}
			tr.accData.Incarnation = tr.a.Incarnation
			data = &tr.accData
		}
		tr.wasIHStorage = false
		tr.currStorage.Reset()
		tr.succStorage.Reset()
		tr.groupsStorage = tr.groupsStorage[:0]
		var err error
		if tr.groups, err = GenStructStep(tr.rs.HashOnly, tr.curr.Bytes(), tr.succ.Bytes(), tr.hb, data, tr.groups, false); err != nil {
			return err
		}
		tr.accData.FieldSet = 0
	}
	// Remember the current key and value
	tr.wasIH = isIH

	if isIH {
		tr.value.Reset()
		tr.value.Write(v)
		return nil
	}

	if err := tr.a.DecodeForStorage(v); err != nil {
		return fmt.Errorf("fail DecodeForStorage: %w", err)
	}
	copy(tr.accAddrHashWithInc, k)
	binary.BigEndian.PutUint64(tr.accAddrHashWithInc[32:40], ^tr.a.Incarnation)
	// Place code on the stack first, the storage will follow
	if !tr.a.IsEmptyCodeHash() {
		// the first item ends up deepest on the stack, the second item - on the top
		tr.accData.FieldSet |= AccountFieldCodeOnly
		if err := tr.hb.hash(tr.a.CodeHash[:]); err != nil {
			return err
		}
	}
	return nil
}

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieHashBucket
func (tr *ResolverStateful) MultiWalk2(db *bolt.DB, startkeys [][]byte, fixedbits []int, cutoffs []int, accWalker walker, storageWalker walker) error {
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
		copy(tr.accAddrHashWithInc, startkey[:common.HashLength+common.IncarnationLength])
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
		if tr.trace {
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
						if err := tr.finaliseRoot(cutoff); err != nil {
							return err
						}
						fixedbytes, mask = ethdb.Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
						if len(startkey) > common.HashLength {
							// Looking for storage sub-tree
							copy(tr.accAddrHashWithInc, startkey[:common.HashLength+common.IncarnationLength])
						}
						cutoff = cutoffs[rangeIdx]
						tr.hb.Reset()
						tr.wasIH = false
						tr.wasIHStorage = false
						tr.groups = tr.groups[:0]
						tr.groupsStorage = tr.groupsStorage[:0]
						tr.keyIdx = rangeIdx
						tr.curr.Reset()
						tr.succ.Reset()
						tr.currStorage.Reset()
						tr.succStorage.Reset()
					}
				}
			}

			if !isIH {
				if len(k) > common.HashLength && !bytes.HasPrefix(k, tr.accAddrHashWithInc) {
					if bytes.Compare(k, tr.accAddrHashWithInc) < 0 {
						// Skip all the irrelevant storage in the middle
						k, v = c.SeekTo(tr.accAddrHashWithInc)
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
					if tr.trace {
						fmt.Printf("k after storageWalker and Next: %x\n", k)
					}
				} else {
					if err := accWalker(false, rangeIdx, k, v); err != nil {
						return err
					}
					// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
					// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
					// skips over all storage items
					k, v = c.SeekTo(tr.accAddrHashWithInc)
					if ih != nil {
						if !bytes.HasPrefix(ihK, tr.accAddrHashWithInc) {
							ihK, ihV = ih.SeekTo(tr.accAddrHashWithInc)
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

			canUseIntermediateHash := tr.rs.HashOnly(minKeyAsNibbles.Bytes())
			if tr.trace {
				fmt.Printf("tr.rs.HashOnly(%x)=%t\n", minKeyAsNibbles.Bytes(), canUseIntermediateHash)
			}

			if !canUseIntermediateHash { // can't use ih as is, need go to children
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			if len(ihK) > common.HashLength && !bytes.HasPrefix(ihK, tr.accAddrHashWithInc) {
				if bytes.Compare(ihK, tr.accAddrHashWithInc) < 0 {
					// Skip all the irrelevant storage in the middle
					ihK, ihV = ih.SeekTo(tr.accAddrHashWithInc)
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
				if canUseIntermediateHash { // last sub-tree was taken from IH, then nothing to look in the main bucket. Can stop.
					break
				}
				ihK, ihV = nil, nil
				continue
			}
			if tr.trace {
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
			if tr.trace {
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
			if tr.trace {
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
