package trie

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strings"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

const TraceFromBlock uint64 = 258199

type ResolverStatefulCached struct {
	*ResolverStateful
	fromCache bool
	hashData  GenStructStepHashData
	trace     bool
}

func NewResolverStatefulCached(topLevels int, requests []*ResolveRequest, hookFunction hookFunction) *ResolverStatefulCached {
	return &ResolverStatefulCached{
		ResolverStateful: NewResolverStateful(topLevels, requests, hookFunction),
	}
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

// PrepareResolveParams prepares information for the MultiWalk
// Changes compare to ResolverStateful:
//   - key can have 0 length - to be able walk by cache bucket
func (tr *ResolverStatefulCached) PrepareResolveParams() ([][]byte, []uint) {
	// Remove requests strictly contained in the preceding ones
	startkeys := [][]byte{}
	fixedbits := []uint{}
	tr.rss = tr.rss[:0]
	if len(tr.requests) == 0 {
		return startkeys, fixedbits
	}
	var prevReq *ResolveRequest
	for i, req := range tr.requests {
		if prevReq == nil ||
			!bytes.Equal(req.contract, prevReq.contract) ||
			!bytes.Equal(req.resolveHex[:req.resolvePos], prevReq.resolveHex[:prevReq.resolvePos]) {

			tr.reqIndices = append(tr.reqIndices, i)
			pLen := len(req.contract)
			key := make([]byte, pLen+len(req.resolveHex[:req.resolvePos]))
			copy(key[:], req.contract)
			decodeNibbles(req.resolveHex[:req.resolvePos], key[pLen:])
			startkeys = append(startkeys, key)
			req.extResolvePos = req.resolvePos + 2*pLen
			fixedbits = append(fixedbits, uint(4*req.extResolvePos))
			prevReq = req
			var minLength int
			if req.resolvePos >= tr.topLevels {
				minLength = 0
			} else {
				minLength = tr.topLevels - req.resolvePos
			}
			rs := NewResolveSet(minLength)
			tr.rss = append(tr.rss, rs)
			rs.AddHex(req.resolveHex[req.resolvePos:])
		} else {
			rs := tr.rss[len(tr.rss)-1]
			rs.AddHex(req.resolveHex[req.resolvePos:])
		}
	}
	tr.currentReq = tr.requests[tr.reqIndices[0]]
	tr.currentRs = tr.rss[0]
	return startkeys, fixedbits
}

func (tr *ResolverStatefulCached) finaliseRoot() error {
	tr.curr.Reset()
	tr.curr.Write(tr.succ.Bytes())
	tr.succ.Reset()
	if tr.curr.Len() > 0 {
		var err error
		var data GenStructStepData
		if tr.fromCache {
			tr.hashData.Hash = common.BytesToHash(tr.value.Bytes())
			data = &tr.hashData
		} else if tr.fieldSet == 0 {
			tr.leafData.Value = rlphacks.RlpSerializableBytes(tr.value.Bytes())
			data = &tr.leafData
		} else {
			tr.accData.FieldSet = tr.fieldSet
			tr.accData.StorageSize = tr.a.StorageSize
			tr.accData.Balance.Set(&tr.a.Balance)
			tr.accData.Nonce = tr.a.Nonce
			tr.accData.Incarnation = tr.a.Incarnation
			data = &tr.accData
		}
		tr.groups, err = GenStructStep(tr.currentRs.HashOnly, tr.curr.Bytes(), tr.succ.Bytes(), tr.hb, data, tr.groups, false)
		if err != nil {
			return err
		}
	}
	if tr.hb.hasRoot() {
		hbRoot := tr.hb.root()
		hbHash := tr.hb.rootHash()
		return tr.hookFunction(tr.currentReq, hbRoot, hbHash)
	}
	return nil
}

// keyIsBefore - kind of bytes.Compare, but nil is the last key. And return
func keyIsBefore(k1, k2 []byte) (bool, []byte) {
	if k1 == nil {
		return false, k2
	}

	if k2 == nil {
		return true, k1
	}

	switch cmpWithoutIncarnation(k1, k2) {
	case -1, 0:
		return true, k1
	default:
		return false, k2
	}
}

// cmpWithoutIncarnation - removing incarnation from 2nd key if necessary
func cmpWithoutIncarnation(k1, k2 []byte) int {
	if k1 == nil {
		return 1
	}

	if k2 == nil {
		return -1
	}

	if len(k2) <= common.HashLength {
		return bytes.Compare(k1, k2)
	}

	if len(k2) <= common.HashLength+8 {
		return bytes.Compare(k1, k2[:common.HashLength])
	}

	buf := pool.GetBuffer(256)
	defer pool.PutBuffer(buf)
	buf.B = append(buf.B[:0], k2[:common.HashLength]...)
	buf.B = append(buf.B, k2[common.HashLength+8:]...)

	return bytes.Compare(k1, buf.B)
}

func (tr *ResolverStatefulCached) RebuildTrie(
	db ethdb.Database,
	blockNr uint64,
	accounts bool,
	historical bool,
	trace bool) error {
	startkeys, fixedbits := tr.PrepareResolveParams()
	if db == nil {
		var b strings.Builder
		fmt.Fprintf(&b, "ResolveWithDb(db=nil), accounts: %t\n", accounts)
		for i, sk := range startkeys {
			fmt.Fprintf(&b, "sk %x, bits: %d\n", sk, fixedbits[i])
		}
		return fmt.Errorf("unexpected resolution: %s at %s", b.String(), debug.Stack())
	}
	if trace {
		fmt.Printf("RebuildTrie %d\n", len(startkeys))
		for _, startkey := range startkeys {
			fmt.Printf("%x\n", startkey)
		}
	}
	tr.trace = trace

	var boltDb *bolt.DB
	if hasBolt, ok := db.(ethdb.HasKV); ok {
		boltDb = hasBolt.KV()
	}

	if boltDb == nil {
		return fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	//if blockNr > TraceFromBlock {
	//	for _, req := range tr.requests {
	//		fmt.Printf("req.resolvePos: %d, req.extResolvePos: %d, len(req.resolveHex): %d, len(req.contract): %d\n", req.resolvePos, req.extResolvePos, len(req.resolveHex), len(req.contract))
	//		fmt.Printf("req.contract: %x, req.resolveHex: %x\n", req.contract, req.resolveHex)
	//	}
	//	fmt.Printf("fixedbits: %d\n", fixedbits)
	//	fmt.Printf("startkey: %x\n", startkeys)
	//}

	var err error
	if accounts {
		if historical {
			return errors.New("historical resolver not supported yet")
		}
		err = tr.MultiWalk2(boltDb, blockNr, dbutils.AccountsBucket, startkeys, fixedbits, tr.WalkerAccounts)
	} else {
		if historical {
			return errors.New("historical resolver not supported yet")
		}
		err = tr.MultiWalk2(boltDb, blockNr, dbutils.StorageBucket, startkeys, fixedbits, tr.WalkerStorage)

	}
	if err != nil {
		return err
	}

	if err = tr.finaliseRoot(); err != nil {
		fmt.Println("Err in finalize root, writing down resolve params")
		for _, req := range tr.requests {
			fmt.Printf("req.resolveHash: %s\n", req.resolveHash)
			fmt.Printf("req.resolvePos: %d, req.extResolvePos: %d, len(req.resolveHex): %d, len(req.contract): %d\n", req.resolvePos, req.extResolvePos, len(req.resolveHex), len(req.contract))
			fmt.Printf("req.contract: %x, req.resolveHex: %x\n", req.contract, req.resolveHex)
		}
		fmt.Printf("fixedbits: %d\n", fixedbits)
		fmt.Printf("startkey: %x\n", startkeys)
		return fmt.Errorf("error in finaliseRoot, for block %d: %w", blockNr, err)
	}
	return nil
}

func (tr *ResolverStatefulCached) WalkerAccounts(keyIdx int, blockNr uint64, k []byte, v []byte, fromCache bool) error {
	return tr.Walker(true, blockNr, fromCache, keyIdx, k, v)
}

func (tr *ResolverStatefulCached) WalkerStorage(keyIdx int, blockNr uint64, k []byte, v []byte, fromCache bool) error {
	return tr.Walker(false, blockNr, fromCache, keyIdx, k, v)
}

// Walker - k, v - shouldn't be reused in the caller's code
func (tr *ResolverStatefulCached) Walker(isAccount bool, blockNr uint64, fromCache bool, keyIdx int, kAsNibbles []byte, v []byte) error {
	//if isAccount && fromCache {
	//	buf := pool.GetBuffer(256)
	//	CompressNibbles(kAsNibbles, &buf.B)
	if tr.trace {
		fmt.Printf("Walker Cached: blockNr: %d, keyIdx: %d key:%x  value:%x, fromCache: %v\n", blockNr, keyIdx, kAsNibbles, v, fromCache)
	}

	if keyIdx != tr.keyIdx {
		if err := tr.finaliseRoot(); err != nil {
			return err
		}
		tr.hb.Reset()
		tr.groups = nil
		tr.keyIdx = keyIdx
		tr.currentReq = tr.requests[tr.reqIndices[keyIdx]]
		tr.currentRs = tr.rss[keyIdx]
		tr.curr.Reset()
	}
	if len(v) > 0 {
		tr.curr.Reset()
		tr.curr.Write(tr.succ.Bytes())
		tr.succ.Reset()
		skip := tr.currentReq.extResolvePos // how many first nibbles to skip
		if fromCache && skip > common.HashLength*2 {
			skip -= 16 // no incarnation in hash bucket
		}
		tr.succ.Write(kAsNibbles[skip:])

		if !fromCache {
			tr.succ.WriteByte(16)
		}

		if tr.curr.Len() > 0 {
			var err error
			var data GenStructStepData
			if tr.fromCache {
				tr.hashData.Hash = common.BytesToHash(tr.value.Bytes())
				data = &tr.hashData
			} else if tr.fieldSet == 0 {
				tr.leafData.Value = rlphacks.RlpSerializableBytes(tr.value.Bytes())
				data = &tr.leafData
			} else {
				tr.accData.FieldSet = tr.fieldSet
				tr.accData.StorageSize = tr.a.StorageSize
				tr.accData.Balance.Set(&tr.a.Balance)
				tr.accData.Nonce = tr.a.Nonce
				tr.accData.Incarnation = tr.a.Incarnation
				data = &tr.accData
			}
			tr.groups, err = GenStructStep(tr.currentRs.HashOnly, tr.curr.Bytes(), tr.succ.Bytes(), tr.hb, data, tr.groups, false)
			if err != nil {
				return err
			}
		}
		// Remember the current key and value
		tr.fromCache = fromCache
		if fromCache {
			tr.value.Reset()
			tr.value.Write(v)
			return nil
		}

		if isAccount {
			if err := tr.a.DecodeForStorage(v); err != nil {
				return fmt.Errorf("fail DecodeForStorage: %w", err)
			}
			if tr.a.IsEmptyCodeHash() && tr.a.IsEmptyRoot() {
				tr.fieldSet = AccountFieldSetNotContract
			} else {
				if tr.a.HasStorageSize {
					tr.fieldSet = AccountFieldSetContractWithSize
				} else {
					tr.fieldSet = AccountFieldSetContract
				}
				// the first item ends up deepest on the stack, the seccond item - on the top
				if err := tr.hb.hash(tr.a.CodeHash[:]); err != nil {
					return err
				}
				if err := tr.hb.hash(tr.a.Root[:]); err != nil {
					return err
				}
			}
		} else {
			tr.value.Reset()
			tr.value.Write(v)
			tr.fieldSet = AccountFieldSetNotAccount
		}
	}
	return nil
}

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieHashBucket
func (tr *ResolverStatefulCached) MultiWalk2(db *bolt.DB, blockNr uint64, bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(keyIdx int, blockNr uint64, k []byte, v []byte, fromCache bool) error) error {
	if len(startkeys) == 0 {
		return nil
	}
	isAccountBucket := bytes.Equal(bucket, dbutils.AccountsBucket)
	maxAccountKeyLen := common.HashLength - 1

	keyAsNibbles := pool.GetBuffer(256)
	defer pool.PutBuffer(keyAsNibbles)

	startKeyNoInc := pool.GetBuffer(common.HashLength * 2)
	defer pool.PutBuffer(startKeyNoInc)

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := ethdb.Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]

	startKeyNoInc.Reset()
	dbutils.RemoveIncarnationFromKey(startkey, &startKeyNoInc.B)
	if tr.trace {
		fmt.Printf("RemoveInc startkey, startKeyNoInc.B = %x, %x\n", startkey, startKeyNoInc.B)
	}

	err := db.View(func(tx *bolt.Tx) error {
		cacheBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		if cacheBucket == nil {
			return nil
		}
		cache := cacheBucket.Cursor()

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()

		k, v := c.Seek(startkey)
		cacheK, cacheV := cache.Seek(startKeyNoInc.B)

		var minKey []byte
		var fromCache bool
		for k != nil || cacheK != nil {
			//if blockNr > TraceFromBlock {
			if tr.trace {
				fmt.Printf("For loop: %x, %x\n", cacheK, k)
			}

			// for Address bucket, skip cache keys longer than 31 bytes
			if isAccountBucket {
				for len(cacheK) > maxAccountKeyLen {
					cacheK, cacheV = cache.Next()
				}
			}

			fromCache, minKey = keyIsBefore(cacheK, k)
			if fixedbytes > 0 {
				// Adjust rangeIdx if needed
				cmp := int(-1)
				for cmp != 0 {
					startKeyIndex := 0
					minKeyIndex := 0
					startKeyIndexIsBigger := false

					if fromCache && fixedbytes > 32 && fixedbytes <= 40 { // compare only part before incarnation
						startKeyIndex = 31
						minKeyIndex = minInt(len(minKey), 31)
						startKeyIndexIsBigger = startKeyIndex > minKeyIndex
						cmp = bytes.Compare(minKey[:minKeyIndex], startkey[:startKeyIndex])
						if tr.trace {
							fmt.Printf("cmp1 %x %x (%d)\n", minKey[:minKeyIndex], startkey[:startKeyIndex], cmp)
						}
					} else if fromCache && fixedbytes > 40 { // compare without incarnation
						startKeyIndex = fixedbytes - 1 // will use it on startKey (which has incarnation) later
						minKeyIndex = minInt(len(minKey), fixedbytes-1-8)
						startKeyIndexIsBigger = startKeyIndex-8 > minKeyIndex
						cmp = bytes.Compare(minKey[:minKeyIndex], startKeyNoInc.B[:startKeyIndex-8])
						if tr.trace {
							fmt.Printf("cmp2 %x %x [%x] (%d), %d %d\n", minKey[:minKeyIndex], startKeyNoInc.B[:startKeyIndex-8], startKeyNoInc.B, cmp, startKeyIndex-8, len(startKeyNoInc.B))
						}
					} else if fromCache {
						startKeyIndex = fixedbytes - 1
						minKeyIndex = minInt(len(minKey), fixedbytes-1)
						startKeyIndexIsBigger = startKeyIndex > minKeyIndex
						cmp = bytes.Compare(minKey[:minKeyIndex], startkey[:startKeyIndex])
						if tr.trace {
							fmt.Printf("cmp3 %x %x [%x] (%d), %d %d\n", minKey[:minKeyIndex], startkey[:startKeyIndex], startkey, cmp, startKeyIndex, len(startkey))
						}
					} else {
						startKeyIndex = fixedbytes - 1
						minKeyIndex = fixedbytes - 1
						startKeyIndexIsBigger = startKeyIndex > minKeyIndex
						cmp = bytes.Compare(minKey[:minKeyIndex], startkey[:startKeyIndex])
						if tr.trace {
							fmt.Printf("cmp4 %x %x (%d)\n", minKey[:minKeyIndex], startkey[:startKeyIndex], cmp)
						}
					}

					if cmp == 0 && minKeyIndex == len(minKey) { // minKey has no more bytes to compare, then it's less than startKey
						if startKeyIndexIsBigger {
							cmp = -1
						}
					} else if cmp == 0 {
						if tr.trace {
							fmt.Printf("cmp5: [%x] %x %x %b, %d %d\n", minKey, minKey[minKeyIndex], startkey[startKeyIndex], mask, minKeyIndex, startKeyIndex)
						}
						k1 := minKey[minKeyIndex] & mask
						k2 := startkey[startKeyIndex] & mask
						if k1 < k2 {
							cmp = -1
						} else if k1 > k2 {
							cmp = 1
						}
					}

					if cmp < 0 {
						k, v = c.SeekTo(startkey)
						cacheK, cacheV = cache.SeekTo(startKeyNoInc.B)
						if tr.trace {
							fmt.Printf("c.SeekTo(%x) = %x\n", startkey, k)
							fmt.Printf("[fromCache = %t], cache.SeekTo(%x) = %x\n", fromCache, startKeyNoInc.B, cacheK)
							fmt.Printf("[request = %s]\n", tr.requests[tr.reqIndices[rangeIdx]])
						}
						// for Address bucket, skip cache keys longer than 31 bytes
						if isAccountBucket && len(cacheK) > maxAccountKeyLen {
							for len(cacheK) > maxAccountKeyLen {
								cacheK, cacheV = cache.Next()
							}
						}
						if k == nil && cacheK == nil {
							return nil
						}

						fromCache, minKey = keyIsBefore(cacheK, k)
						if tr.trace {
							//fmt.Printf("fromCache, minKey = %t, %x\n", fromCache, minKey)
						}
					} else if cmp > 0 {
						rangeIdx++
						if rangeIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = ethdb.Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
						startKeyNoInc.Reset()
						dbutils.RemoveIncarnationFromKey(startkey, &startKeyNoInc.B)
						if tr.trace {
							fmt.Printf("RemoveInc2 startkey, startKeyNoInc.B = %x, %x\n", startkey, startKeyNoInc.B)
						}
					}
				}
			}

			if !fromCache {
				if len(v) > 0 {
					keyAsNibbles.Reset()
					DecompressNibbles(minKey, &keyAsNibbles.B)
					if err := walker(rangeIdx, blockNr, keyAsNibbles.B, v, false); err != nil {
						return err
					}
				}
				k, v = c.Next()
				continue
			}

			// cache part
			canUseCache := false

			// Special case: self-destructed accounts.
			// self-destructed accounts may be marked in cache bucket by empty value
			// in this case: account - add to Trie, storage - skip with subtree (it will be deleted by a background pruner)
			isSelfDestructedMarker := len(cacheV) == 0
			if isSelfDestructedMarker {
				if isAccountBucket && len(v) > 0 && bytes.Equal(k, cacheK) {
					keyAsNibbles.Reset()
					DecompressNibbles(minKey, &keyAsNibbles.B)
					if err := walker(rangeIdx, blockNr, keyAsNibbles.B, v, false); err != nil {
						return err
					}
				}
				// skip subtree
			} else {
				currentReq := tr.requests[tr.reqIndices[rangeIdx]]
				currentRs := tr.rss[rangeIdx]
				keyAsNibbles.Reset()
				DecompressNibbles(minKey, &keyAsNibbles.B)

				if len(keyAsNibbles.B) < currentReq.extResolvePos {
					cacheK, cacheV = cache.Next() // go to children, not to sibling
					continue
				}

				canUseCache = currentRs.HashOnly(keyAsNibbles.B[currentReq.extResolvePos:])
				if !canUseCache { // can't use cache as is, need go to children
					cacheK, cacheV = cache.Next() // go to children, not to sibling
					continue
				}

				if err := walker(rangeIdx, blockNr, keyAsNibbles.B, cacheV, fromCache); err != nil {
					return fmt.Errorf("waker err: %w", err)
				}
			}

			// skip subtree

			next, ok := nextSubtree(cacheK)
			if !ok { // no siblings left
				if canUseCache { // last sub-tree was taken from cache, then nothing to look in the main bucket. Can stop.
					break
				}
				cacheK, cacheV = nil, nil
				continue
			}

			if isAccountBucket || len(cacheK) <= common.HashLength {
				k, v = c.Seek(next)
			} else {
				// skip subtree - can't .Seek because storage bucket has incarnation in keys
				for k, v = c.Next(); k != nil; k, v = c.Next() {
					matchAcc := bytes.HasPrefix(k[:common.HashLength], next[:common.HashLength])
					// don't check incarnation ...
					matchStorage := bytes.HasPrefix(k[common.HashLength+8:], next[common.HashLength:])

					if matchAcc && matchStorage {
						break
					}
				}
			}

			cacheK, cacheV = cache.Seek(next)
		}
		return nil
	})
	return err
}

func minInt(i1, i2 int) int {
	return int(math.Min(float64(i1), float64(i2)))
}
