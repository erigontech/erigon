package trie

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strings"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

type ResolverStatefulCached struct {
	*ResolverStateful
	fromCache bool
	hashData  GenStructStepHashData
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
	tr.rss = nil
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
		err := tr.hookFunction(tr.currentReq, hbRoot, hbHash)
		if err != nil {
			return err
		}
		return nil
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

	switch bytes.Compare(k1, k2) {
	case -1, 0:
		return true, k1
	default:
		return false, k2
	}
}

func (tr *ResolverStatefulCached) RebuildTrie(
	db ethdb.Database,
	blockNr uint64,
	accounts bool,
	historical bool) error {
	startkeys, fixedbits := tr.PrepareResolveParams()
	if db == nil {
		var b strings.Builder
		fmt.Fprintf(&b, "ResolveWithDb(db=nil), accounts: %t\n", accounts)
		for i, sk := range startkeys {
			fmt.Fprintf(&b, "sk %x, bits: %d\n", sk, fixedbits[i])
		}
		return fmt.Errorf("unexpected resolution: %s at %s", b.String(), debug.Stack())
	}

	var boltDb *bolt.DB
	if hasBolt, ok := db.(ethdb.HasBolt); ok {
		boltDb = hasBolt.DB()
	} else if hasDb, ok := db.(ethdb.HasDb); ok {
		if hasBolt, ok := hasDb.DB().(ethdb.HasBolt); ok {
			boltDb = hasBolt.DB()
		}
	}

	if boltDb == nil {
		return fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	if err := boltDb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dbutils.IntermediateTrieCacheBucket, false)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

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
		return fmt.Errorf("error in finaliseRoot, for block %d: %w", blockNr, err)
	}
	return nil
}

func (tr *ResolverStatefulCached) WalkerAccounts(keyIdx int, k []byte, v []byte, fromCache bool) error {
	return tr.Walker(true, fromCache, keyIdx, k, v)
}

func (tr *ResolverStatefulCached) WalkerStorage(keyIdx int, k []byte, v []byte, fromCache bool) error {
	return tr.Walker(false, fromCache, keyIdx, k, v)
}

// Walker - k, v - shouldn't be reused in the caller's code
func (tr *ResolverStatefulCached) Walker(isAccount bool, fromCache bool, keyIdx int, k []byte, v []byte) error {
	//if bytes.Compare(common.FromHex("0808"), k) == 0 {
	//	fmt.Printf("Walker Cached: keyIdx: %d key:%x  value:%x, fromCache: %v\n", keyIdx, k, v, fromCache)
	//}
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
		i := 0
		for _, b := range k {
			if i >= skip {
				tr.succ.WriteByte(b / 16)
			}
			i++
			if i >= skip {
				tr.succ.WriteByte(b % 16)
			}
			i++
		}

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
				return fmt.Errorf("fail GenStructStep: %w", err)
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

var csvCached *bufio.Writer

func init() {
	fmFile, err := os.OpenFile("cached_iterations.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	csvCached = bufio.NewWriter(fmFile)
}

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieCacheBucket
func (tr *ResolverStatefulCached) MultiWalk2(db *bolt.DB, blockNr uint64, bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(keyIdx int, k []byte, v []byte, fromCache bool) error) error {
	nibblesBuf := pool.GetBuffer(128)
	defer pool.PutBuffer(nibblesBuf)

	if len(startkeys) == 0 {
		return nil
	}
	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := ethdb.Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	err := db.View(func(tx *bolt.Tx) error {
		cacheBucket := tx.Bucket(dbutils.IntermediateTrieCacheBucket)
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
		cacheK, cacheV := cache.Seek(startkey)

		var minKey []byte
		var fromCache bool
		for k != nil || cacheK != nil {
			// for Address bucket, skip cache keys longer than 31 bytes
			if len(k) == common.HashLength && len(cacheK) > common.HashLength-1 {
				next, ok := nextSubtree(cacheK[:common.HashLength-1])
				if !ok { // no siblings left in cache
					cacheK, cacheV = nil, nil
					continue
				}
				cacheK, cacheV = cache.SeekTo(next)
				continue
			}

			fromCache, minKey = keyIsBefore(cacheK, k)
			if fixedbytes > 0 {
				// Adjust rangeIdx if needed
				cmp := int(-1)
				for cmp != 0 {
					minKeyIndex := int(math.Min(float64(len(minKey)), float64(fixedbytes-1)))
					startKeyIndex := int(math.Min(float64(len(startkey)), float64(fixedbytes-1)))

					cmp = bytes.Compare(minKey[:minKeyIndex], startkey[:startKeyIndex])

					if cmp == 0 && minKeyIndex == len(minKey) { // minKey has no more bytes to compare, then it's less than startKey
						cmp = -1
					}

					if cmp == 0 {
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
						cacheK, cacheV = cache.SeekTo(startkey)
						if k == nil && cacheK == nil {
							return nil
						}
						_, minKey = keyIsBefore(cacheK, k)
					} else if cmp > 0 {
						rangeIdx++
						if rangeIdx == len(startkeys) {
							return nil
						}
						fixedbytes, mask = ethdb.Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
					}
				}
			}

			fromCache, _ = keyIsBefore(cacheK, k)
			if !fromCache {
				if len(v) > 0 {
					if err := walker(rangeIdx, k, v, false); err != nil {
						return err
					}
				}
				k, v = c.Next()
				continue
			}

			// cache part

			// Special case: self-destructed accounts.
			// self destcructed accounts may be marked in cache bucket by empty value
			// in this case: account - add to Trie, storage - skip with subtree (it will be deleted by a background pruner)
			if len(cacheV) == 0 {
				if len(cacheK) != common.HashLength { // skip invalid cache key
					cacheK, cacheV = cache.Next()
					continue
				}

				if len(k) == common.HashLength && len(v) > 0 && bytes.Equal(k, cacheK) {
					if err := walker(rangeIdx, k, v, false); err != nil {
						return err
					}
				}
				// skip subtree
			}

			if rangeIdx != tr.keyIdx {
				tr.groups = nil
				tr.keyIdx = rangeIdx
				tr.currentReq = tr.requests[tr.reqIndices[rangeIdx]]
				tr.currentRs = tr.rss[rangeIdx]
			}

			canUseCache := false
			if len(cacheV) > 0 {
				if len(cacheK)*2 < tr.currentReq.extResolvePos {
					cacheK, cacheV = cache.Next() // go to children, not to sibling
					continue
				}

				nibblesBuf.Reset()
				if err := DecompressNibbles(cacheK, &nibblesBuf.B); err != nil {
					return err
				}

				canUseCache = tr.currentRs.HashOnly(nibblesBuf.B[tr.currentReq.extResolvePos:])
				if !canUseCache { // can't use cache as is, need go to children
					cacheK, cacheV = cache.Next() // go to children, not to sibling
					continue
				}
				/*
					fmt.Printf("HashOnly says yes: %d %d %d %x %x\n", tr.topLevels, rangeIdx, tr.currentReq.extResolvePos, nibblesBuf.B[tr.currentReq.extResolvePos:], nibblesBuf.B)
					for i, a := range tr.rss {
						for _, h := range a.hexes {
							fmt.Printf("In hexes: %d %x\n", i, h)
						}
					}

					for _, r := range tr.requests {
						fmt.Printf("In request: %x %d\n", r.resolveHex, r.resolvePos)
					}
				*/

				if err := walker(rangeIdx, common.CopyBytes(cacheK), common.CopyBytes(cacheV), fromCache); err != nil {
					return fmt.Errorf("waker err: %w", err)
				}

			}
			// skip subtree

			next, ok := nextSubtree(cacheK)
			if !ok { // no siblings left in cache
				cacheK, cacheV = nil, nil
				if canUseCache { // last sub-tree was taken from cache
					break
				}
				continue
			}
			k, v = c.SeekTo(next)
			cacheK, cacheV = cache.SeekTo(next)
		}
		return nil
	})
	return err
}
