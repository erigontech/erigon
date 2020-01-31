package trie

import (
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
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

// hexIncrement does []byte++. Returns nil if overflow.
func hexIncrement(in []byte) []byte {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 255 {
			r[i]++
			return r
		}

		r[i] = 0
	}
	return nil
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
		err = tr.MultiWalk2(boltDb, dbutils.AccountsBucket, startkeys, fixedbits, tr.WalkerAccounts)
	} else {
		if historical {
			return errors.New("historical resolver not supported yet")
		}
		err = tr.MultiWalk2(boltDb, dbutils.StorageBucket, startkeys, fixedbits, tr.WalkerStorage)
	}
	if err != nil {
		return fmt.Errorf("failed RebuildTrie: %w", err)
	}
	return tr.finaliseRoot()
}

func (tr *ResolverStatefulCached) WalkerAccounts(keyIdx int, k []byte, v []byte, fromCache bool) error {
	return tr.Walker(true, fromCache, keyIdx, k, v)
}

func (tr *ResolverStatefulCached) WalkerStorage(keyIdx int, k []byte, v []byte, fromCache bool) error {
	return tr.Walker(false, fromCache, keyIdx, k, v)
}

// Walker - k, v - shouldn't be reused in the caller's code
func (tr *ResolverStatefulCached) Walker(isAccount bool, fromCache bool, keyIdx int, k []byte, v []byte) error {
	//fmt.Printf("Walker Cached: keyIdx: %d key:%x  value:%x, fromCache: %v\n", keyIdx, k, v, fromCache)
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
		tr.succ.WriteByte(16)
		if tr.curr.Len() > 0 {
			var err error
			var data GenStructStepData
			if tr.fromCache {
				data = GenStructStepHashData{Hash: common.BytesToHash(tr.value.Bytes())}
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
		if fromCache {
			tr.value.Reset()
			tr.value.Write(v)
			tr.fromCache = true
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

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieCacheBucket
func (tr *ResolverStatefulCached) MultiWalk2(db *bolt.DB, bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(keyIdx int, k []byte, v []byte, fromCache bool) error) error {
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
			fromCache, minKey = keyIsBefore(cacheK, k)
			if fromCache && !tr.currentRs.HashOnly(minKey) { // can't use cache as is, need go to children
				cacheK, cacheV = cache.SeekTo(append(minKey, 0)) // seek only cache cursor, because it's minimal move
				continue
			}
			if fixedbytes > 0 {
				// Adjust rangeIdx if needed
				cmp := int(-1)
				for cmp != 0 {
					_, minKey = keyIsBefore(cacheK, k)

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
					if cmp < 0 {
						k, v = c.SeekTo(startkey)
						cacheK, cacheV = cache.SeekTo(startkey)
						if k == nil && cacheK == nil {
							return nil
						}
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
					if err := walker(rangeIdx, k, v, fromCache); err != nil {
						return fmt.Errorf("waker err: %w", err)
					}
				}
				k, v = c.Next()
				continue
			}

			if len(cacheV) > 0 {
				if err := walker(rangeIdx, cacheK, cacheV, fromCache); err != nil {
					return fmt.Errorf("waker err: %w", err)
				}
			}

			next := hexIncrement(cacheK)
			if next == nil { // no siblings left
				break
			}
			k, v = c.SeekTo(next)
			cacheK, cacheV = cache.SeekTo(next)
		}
		return nil
	})
	return err
}
