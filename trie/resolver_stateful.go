package trie

import (
	"bytes"
	"fmt"
	"math"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

var (
	trieResolveStatefulTimer = metrics.NewRegisteredTimer("trie/resolve/stateful", nil)
)

type hookFunction func(*ResolveRequest, node, common.Hash) error

type ResolverStateful struct {
	rss        []*ResolveSet
	curr       bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ       bytes.Buffer
	value      bytes.Buffer // Current value to be used as the value tape for the hash builder
	groups     []uint16
	reqIndices []int // Indices pointing back to request slice from slices returned by PrepareResolveParams
	hb         *HashBuilder
	topLevels  int             // How many top levels of the trie to keep (not roll into hashes)
	currentReq *ResolveRequest // Request currently being handled
	currentRs  *ResolveSet     // ResolveSet currently being used
	keyIdx     int
	fieldSet   uint32 // fieldSet for the next invocation of genStructStep
	a          accounts.Account
	leafData   GenStructStepLeafData
	accData    GenStructStepAccountData
	requests   []*ResolveRequest

	roots        []node // roots of the tries that are being built
	hookFunction hookFunction

	isIH     bool
	hashData GenStructStepHashData
	trace    bool
}

func NewResolverStateful(topLevels int, requests []*ResolveRequest, hookFunction hookFunction) *ResolverStateful {
	return &ResolverStateful{
		topLevels:    topLevels,
		hb:           NewHashBuilder(false),
		reqIndices:   []int{},
		requests:     requests,
		hookFunction: hookFunction,
	}
}

// Reset prepares the Resolver for reuse
func (tr *ResolverStateful) Reset(topLevels int, requests []*ResolveRequest, hookFunction hookFunction) {
	tr.topLevels = topLevels
	tr.requests = tr.requests[:0]
	tr.reqIndices = tr.reqIndices[:0]
	tr.keyIdx = 0
	tr.currentReq = nil
	tr.currentRs = nil
	tr.fieldSet = 0
	tr.rss = tr.rss[:0]
	tr.requests = requests
	tr.hookFunction = hookFunction
	tr.curr.Reset()
	tr.succ.Reset()
	tr.value.Reset()
	tr.groups = tr.groups[:0]
	tr.a.Reset()
	tr.hb.Reset()
	tr.isIH = false
}

func (tr *ResolverStateful) PopRoots() []node {
	roots := tr.roots
	tr.roots = nil
	return roots
}

// PrepareResolveParams prepares information for the MultiWalk
func (tr *ResolverStateful) PrepareResolveParams() ([][]byte, []uint) {
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

func (tr *ResolverStateful) finaliseRoot() error {
	tr.curr.Reset()
	tr.curr.Write(tr.succ.Bytes())
	tr.succ.Reset()
	if tr.curr.Len() > 0 {
		var err error
		var data GenStructStepData
		if tr.isIH {
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
			if !tr.a.IsEmptyCodeHash() || !tr.a.IsEmptyRoot() {
				// the first item ends up deepest on the stack, the second item - on the top
				err = tr.hb.hash(tr.a.CodeHash[:])
				if err != nil {
					return err
				}
				err = tr.hb.hash(tr.a.Root[:])
				if err != nil {
					return err
				}
			}
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

func (tr *ResolverStateful) RebuildTrie(
	db ethdb.Database,
	blockNr uint64,
	isAccount bool,
	historical bool,
	trace bool) error {
	defer trieResolveStatefulTimer.UpdateSince(time.Now())
	tr.trace = trace

	startkeys, fixedbits := tr.PrepareResolveParams()
	if db == nil {
		var b strings.Builder
		fmt.Fprintf(&b, "ResolveWithDb(db=nil), isAccount: %t\n", isAccount)
		for i, sk := range startkeys {
			fmt.Fprintf(&b, "sk %x, bits: %d\n", sk, fixedbits[i])
		}
		return fmt.Errorf("unexpected resolution: %s at %s", b.String(), debug.Stack())
	}
	if tr.trace {
		fmt.Printf("RebuildTrie %d, blockNr %d\n", len(startkeys), blockNr)
		for _, startkey := range startkeys {
			fmt.Printf("%x\n", startkey)
		}
	}

	var boltDB *bolt.DB
	if hasBolt, ok := db.(ethdb.HasKV); ok {
		boltDB = hasBolt.KV()
	}

	if boltDB == nil {
		return fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	var err error
	if isAccount {
		if historical {
			panic("historical data is not implemented")
		} else {
			err = tr.MultiWalk2(boltDB, startkeys, fixedbits, tr.WalkerAccounts, true)
		}
	} else {
		if historical {
			panic("historical data is not implemented")
		} else {
			err = tr.MultiWalk2(boltDB, startkeys, fixedbits, tr.WalkerStorage, false)
		}
	}
	if err != nil {
		return err
	}
	if err = tr.finaliseRoot(); err != nil {
		fmt.Println("Err in finalize root, writing down resolve params", isAccount)
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

func (tr *ResolverStateful) WalkerAccounts(keyIdx int, k []byte, v []byte, isIH bool, accRoot func(addrHash []byte, a accounts.Account) ([]byte, error)) error {
	return tr.Walker(true, isIH, keyIdx, k, v, accRoot)
}

func (tr *ResolverStateful) WalkerStorage(keyIdx int, k []byte, v []byte, isIH bool, accRoot func(addrHash []byte, a accounts.Account) ([]byte, error)) error {
	return tr.Walker(false, isIH, keyIdx, k, v, accRoot)
}

// Walker - k, v - shouldn't be reused in the caller's code
func (tr *ResolverStateful) Walker(isAccount bool, isIH bool, keyIdx int, k []byte, v []byte, accRoot func(addrHash []byte, a accounts.Account) ([]byte, error)) error {
	if tr.trace {
		fmt.Printf("Walker: %t, keyIdx: %d key:%x  value:%x, isIH: %v\n", isAccount, keyIdx, k, v, isIH)
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

		if !isIH {
			tr.succ.WriteByte(16)
		}

		if tr.curr.Len() > 0 {
			var err error
			var data GenStructStepData
			if tr.isIH {
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
				if !tr.a.IsEmptyCodeHash() || !tr.a.IsEmptyRoot() {
					// the first item ends up deepest on the stack, the second item - on the top
					err = tr.hb.hash(tr.a.CodeHash[:])
					if err != nil {
						return err
					}
					err = tr.hb.hash(tr.a.Root[:])
					if err != nil {
						return err
					}
				}
			}
			tr.groups, err = GenStructStep(tr.currentRs.HashOnly, tr.curr.Bytes(), tr.succ.Bytes(), tr.hb, data, tr.groups, false)
			if err != nil {
				return err
			}
		}
		// Remember the current key and value
		tr.isIH = isIH
		if isIH {
			tr.value.Reset()
			tr.value.Write(v)
			return nil
		}

		if !isAccount {
			tr.value.Reset()
			tr.value.Write(v)
			tr.fieldSet = AccountFieldSetNotAccount
			return nil
		}

		if err := tr.a.DecodeForStorage(v); err != nil {
			return fmt.Errorf("fail DecodeForStorage: %w", err)
		}
		storageHash, err := accRoot(k, tr.a)
		if err != nil {
			return err
		}
		tr.a.Root.SetBytes(storageHash)

		if tr.a.IsEmptyCodeHash() && tr.a.IsEmptyRoot() {
			tr.fieldSet = AccountFieldSetNotContract
		} else {
			if tr.a.HasStorageSize {
				tr.fieldSet = AccountFieldSetContractWithSize
			} else {
				tr.fieldSet = AccountFieldSetContract
			}
		}
	}

	return nil
}

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieHashBucket
func (tr *ResolverStateful) MultiWalk2(db *bolt.DB, startkeys [][]byte, fixedbits []uint, walker func(keyIdx int, k []byte, v []byte, isIH bool, accRoot func([]byte, accounts.Account) ([]byte, error)) error, isAccount bool) error {
	if len(startkeys) == 0 {
		return nil
	}

	keyAsNibbles := pool.GetBuffer(256)
	defer pool.PutBuffer(keyAsNibbles)

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := ethdb.Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	err := db.View(func(tx *bolt.Tx) error {
		ihBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		var ih *bolt.Cursor
		if ihBucket != nil {
			ih = ihBucket.Cursor()
		}
		c := tx.Bucket(dbutils.CurrentStateBucket).Cursor()
		accRoots := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor()

		k, v := c.Seek(startkey)
		getAccRoot := func(addrHash []byte, a accounts.Account) ([]byte, error) {
			seekKey := dbutils.GenerateStoragePrefix(addrHash, a.Incarnation)
			accRootKey, accRoot := accRoots.SeekTo(seekKey)
			if tr.trace {
				fmt.Printf("getAccRoot: %x -> %x, %x\n", seekKey, accRootKey, accRoot)
			}
			if accRoot == nil || !bytes.Equal(seekKey, accRootKey) {
				return nil, fmt.Errorf("acc root not found for %x", seekKey)
			}
			return accRoot, nil
		}

		var ihK, ihV []byte
		if ih != nil {
			ihK, ihV = ih.Seek(startkey)
		}

		var minKey []byte
		var isIH bool
		for k != nil || ihK != nil {
			// for Address bucket, skip ih keys longer than 31 bytes
			if isAccount {
				for len(ihK) > 31 {
					ihK, ihV = ih.Next()
				}
				for len(k) > 32 {
					k, v = c.Next()
				}
			} else {
				for len(k) == 32 {
					k, v = c.Next()
				}
			}
			if ihK == nil && k == nil {
				break
			}
			if tr.trace {
				fmt.Printf("For loop: %x, %x\n", ihK, k)
			}

			isIH, minKey = keyIsBefore(ihK, k)
			if fixedbytes > 0 {
				// Adjust rangeIdx if needed
				cmp := int(-1)
				for cmp != 0 {
					startKeyIndex := fixedbytes - 1
					minKeyIndex := minInt(len(minKey), fixedbytes-1)
					startKeyIndexIsBigger := startKeyIndex > minKeyIndex
					cmp = bytes.Compare(minKey[:minKeyIndex], startkey[:startKeyIndex])
					if tr.trace {
						fmt.Printf("cmp3 %x %x [%x] (%d), %d %d\n", minKey[:minKeyIndex], startkey[:startKeyIndex], startkey, cmp, startKeyIndex, len(startkey))
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
						ihK, ihV = ih.SeekTo(startkey)
						if tr.trace {
							fmt.Printf("c.SeekTo(%x) = %x\n", startkey, k)
							fmt.Printf("[isIH = %t], ih.SeekTo(%x) = %x\n", isIH, startkey, ihK)
							fmt.Printf("[request = %s]\n", tr.requests[tr.reqIndices[rangeIdx]])
						}
						// for Address bucket, skip ih keys longer than 31 bytes
						if isAccount {
							for len(ihK) > 31 {
								ihK, ihV = ih.Next()
							}
							for len(k) > 32 {
								k, v = c.Next()
							}
						} else {
							for len(k) == 32 {
								k, v = c.Next()
							}
						}
						if k == nil && ihK == nil {
							return nil
						}

						isIH, minKey = keyIsBefore(ihK, k)
						if tr.trace {
							fmt.Printf("isIH, minKey = %t, %x\n", isIH, minKey)
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

			if !isIH {
				if err := walker(rangeIdx, minKey, v, false, getAccRoot); err != nil {
					return err
				}
				k, v = c.Next()
				continue
			}

			// ih part
			canUseIntermediateHash := false

			currentReq := tr.requests[tr.reqIndices[rangeIdx]]
			currentRs := tr.rss[rangeIdx]

			keyAsNibbles.Reset()
			DecompressNibbles(minKey, &keyAsNibbles.B)

			if len(keyAsNibbles.B) < currentReq.extResolvePos {
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			canUseIntermediateHash = currentRs.HashOnly(keyAsNibbles.B[currentReq.extResolvePos:])
			if !canUseIntermediateHash { // can't use ih as is, need go to children
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			if err := walker(rangeIdx, minKey, ihV, isIH, nil); err != nil {
				return fmt.Errorf("waker err: %w", err)
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

			k, v = c.Seek(next)
			ihK, ihV = ih.Seek(next)
		}
		return nil
	})
	return err
}

func minInt(i1, i2 int) int {
	return int(math.Min(float64(i1), float64(i2)))
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

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieHashBucket
func (tr *ResolverStateful) MultiWalk2(db *bolt.DB, blockNr uint64, bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(keyIdx int, blockNr uint64, k []byte, v []byte, fromCache bool, accRoot func([]byte, accounts.Account) ([]byte, error)) error, isAccount bool) error {
	if len(startkeys) == 0 {
		return nil
	}

	keyAsNibbles := pool.GetBuffer(256)
	defer pool.PutBuffer(keyAsNibbles)

	rangeIdx := 0 // What is the current range we are extracting
	fixedbytes, mask := ethdb.Bytesmask(fixedbits[rangeIdx])
	startkey := startkeys[rangeIdx]
	err := db.View(func(tx *bolt.Tx) error {
		cacheBucket := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		var cache *bolt.Cursor
		if cacheBucket != nil {
			cache = cacheBucket.Cursor()
		}
		c := tx.Bucket(bucket).Cursor()
		accRoots := tx.Bucket(dbutils.IntermediateTrieHashBucket).Cursor()

		k, v := c.Seek(startkey)
		getAccRoot := func(addrHash []byte, a accounts.Account) ([]byte, error) {
			seekKey := dbutils.GenerateStoragePrefix(common.BytesToHash(addrHash), a.Incarnation)
			accRootKey, accRoot := accRoots.SeekTo(seekKey)
			if tr.trace {
				fmt.Printf("getAccRoot: %x -> %x, %x\n", seekKey, accRootKey, accRoot)
			}
			if accRoot == nil || !bytes.Equal(seekKey, accRootKey) {
				return nil, fmt.Errorf("acc root not found for %x", seekKey)
			}
			return accRoot, nil
		}

		var cacheK, cacheV []byte
		if cache != nil {
			cacheK, cacheV = cache.Seek(startkey)
		}

		var minKey []byte
		var fromCache bool
		for k != nil || cacheK != nil {
			// for Address bucket, skip cache keys longer than 31 bytes
			if isAccount {
				for len(cacheK) > 31 {
					cacheK, cacheV = cache.Next()
				}
				for len(k) > 32 {
					k, v = c.Next()
				}
			} else {
				for len(k) == 32 {
					k, v = c.Next()
				}
			}
			if cacheK == nil && k == nil {
				break
			}
			if tr.trace {
				fmt.Printf("For loop: %x, %x\n", cacheK, k)
			}

			fromCache, minKey = keyIsBefore(cacheK, k)
			if fixedbytes > 0 {
				// Adjust rangeIdx if needed
				cmp := int(-1)
				for cmp != 0 {
					startKeyIndex := fixedbytes - 1
					minKeyIndex := minInt(len(minKey), fixedbytes-1)
					startKeyIndexIsBigger := startKeyIndex > minKeyIndex
					cmp = bytes.Compare(minKey[:minKeyIndex], startkey[:startKeyIndex])
					if tr.trace {
						fmt.Printf("cmp3 %x %x [%x] (%d), %d %d\n", minKey[:minKeyIndex], startkey[:startKeyIndex], startkey, cmp, startKeyIndex, len(startkey))
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
						cacheK, cacheV = cache.SeekTo(startkey)
						if tr.trace {
							fmt.Printf("c.SeekTo(%x) = %x\n", startkey, k)
							fmt.Printf("[isIH = %t], cache.SeekTo(%x) = %x\n", fromCache, startkey, cacheK)
							fmt.Printf("[request = %s]\n", tr.requests[tr.reqIndices[rangeIdx]])
						}
						// for Address bucket, skip cache keys longer than 31 bytes
						if isAccount {
							for len(cacheK) > 31 {
								cacheK, cacheV = cache.Next()
							}
							for len(k) > 32 {
								k, v = c.Next()
							}
						} else {
							for len(k) == 32 {
								k, v = c.Next()
							}
						}
						if k == nil && cacheK == nil {
							return nil
						}

						fromCache, minKey = keyIsBefore(cacheK, k)
						if tr.trace {
							fmt.Printf("isIH, minKey = %t, %x\n", fromCache, minKey)
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

			if !fromCache {
				if err := walker(rangeIdx, blockNr, minKey, v, false, getAccRoot); err != nil {
					return err
				}
				k, v = c.Next()
				continue
			}

			// cache part
			canUseCache := false

			currentReq := tr.requests[tr.reqIndices[rangeIdx]]
			currentRs := tr.rss[rangeIdx]

			if len(keyAsNibbles.B) < currentReq.extResolvePos {
				cacheK, cacheV = cache.Next() // go to children, not to sibling
				continue
			}

			keyAsNibbles.Reset()
			DecompressNibbles(minKey, &keyAsNibbles.B)
			canUseCache = currentRs.HashOnly(keyAsNibbles.B[currentReq.extResolvePos:])
			if !canUseCache { // can't use cache as is, need go to children
				cacheK, cacheV = cache.Next() // go to children, not to sibling
				continue
			}

			if err := walker(rangeIdx, blockNr, minKey, cacheV, fromCache, nil); err != nil {
				return fmt.Errorf("waker err: %w", err)
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

			k, v = c.Seek(next)
			cacheK, cacheV = cache.Seek(next)
		}
		return nil
	})
	return err
}

func minInt(i1, i2 int) int {
	return int(math.Min(float64(i1), float64(i2)))
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
