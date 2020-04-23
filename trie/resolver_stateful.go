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
	a          accounts.Account
	leafData   GenStructStepLeafData
	accData    GenStructStepAccountData
	requests   []*ResolveRequest

	roots        []node // roots of the tries that are being built
	hookFunction hookFunction

	wasIH              bool
	wasAccount         bool
	hashData           GenStructStepHashData
	trace              bool
	accAddrHashIsValid bool
	accAddrHash        []byte
}

func NewResolverStateful(topLevels int, requests []*ResolveRequest, hookFunction hookFunction) *ResolverStateful {
	return &ResolverStateful{
		topLevels:    topLevels,
		hb:           NewHashBuilder(false),
		reqIndices:   []int{},
		requests:     requests,
		hookFunction: hookFunction,
		accAddrHash:  []byte{},
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
	tr.rss = tr.rss[:0]
	tr.requests = requests
	tr.hookFunction = hookFunction
	tr.curr.Reset()
	tr.succ.Reset()
	tr.value.Reset()
	tr.groups = tr.groups[:0]
	tr.a.Reset()
	tr.hb.Reset()
	tr.wasIH = false
	tr.accAddrHashIsValid = false
	tr.accAddrHash = tr.accAddrHash[:0]
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

func (tr *ResolverStateful) finaliseRoot(accRoot getAccRootF) error {
	tr.curr.Reset()
	tr.curr.Write(tr.succ.Bytes())
	tr.succ.Reset()
	if tr.curr.Len() > 0 {
		var err error
		var data GenStructStepData
		if tr.wasIH {
			tr.hashData.Hash = common.BytesToHash(tr.value.Bytes())
			data = &tr.hashData
		} else if !tr.wasAccount {
			tr.leafData.Value = rlphacks.RlpSerializableBytes(tr.value.Bytes())
			data = &tr.leafData
		} else {
			storageHash, err2 := accRoot(common.BytesToHash(tr.accAddrHash), tr.a.Incarnation)
			if err2 != nil {
				return err2
			}
			tr.a.Root.SetBytes(storageHash.Bytes())

			if tr.a.IsEmptyCodeHash() && tr.a.IsEmptyRoot() {
				tr.accData.FieldSet = AccountFieldSetNotContract
			} else {
				if tr.a.HasStorageSize {
					tr.accData.FieldSet = AccountFieldSetContractWithSize
				} else {
					tr.accData.FieldSet = AccountFieldSetContract
				}
			}

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

	getAccRot := func(acc common.Hash, incarnation uint64) (common.Hash, error) {
		root := EmptyRoot
		root2 := EmptyRoot
		rootIH := EmptyRoot
		accWithInc := dbutils.GenerateStoragePrefix(acc, incarnation)
		resolver := NewResolverStateful(0, []*ResolveRequest{
			tr.currentReq.t.NewResolveRequest(accWithInc, nil, 0, nil),
		}, func(req *ResolveRequest, node node, hash common.Hash) error {
			root.SetBytes(common.CopyBytes(hash.Bytes()))

			h := req.t.getHasher()
			defer returnHasherToPool(h)
			if _, err := h.hash(node, true, root2[:]); err != nil {
				return err
			}

			return nil
		})

		if err := resolver.RebuildTrie(db, 0, false, false, false); err != nil {
			return root, err
		}

		if err := boltDB.View(func(tx *bolt.Tx) error {
			v, _ := tx.Bucket(dbutils.IntermediateTrieHashBucket).Get(accWithInc)
			if v == nil {
				return ethdb.ErrKeyNotFound
			}

			rootIH.SetBytes(common.CopyBytes(v))
			return nil
		}); err != nil {
			return root, err
		}

		//if !(bytes.Equal(root.Bytes(), rootIH.Bytes()) && bytes.Equal(root.Bytes(), root2.Bytes())) {
		//	fmt.Printf("%x!=%x!=%x\n\n", root, rootIH, root2)
		//	panic(1)
		//}

		return root, nil
	}

	var err error
	if isAccount {
		if historical {
			panic("historical data is not implemented")
		} else {
			err = tr.MultiWalk2(boltDB, startkeys, fixedbits, tr.Walker, true, getAccRot)
		}
	} else {
		if historical {
			panic("historical data is not implemented")
		} else {
			err = tr.MultiWalk2(boltDB, startkeys, fixedbits, tr.Walker, false, getAccRot)
		}
	}
	if err != nil {
		return err
	}
	if err = tr.finaliseRoot(getAccRot); err != nil {
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

// Walker - k, v - shouldn't be reused in the caller's code
func (tr *ResolverStateful) Walker(isIH bool, keyIdx int, k, v []byte, accRoot getAccRootF) error {
	isAccount := !isIH && len(k) == 32
	if tr.trace {
		fmt.Printf("Walker: %t, keyIdx: %d key:%x  value:%x, isIH: %v\n", isAccount, keyIdx, k, v, isIH)
	}

	if keyIdx != tr.keyIdx {
		if err := tr.finaliseRoot(accRoot); err != nil {
			return err
		}
		tr.hb.Reset()
		tr.groups = nil
		tr.keyIdx = keyIdx
		tr.currentReq = tr.requests[tr.reqIndices[keyIdx]]
		tr.currentRs = tr.rss[keyIdx]
		tr.curr.Reset()
		tr.accAddrHashIsValid = false
		tr.accAddrHash = tr.accAddrHash[:0]
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
			if tr.wasIH {
				tr.hashData.Hash = common.BytesToHash(tr.value.Bytes())
				data = &tr.hashData
			} else if !tr.wasAccount {
				tr.leafData.Value = rlphacks.RlpSerializableBytes(tr.value.Bytes())
				data = &tr.leafData
			} else {
				storageHash, err2 := accRoot(common.BytesToHash(tr.accAddrHash), tr.a.Incarnation)
				if err2 != nil {
					return err2
				}
				tr.a.Root.SetBytes(storageHash.Bytes())

				if tr.a.IsEmptyCodeHash() && tr.a.IsEmptyRoot() {
					tr.accData.FieldSet = AccountFieldSetNotContract
				} else {
					if tr.a.HasStorageSize {
						tr.accData.FieldSet = AccountFieldSetContractWithSize
					} else {
						tr.accData.FieldSet = AccountFieldSetContract
					}
				}

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
		tr.wasIH = isIH
		tr.wasAccount = isAccount

		if tr.wasIH || !tr.wasAccount {
			tr.value.Reset()
			tr.value.Write(v)
			return nil
		}

		if err := tr.a.DecodeForStorage(v); err != nil {
			return fmt.Errorf("fail DecodeForStorage: %w", err)
		}

		tr.accAddrHashIsValid = true
		tr.accAddrHash = tr.accAddrHash[:0]
		tr.accAddrHash = append(tr.accAddrHash, k...)
	}

	return nil
}

type getAccRootF = func(acc common.Hash, incarnation uint64) (common.Hash, error)

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieHashBucket
func (tr *ResolverStateful) MultiWalk2(db *bolt.DB, startkeys [][]byte, fixedbits []uint, walker func(isIH bool, keyIdx int, k, v []byte, accRoot getAccRootF) error, isAccount bool, accRoot getAccRootF) error {
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
							fmt.Printf("[wasIH = %t], ih.SeekTo(%x) = %x\n", isIH, startkey, ihK)
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
							fmt.Printf("wasIH, minKey = %t, %x\n", isIH, minKey)
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
				if err := walker(false, rangeIdx, minKey, v, accRoot); err != nil {
					return err
				}
				k, v = c.Next()
				continue
			}

			// ih part
			canUseIntermediateHash := false

			currentReq := tr.requests[tr.reqIndices[rangeIdx]]
			currentRs := tr.rss[rangeIdx]
			_ = currentRs

			keyAsNibbles.Reset()
			DecompressNibbles(minKey, &keyAsNibbles.B)

			if len(keyAsNibbles.B) < currentReq.extResolvePos {
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			canUseIntermediateHash = currentRs.HashOnly(keyAsNibbles.B[currentReq.extResolvePos:])
			//canUseIntermediateHash = false
			if !canUseIntermediateHash { // can't use ih as is, need go to children
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			if err := walker(true, rangeIdx, ihK, ihV, accRoot); err != nil {
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

		k, v := c.Seek(startkey)

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
