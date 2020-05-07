package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
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
	rss              []*ResolveSet
	rssChopped       []*ResolveSet
	curr             bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succ             bytes.Buffer
	value            bytes.Buffer // Current value to be used as the value tape for the hash builder
	groups           []uint16
	reqIndices       []int // Indices pointing back to request slice from slices returned by PrepareResolveParams
	hb               *HashBuilder
	topLevels        int             // How many top levels of the trie to keep (not roll into hashes)
	currentReq       *ResolveRequest // Request currently being handled
	currentRs        *ResolveSet     // ResolveSet currently being used
	currentRsChopped *ResolveSet
	keyIdx           int
	a                accounts.Account
	leafData         GenStructStepLeafData
	accData          GenStructStepAccountData
	requests         []*ResolveRequest

	roots        []node // roots of the tries that are being built
	hookFunction hookFunction

	wasIH        bool
	wasIHStorage bool
	hashData     GenStructStepHashData
	trace        bool

	currStorage   bytes.Buffer // Current key for the structure generation algorithm, as well as the input tape for the hash builder
	succStorage   bytes.Buffer
	valueStorage  bytes.Buffer // Current value to be used as the value tape for the hash builder
	groupsStorage []uint16
	hbStorage     *HashBuilder

	seenAccount        bool
	accAddrHashWithInc []byte // valid only if `seenAccount` is true
}

func NewResolverStateful(requests []*ResolveRequest, hookFunction hookFunction) *ResolverStateful {
	return &ResolverStateful{
		hb:                 NewHashBuilder(false),
		hbStorage:          NewHashBuilder(false),
		reqIndices:         []int{},
		requests:           requests,
		hookFunction:       hookFunction,
		accAddrHashWithInc: make([]byte, 40),
	}
}

// Reset prepares the Resolver for reuse
func (tr *ResolverStateful) Reset(requests []*ResolveRequest, hookFunction hookFunction) {
	tr.requests = tr.requests[:0]
	tr.reqIndices = tr.reqIndices[:0]
	tr.keyIdx = 0
	tr.currentReq = nil
	tr.currentRs = nil
	tr.currentRsChopped = nil
	tr.rss = tr.rss[:0]
	tr.rssChopped = tr.rssChopped[:0]
	tr.requests = requests
	tr.hookFunction = hookFunction
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
	tr.hbStorage.Reset()
	tr.groupsStorage = tr.groupsStorage[:0]
	tr.wasIHStorage = false
	tr.seenAccount = false
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
	tr.rssChopped = tr.rssChopped[:0]
	if len(tr.requests) == 0 {
		return startkeys, fixedbits
	}
	contractAsNibbles := pool.GetBuffer(256)
	defer pool.PutBuffer(contractAsNibbles)

	var prevReq *ResolveRequest
	for i, req := range tr.requests {
		contractAsNibbles.Reset()
		if req.contract != nil {
			DecompressNibbles(req.contract, &contractAsNibbles.B)
		}
		if prevReq != nil &&
			bytes.Equal(req.contract, prevReq.contract) &&
			bytes.Equal(req.resolveHex[:req.resolvePos], prevReq.resolveHex[:prevReq.resolvePos]) {

			tr.rssChopped[len(tr.rssChopped)-1].AddHex(req.resolveHex[req.resolvePos:])
			if req.contract == nil {
				tr.rss[len(tr.rss)-1].AddHex(req.resolveHex)
			} else {
				k := append(append([]byte{}, contractAsNibbles.B[:common.HashLength*2]...), req.resolveHex...)
				tr.rss[len(tr.rss)-1].AddHex(k)
			}
			continue
		}
		if prevReq != nil && prevReq.contract == nil && req.contract != nil {
			prevHex := prevReq.resolveHex
			if len(prevHex) == 65 {
				prevHex = prevHex[:64]
			}
			if bytes.Equal(prevHex[:prevReq.resolvePos], contractAsNibbles.B[:prevReq.resolvePos]) {
				k := append(append([]byte{}, contractAsNibbles.B[:common.HashLength*2]...), req.resolveHex...)
				tr.rss[len(tr.rss)-1].AddHex(k)
				tr.rssChopped[len(tr.rssChopped)-1].AddHex(req.resolveHex[req.resolvePos:])
				continue
			}
		}
		tr.reqIndices = append(tr.reqIndices, i)
		pLen := len(req.contract)
		req.extResolvePos = req.resolvePos + 2*pLen
		fixedbits = append(fixedbits, uint(4*req.extResolvePos))

		if pLen == 32 { // if we don't know incarnation, then just start resolution from account record
			startkeys = append(startkeys, req.contract)
			req.extResolvePos += 16
		} else {
			key := make([]byte, pLen+int(math.Ceil(float64(req.resolvePos)/2)))
			copy(key[:], req.contract)
			decodeNibbles(req.resolveHex[:req.resolvePos], key[pLen:])

			startkeys = append(startkeys, key)
		}

		tr.rssChopped = append(tr.rssChopped, NewResolveSet(0))
		tr.rss = append(tr.rss, NewResolveSet(0))

		tr.rssChopped[len(tr.rssChopped)-1].AddHex(req.resolveHex[req.resolvePos:])
		if req.contract == nil {
			tr.rss[len(tr.rss)-1].AddHex(req.resolveHex)
		} else {
			k := append(append([]byte{}, contractAsNibbles.B[:common.HashLength*2]...), req.resolveHex...)
			tr.rss[len(tr.rss)-1].AddHex(k)
		}

		prevReq = req
	}

	tr.currentReq = tr.requests[tr.reqIndices[0]]
	tr.currentRs = tr.rss[0]
	tr.currentRsChopped = tr.rssChopped[0]
	return startkeys, fixedbits
}

func (tr *ResolverStateful) finaliseRoot() error {
	tr.curr.Reset()
	tr.curr.Write(tr.succ.Bytes())
	tr.succ.Reset()
	if tr.curr.Len() > 0 {
		var err error
		var data GenStructStepData
		if tr.wasIH {
			tr.hashData.Hash = common.BytesToHash(tr.value.Bytes())
			data = &tr.hashData
		} else {
			var storageNode node
			if tr.a.Incarnation == 0 {
				tr.a.Root = EmptyRoot
			} else {
				err = tr.finaliseStorageRoot()
				if err != nil {
					return err
				}

				if tr.hbStorage.hasRoot() {
					tr.a.Root.SetBytes(tr.hbStorage.rootHash().Bytes())
					storageNode = tr.hbStorage.root()
				} else {
					tr.a.Root = EmptyRoot
				}

				tr.hbStorage.Reset()
				tr.wasIHStorage = false
				tr.groupsStorage = nil
				tr.currStorage.Reset()
				tr.succStorage.Reset()
			}
			tr.accData.FieldSet = 0
			if !tr.a.IsEmptyCodeHash() {
				tr.accData.FieldSet |= AccountFieldCodeOnly
			}
			if storageNode != nil || !tr.a.IsEmptyRoot() {
				tr.accData.FieldSet |= AccountFieldStorageOnly
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
			if !tr.a.IsEmptyCodeHash() {
				// the first item ends up deepest on the stack, the second item - on the top
				err = tr.hb.hash(tr.a.CodeHash[:])
				if err != nil {
					return err
				}
			}
			if storageNode != nil || !tr.a.IsEmptyRoot() {
				tr.hb.hashStack = append(tr.hb.hashStack, 0x80+common.HashLength)
				tr.hb.hashStack = append(tr.hb.hashStack, tr.a.Root[:]...)
				tr.hb.nodeStack = append(tr.hb.nodeStack, storageNode)
			}
		}
		tr.groups, err = GenStructStep(tr.currentRsChopped.HashOnly, tr.curr.Bytes(), tr.succ.Bytes(), tr.hb, data, tr.groups, false)
		if err != nil {
			return err
		}
	} else {
		// if only storage resolution required, then no account records
		err := tr.finaliseStorageRoot()
		if err != nil {
			return err
		}
		if tr.hbStorage.hasRoot() {
			hbRoot := tr.hbStorage.root()
			hbHash := tr.hbStorage.rootHash()

			tr.hbStorage.Reset()
			tr.wasIHStorage = false
			tr.groupsStorage = nil
			tr.currStorage.Reset()
			tr.succStorage.Reset()
			return tr.hookFunction(tr.currentReq, hbRoot, hbHash)
		}
		return nil
	}
	if tr.hb.hasRoot() {
		hbRoot := tr.hb.root()
		hbHash := tr.hb.rootHash()
		if err := tr.hookFunction(tr.currentReq, hbRoot, hbHash); err != nil {
			return err
		}

		return nil
	}
	return nil
}

func (tr *ResolverStateful) finaliseStorageRoot() error {
	tr.currStorage.Reset()
	tr.currStorage.Write(tr.succStorage.Bytes())
	tr.succStorage.Reset()
	if tr.currStorage.Len() > 0 {
		var err error
		var data GenStructStepData
		if tr.wasIHStorage {
			tr.hashData.Hash = common.BytesToHash(tr.valueStorage.Bytes())
			data = &tr.hashData
		} else {
			tr.leafData.Value = rlphacks.RlpSerializableBytes(tr.valueStorage.Bytes())
			data = &tr.leafData
		}
		tr.groupsStorage, err = GenStructStep(tr.rssChopped[tr.keyIdx].HashOnly, tr.currStorage.Bytes(), tr.succStorage.Bytes(), tr.hbStorage, data, tr.groupsStorage, false)
		if err != nil {
			return err
		}
	} else {
		// Special case when from IH we took storage root. In this case tr.currStorage.Len() == 0.
		// But still can put value on stack.
		if tr.wasIHStorage {
			if err := tr.hbStorage.hash(tr.valueStorage.Bytes()); err != nil {
				return err
			}
		} else {
			if err := tr.hbStorage.hash(EmptyRoot.Bytes()); err != nil {
				return err
			}
		}
	}

	return nil
}

func (tr *ResolverStateful) RebuildTrie(db ethdb.Database, blockNr uint64, historical bool, trace bool) error {
	if len(tr.requests) == 0 {
		return nil
	}

	defer trieResolveStatefulTimer.UpdateSince(time.Now())
	//trace = true
	tr.trace = trace

	if tr.trace {
		fmt.Printf("----------\n")
		fmt.Printf("RebuildTrie blockNr %d\n", blockNr)
	}

	startkeys, fixedbits := tr.PrepareResolveParams()
	if tr.trace {
		for i := range tr.rss {
			fmt.Printf("tr.rss[%d]->%x\n", i, tr.rss[i].hexes)
		}
		for i := range tr.rssChopped {
			fmt.Printf("tr.rssChopped[%d]->%x\n", i, tr.rssChopped[i].hexes)
		}
		for i := range tr.requests {
			fmt.Printf("req[%d]->%s\n", i, tr.requests[i])
		}
		fmt.Printf("fixedbits: %d\n", fixedbits)
		fmt.Printf("startkey: %x\n", startkeys)
	}
	if db == nil {
		var b strings.Builder
		fmt.Fprintf(&b, "ResolveWithDb(db=nil)\n")
		for i, sk := range startkeys {
			fmt.Fprintf(&b, "sk %x, bits: %d\n", sk, fixedbits[i])
		}
		return fmt.Errorf("unexpected resolution: %s at %s", b.String(), debug.Callers(10))
	}

	var boltDB *bolt.DB
	if hasBolt, ok := db.(ethdb.HasKV); ok {
		boltDB = hasBolt.KV()
	}

	if boltDB == nil {
		return fmt.Errorf("only Bolt supported yet, given: %T", db)
	}

	var err error
	if historical {
		panic("historical data is not implemented")
	} else {
		err = tr.MultiWalk2(boltDB, startkeys, fixedbits, tr.WalkerAccount, tr.WalkerStorage, true)
	}

	if err != nil {
		return err
	}
	if err = tr.finaliseRoot(); err != nil {
		fmt.Println("Err in finalize root, writing down resolve params")
		for i := range tr.rss {
			fmt.Printf("tr.rss[%d]->%x\n", i, tr.rss[i].hexes)
		}
		for i := range tr.rssChopped {
			fmt.Printf("tr.rssChopped[%d]->%x\n", i, tr.rssChopped[i].hexes)
		}
		for i := range tr.requests {
			fmt.Printf("req[%d]->%s\n", i, tr.requests[i])
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

type walker func(isIH bool, keyIdx int, k, v []byte) error

func (tr *ResolverStateful) WalkerStorage(isIH bool, keyIdx int, k, v []byte) error {
	if tr.trace {
		//fmt.Printf("WalkerStorage: isIH=%v keyIdx=%d key=%x value=%x\n", isIH, keyIdx, k, v)
	}

	if keyIdx != tr.keyIdx {
		if err := tr.finaliseRoot(); err != nil {
			return err
		}
		tr.hb.Reset()
		tr.wasIH = false
		tr.groups = nil
		tr.keyIdx = keyIdx
		tr.currentReq = tr.requests[tr.reqIndices[keyIdx]]
		tr.currentRs = tr.rss[keyIdx]
		tr.currentRsChopped = tr.rssChopped[keyIdx]
		tr.curr.Reset()
		tr.hbStorage.Reset()
		tr.wasIHStorage = false
		if tr.trace {
			fmt.Printf("Reset hbStorage from WalkerStorage\n")
		}
		tr.groupsStorage = nil
		tr.currStorage.Reset()
		tr.succStorage.Reset()
		tr.seenAccount = false
	}

	if !isIH && tr.seenAccount {
		// skip storage keys:
		// - if it has wrong incarnation
		// - if it abandoned (account deleted)
		if tr.a.Incarnation == 0 { // skip all storage if incarnation is 0
			if tr.trace {
				fmt.Printf("WalkerStorage: skip %x, because 0 incarnation\n", k)
			}
			return nil
		}

		// skip ih or storage if it has another incarnation
		if !bytes.HasPrefix(k, tr.accAddrHashWithInc) {
			if tr.trace {
				fmt.Printf("WalkerStorage: skip, not match accWithInc=%x\n", tr.accAddrHashWithInc)
			}

			return nil
		}
	}

	if len(v) > 0 {
		tr.currStorage.Reset()
		tr.currStorage.Write(tr.succStorage.Bytes())
		tr.succStorage.Reset()
		skip := tr.currentReq.extResolvePos // how many first nibbles to skip

		if skip < 80 {
			skip = 80
		}

		i := 0
		for _, b := range k {
			if i >= skip {
				tr.succStorage.WriteByte(b / 16)
			}
			i++
			if i >= skip {
				tr.succStorage.WriteByte(b % 16)
			}
			i++
		}

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
			tr.groupsStorage, err = GenStructStep(tr.rssChopped[tr.keyIdx].HashOnly, tr.currStorage.Bytes(), tr.succStorage.Bytes(), tr.hbStorage, data, tr.groupsStorage, false)
			if err != nil {
				return err
			}
		}
		// Remember the current key and value
		tr.wasIHStorage = isIH
		tr.valueStorage.Reset()
		tr.valueStorage.Write(v)
	}

	return nil
}

// Walker - k, v - shouldn't be reused in the caller's code
func (tr *ResolverStateful) WalkerAccount(isIH bool, keyIdx int, k, v []byte) error {
	if tr.trace {
		//fmt.Printf("WalkerAccount: isIH=%v keyIdx=%d key=%x value=%x\n", isIH, keyIdx, k, v)
	}

	if keyIdx != tr.keyIdx {
		if err := tr.finaliseRoot(); err != nil {
			return err
		}
		tr.hb.Reset()
		tr.wasIH = false
		tr.groups = nil
		tr.keyIdx = keyIdx
		tr.currentReq = tr.requests[tr.reqIndices[keyIdx]]
		tr.currentRs = tr.rss[keyIdx]
		tr.currentRsChopped = tr.rssChopped[keyIdx]
		tr.curr.Reset()

		tr.hbStorage.Reset()
		tr.wasIHStorage = false
		if tr.trace {
			//fmt.Printf("Reset hbStorage from WalkerAccount\n")
		}
		tr.groupsStorage = nil
		tr.currStorage.Reset()
		tr.succStorage.Reset()
		tr.seenAccount = false
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
			} else {
				var storageNode node
				if tr.a.Incarnation == 0 {
					tr.a.Root = EmptyRoot
				} else {
					if tr.trace {
						fmt.Printf("Finalising storage root for %x\n", tr.accAddrHashWithInc)
					}
					err = tr.finaliseStorageRoot()
					if err != nil {
						return err
					}

					if tr.hbStorage.hasRoot() {
						tr.a.Root.SetBytes(tr.hbStorage.rootHash().Bytes())
						storageNode = tr.hbStorage.root()
						if tr.trace {
							fmt.Printf("Got root: %x\n", tr.a.Root)
						}
					} else {
						tr.a.Root = EmptyRoot
						if tr.trace {
							fmt.Printf("Got empty root\n")
						}
					}
				}
				tr.accData.FieldSet = 0
				if !tr.a.IsEmptyCodeHash() {
					tr.accData.FieldSet |= AccountFieldCodeOnly
				}
				if storageNode != nil || !tr.a.IsEmptyRoot() {
					tr.accData.FieldSet |= AccountFieldStorageOnly
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
				if !tr.a.IsEmptyCodeHash() {
					// the first item ends up deepest on the stack, the second item - on the top
					err = tr.hb.hash(tr.a.CodeHash[:])
					if err != nil {
						return err
					}
				}
				if storageNode != nil || !tr.a.IsEmptyRoot() {
					tr.hb.hashStack = append(tr.hb.hashStack, 0x80+common.HashLength)
					tr.hb.hashStack = append(tr.hb.hashStack, tr.a.Root[:]...)
					tr.hb.nodeStack = append(tr.hb.nodeStack, storageNode)
				}
			}
			tr.hbStorage.Reset()
			tr.wasIHStorage = false
			if tr.trace {
				//fmt.Printf("Reset hbStorage from WalkerAccount - past\n")
			}
			tr.groupsStorage = nil
			tr.currStorage.Reset()
			tr.succStorage.Reset()
			tr.groups, err = GenStructStep(tr.currentRsChopped.HashOnly, tr.curr.Bytes(), tr.succ.Bytes(), tr.hb, data, tr.groups, false)
			if err != nil {
				return err
			}
		}
		// Remember the current key and value
		tr.wasIH = isIH

		if isIH {
			tr.value.Reset()
			tr.value.Write(v)
			tr.seenAccount = false
			return nil
		}

		if err := tr.a.DecodeForStorage(v); err != nil {
			return fmt.Errorf("fail DecodeForStorage: %w", err)
		}
		tr.seenAccount = true
		copy(tr.accAddrHashWithInc, k)
		binary.BigEndian.PutUint64(tr.accAddrHashWithInc[32:40], ^tr.a.Incarnation)
	}

	return nil
}

// MultiWalk2 - looks similar to db.MultiWalk but works with hardcoded 2-nd bucket IntermediateTrieHashBucket
func (tr *ResolverStateful) MultiWalk2(db *bolt.DB, startkeys [][]byte, fixedbits []uint, accWalker walker, storageWalker walker, isAccount bool) error {
	if len(startkeys) == 0 {
		return nil
	}

	minKeyAsNibbles := pool.GetBuffer(256)
	defer pool.PutBuffer(minKeyAsNibbles)

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
		if len(startkey) <= common.HashLength {
			for ; k != nil && len(k) > common.HashLength; k, v = c.Next() {
			}
		}
		if tr.trace {
			fmt.Printf("c.Seek(%x) = %x\n", startkey, k)
		}

		var ihK, ihV []byte
		if ih != nil {
			ihK, ihV = ih.Seek(startkey)
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
						k, v = c.SeekTo(startkey)
						if len(startkey) <= common.HashLength {
							for ; k != nil && len(k) > common.HashLength; k, v = c.Next() {
							}
						}
						if ih != nil {
							ihK, ihV = ih.SeekTo(startkey)
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
						fixedbytes, mask = ethdb.Bytesmask(fixedbits[rangeIdx])
						startkey = startkeys[rangeIdx]
					}
				}
			}

			if !isIH {
				if len(k) > 32 {
					if err := storageWalker(false, rangeIdx, k, v); err != nil {
						return err
					}
					k, v = c.Next()
				} else {
					if err := accWalker(false, rangeIdx, k, v); err != nil {
						return err
					}
					// Now we know the correct incarnation of the account, and we can skip all irrelevant storage records
					// Since 0 incarnation if 0xfff...fff, and we do not expect any records like that, this automatically
					// skips over all storage items
					k, v = c.Seek(tr.accAddrHashWithInc)
					if ih != nil {
						ihK, ihV = ih.Seek(tr.accAddrHashWithInc)
					}
				}
				continue
			}

			// ih part
			var canUseIntermediateHash bool

			currentReq := tr.requests[tr.reqIndices[rangeIdx]]

			minKeyAsNibbles.Reset()
			DecompressNibbles(minKey, &minKeyAsNibbles.B)

			if len(minKeyAsNibbles.B) < currentReq.extResolvePos {
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			if len(ihK) > common.HashLength {
				canUseIntermediateHash = tr.rss[rangeIdx].HashOnly(append(minKeyAsNibbles.B[:common.HashLength*2], minKeyAsNibbles.B[common.HashLength*2+16:]...))
				if tr.trace {
					fmt.Printf("tr.rss[%d].HashOnly(%x)=%t\n", rangeIdx, minKeyAsNibbles.B[:], canUseIntermediateHash)
				}
			} else {
				canUseIntermediateHash = tr.rss[rangeIdx].HashOnly(minKeyAsNibbles.B[:])
				if tr.trace {
					fmt.Printf("tr.rss[%d].HashOnly(%x)=%t\n", rangeIdx, minKeyAsNibbles.B[:], canUseIntermediateHash)
				}
			}

			if !canUseIntermediateHash { // can't use ih as is, need go to children
				ihK, ihV = ih.Next() // go to children, not to sibling
				continue
			}

			if len(ihK) > 32 {
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

			k, v = c.SeekTo(next)
			if len(startkey) <= common.HashLength {
				for ; k != nil && len(k) > common.HashLength; k, v = c.Next() {
				}
			}
			ihK, ihV = ih.Seek(next)
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
