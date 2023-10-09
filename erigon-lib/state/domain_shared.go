package state

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	math2 "math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/kv/membatch"
	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/types"
)

// KvList sort.Interface to sort write list by keys
type KvList struct {
	Keys []string
	Vals [][]byte
}

func (l *KvList) Push(key string, val []byte) {
	l.Keys = append(l.Keys, key)
	l.Vals = append(l.Vals, val)
}

func (l *KvList) Len() int {
	return len(l.Keys)
}

func (l *KvList) Less(i, j int) bool {
	return l.Keys[i] < l.Keys[j]
}

func (l *KvList) Swap(i, j int) {
	l.Keys[i], l.Keys[j] = l.Keys[j], l.Keys[i]
	l.Vals[i], l.Vals[j] = l.Vals[j], l.Vals[i]
}

type SharedDomains struct {
	*membatch.Mapmutation
	aggCtx *AggregatorV3Context
	roTx   kv.Tx

	txNum    atomic.Uint64
	blockNum atomic.Uint64
	estSize  atomic.Uint64
	trace    bool
	//muMaps   sync.RWMutex
	walLock sync.RWMutex

	account    map[string][]byte
	code       map[string][]byte
	storage    *btree2.Map[string, []byte]
	commitment map[string][]byte
	Account    *Domain
	Storage    *Domain
	Code       *Domain
	Commitment *DomainCommitted
	TracesTo   *InvertedIndex
	LogAddrs   *InvertedIndex
	LogTopics  *InvertedIndex
	TracesFrom *InvertedIndex
}

type HasAggCtx interface {
	AggCtx() *AggregatorV3Context
}

func NewSharedDomains(tx kv.Tx) *SharedDomains {
	var ac *AggregatorV3Context
	if casted, ok := tx.(HasAggCtx); ok {
		ac = casted.AggCtx()
	} else {
		panic(fmt.Sprintf("type %T need AggCtx method", tx))
	}
	if tx == nil {
		panic(fmt.Sprintf("tx is nil"))
	}

	sd := &SharedDomains{
		Mapmutation: membatch.NewHashBatch(tx, ac.a.ctx.Done(), ac.a.dirs.Tmp, ac.a.logger),
		aggCtx:      ac,

		Account:    ac.a.accounts,
		account:    map[string][]byte{},
		Code:       ac.a.code,
		code:       map[string][]byte{},
		Storage:    ac.a.storage,
		storage:    btree2.NewMap[string, []byte](128),
		Commitment: ac.a.commitment,
		commitment: map[string][]byte{},

		TracesTo:   ac.a.tracesTo,
		TracesFrom: ac.a.tracesFrom,
		LogAddrs:   ac.a.logAddrs,
		LogTopics:  ac.a.logTopics,
		roTx:       tx,
	}

	sd.Commitment.ResetFns(sd.branchFn, sd.accountFn, sd.storageFn)
	sd.StartWrites()
	return sd
}

func (sd *SharedDomains) SetInvertedIndices(tracesTo, tracesFrom, logAddrs, logTopics *InvertedIndex) {
	sd.TracesTo = tracesTo
	sd.TracesFrom = tracesFrom
	sd.LogAddrs = logAddrs
	sd.LogTopics = logTopics
}

// aggregator context should call aggCtx.Unwind before this one.
func (sd *SharedDomains) Unwind(ctx context.Context, rwTx kv.RwTx, txUnwindTo uint64) error {
	step := txUnwindTo / sd.aggCtx.a.aggregationStep
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	sd.aggCtx.a.logger.Info("aggregator unwind", "step", step,
		"txUnwindTo", txUnwindTo, "stepsRangeInDB", sd.aggCtx.a.StepsRangeInDBAsStr(rwTx))

	if err := sd.aggCtx.account.Unwind(ctx, rwTx, step, txUnwindTo, math2.MaxUint64, math2.MaxUint64, nil); err != nil {
		return err
	}
	if err := sd.aggCtx.storage.Unwind(ctx, rwTx, step, txUnwindTo, math2.MaxUint64, math2.MaxUint64, nil); err != nil {
		return err
	}
	if err := sd.aggCtx.code.Unwind(ctx, rwTx, step, txUnwindTo, math2.MaxUint64, math2.MaxUint64, nil); err != nil {
		return err
	}
	if err := sd.aggCtx.commitment.Unwind(ctx, rwTx, step, txUnwindTo, math2.MaxUint64, math2.MaxUint64, nil); err != nil {
		return err
	}
	if err := sd.aggCtx.logAddrs.Prune(ctx, rwTx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := sd.aggCtx.logTopics.Prune(ctx, rwTx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := sd.aggCtx.tracesFrom.Prune(ctx, rwTx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	if err := sd.aggCtx.tracesTo.Prune(ctx, rwTx, txUnwindTo, math2.MaxUint64, math2.MaxUint64, logEvery); err != nil {
		return err
	}
	sd.ClearRam(true)

	// TODO what if unwinded to the middle of block? It should cause one more unwind until block beginning or end is not found.
	_, err := sd.SeekCommitment(ctx, rwTx, 0, txUnwindTo)
	return err
}

func (sd *SharedDomains) SeekCommitment(ctx context.Context, tx kv.Tx, fromTx, toTx uint64) (txsFromBlockBeginning uint64, err error) {
	bn, txn, err := sd.Commitment.SeekCommitment(tx, fromTx, toTx, sd.aggCtx.commitment)
	if err != nil {
		return 0, err
	}

	ok, blockNum, err := rawdbv3.TxNums.FindBlockNum(tx, txn)
	if ok {
		if err != nil {
			return txsFromBlockBeginning, fmt.Errorf("failed to find blockNum for txNum %d ok=%t : %w", txn, ok, err)
		}

		firstTxInBlock, err := rawdbv3.TxNums.Min(tx, blockNum)
		if err != nil {
			return txsFromBlockBeginning, fmt.Errorf("failed to find first txNum in block %d : %w", blockNum, err)
		}
		lastTxInBlock, err := rawdbv3.TxNums.Max(tx, blockNum)
		if err != nil {
			return txsFromBlockBeginning, fmt.Errorf("failed to find last txNum in block %d : %w", blockNum, err)
		}
		if sd.trace {
			fmt.Printf("[commitment] found block %d tx %d. DB found block %d, firstTxInBlock %d, lastTxInBlock %d\n", bn, txn, blockNum, firstTxInBlock, lastTxInBlock)
		}
		if txn > firstTxInBlock {
			txn++ // has to move txn cuz state committed at txNum-1 to be included in latest file
			txsFromBlockBeginning = txn - firstTxInBlock
		}
		if sd.trace {
			fmt.Printf("[commitment] block tx range -%d |%d| %d\n", txsFromBlockBeginning, txn, lastTxInBlock-txn)
		}
		if txn == lastTxInBlock {
			blockNum++
		} else {
			txn = firstTxInBlock
		}
	} else {
		blockNum = bn
		if blockNum != 0 {
			txn++
		}
		if sd.trace {
			fmt.Printf("[commitment] found block %d tx %d. No DB info about block first/last txnum has been found\n", blockNum, txn)
		}
	}

	sd.SetBlockNum(blockNum)
	sd.SetTxNum(ctx, txn)
	return
}

func (sd *SharedDomains) ClearRam(resetCommitment bool) {
	//sd.muMaps.Lock()
	//defer sd.muMaps.Unlock()
	sd.account = map[string][]byte{}
	sd.code = map[string][]byte{}
	sd.commitment = map[string][]byte{}
	if resetCommitment {
		sd.Commitment.updates.List(true)
		sd.Commitment.patriciaTrie.Reset()
	}

	sd.storage = btree2.NewMap[string, []byte](128)
	sd.estSize.Store(0)
}

func (sd *SharedDomains) put(table kv.Domain, key string, val []byte) {
	// disable mutex - becuse work on parallel execution postponed after E3 release.
	//sd.muMaps.Lock()
	sd.puts(table, key, val)
	//sd.muMaps.Unlock()
}

func (sd *SharedDomains) puts(table kv.Domain, key string, val []byte) {
	switch table {
	case kv.AccountsDomain:
		if old, ok := sd.account[key]; ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
		sd.account[key] = val
	case kv.CodeDomain:
		if old, ok := sd.code[key]; ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
		sd.code[key] = val
	case kv.StorageDomain:
		if old, ok := sd.storage.Set(key, val); ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
	case kv.CommitmentDomain:
		if old, ok := sd.commitment[key]; ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
		sd.commitment[key] = val
	default:
		panic(fmt.Errorf("sharedDomains put to invalid table %s", table))
	}
}

// Get returns cached value by key. Cache is invalidated when associated WAL is flushed
func (sd *SharedDomains) Get(table kv.Domain, key []byte) (v []byte, ok bool) {
	//sd.muMaps.RLock()
	v, ok = sd.get(table, key)
	//sd.muMaps.RUnlock()
	return v, ok
}

func (sd *SharedDomains) get(table kv.Domain, key []byte) (v []byte, ok bool) {
	keyS := *(*string)(unsafe.Pointer(&key))
	//keyS := string(key)
	switch table {
	case kv.AccountsDomain:
		v, ok = sd.account[keyS]
	case kv.CodeDomain:
		v, ok = sd.code[keyS]
	case kv.StorageDomain:
		v, ok = sd.storage.Get(keyS)
	case kv.CommitmentDomain:
		v, ok = sd.commitment[keyS]
	default:
		panic(table)
	}
	return v, ok
}

func (sd *SharedDomains) SizeEstimate() uint64 {
	return sd.estSize.Load() * 2 // multiply 2 here, to cover data-structures overhead. more precise accounting - expensive.
}

func (sd *SharedDomains) LatestCommitment(prefix []byte) ([]byte, error) {
	v0, ok := sd.Get(kv.CommitmentDomain, prefix)
	if ok {
		return v0, nil
	}
	v, _, err := sd.aggCtx.GetLatest(kv.CommitmentDomain, prefix, nil, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}
	return v, nil
}

func (sd *SharedDomains) LatestCode(addr []byte) ([]byte, error) {
	v0, ok := sd.Get(kv.CodeDomain, addr)
	if ok {
		return v0, nil
	}
	v, _, err := sd.aggCtx.GetLatest(kv.CodeDomain, addr, nil, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("code %x read error: %w", addr, err)
	}
	return v, nil
}

func (sd *SharedDomains) LatestAccount(addr []byte) ([]byte, error) {
	var v0, v []byte
	var err error
	var ok bool

	//defer func() {
	//	curious := "0da27ef618846cfa981516da2891fe0693a54f8418b85c91c384d2c0f4e14727"
	//	if bytes.Equal(hexutility.MustDecodeString(curious), addr) {
	//		fmt.Printf("found %s vDB/File %x vCache %x step %d\n", curious, v, v0, sd.txNum.Load()/sd.Account.aggregationStep)
	//	}
	//}()
	v0, ok = sd.Get(kv.AccountsDomain, addr)
	if ok {
		return v0, nil
	}
	v, _, err = sd.aggCtx.GetLatest(kv.AccountsDomain, addr, nil, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("account %x read error: %w", addr, err)
	}
	return v, nil
}

const CodeSizeTableFake = "CodeSize"

func (sd *SharedDomains) ReadsValid(readLists map[string]*KvList) bool {
	//sd.muMaps.RLock()
	//defer sd.muMaps.RUnlock()

	for table, list := range readLists {
		switch table {
		case string(kv.AccountsDomain):
			m := sd.account
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val) {
						return false
					}
				}
			}
		case string(kv.CodeDomain):
			m := sd.code
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val) {
						return false
					}
				}
			}
		case string(kv.StorageDomain):
			m := sd.storage
			for i, key := range list.Keys {
				if val, ok := m.Get(key); ok {
					if !bytes.Equal(list.Vals[i], val) {
						return false
					}
				}
			}
		case CodeSizeTableFake:
			m := sd.code
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if binary.BigEndian.Uint64(list.Vals[i]) != uint64(len(val)) {
						return false
					}
				}
			}
		default:
			panic(table)
		}
	}

	return true
}

func (sd *SharedDomains) LatestStorage(addrLoc []byte) ([]byte, error) {
	//a := make([]byte, 0, len(addr)+len(loc))
	v0, ok := sd.Get(kv.StorageDomain, addrLoc)
	if ok {
		return v0, nil
	}
	v, _, err := sd.aggCtx.GetLatest(kv.StorageDomain, addrLoc, nil, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("storage %x read error: %w", addrLoc, err)
	}
	return v, nil
}

func (sd *SharedDomains) branchFn(pref []byte) ([]byte, error) {
	v, err := sd.LatestCommitment(pref)
	if err != nil {
		return nil, fmt.Errorf("branchFn failed: %w", err)
	}
	//fmt.Printf("branchFn[sd]: %x: %x\n", pref, v)
	if len(v) == 0 {
		return nil, nil
	}
	// skip touchmap
	return v[2:], nil
}

func (sd *SharedDomains) accountFn(plainKey []byte, cell *commitment.Cell) error {
	encAccount, err := sd.LatestAccount(plainKey)
	if err != nil {
		return fmt.Errorf("accountFn failed: %w", err)
	}
	cell.Nonce = 0
	cell.Balance.Clear()
	if len(encAccount) > 0 {
		nonce, balance, chash := types.DecodeAccountBytesV3(encAccount)
		cell.Nonce = nonce
		cell.Balance.Set(balance)
		if len(chash) > 0 {
			copy(cell.CodeHash[:], chash)
		}
		//fmt.Printf("accountFn[sd]: %x: n=%d b=%d ch=%x\n", plainKey, nonce, balance, chash)
	}

	code, err := sd.LatestCode(plainKey)
	if err != nil {
		return fmt.Errorf("accountFn[sd]: failed to read latest code: %w", err)
	}
	if len(code) > 0 {
		//fmt.Printf("accountFn[sd]: code %x - %x\n", plainKey, code)
		sd.Commitment.updates.keccak.Reset()
		sd.Commitment.updates.keccak.Write(code)
		sd.Commitment.updates.keccak.Read(cell.CodeHash[:])
	} else {
		cell.CodeHash = commitment.EmptyCodeHashArray
	}
	cell.Delete = len(encAccount) == 0 && len(code) == 0
	return nil
}

func (sd *SharedDomains) storageFn(plainKey []byte, cell *commitment.Cell) error {
	// Look in the summary table first
	//addr, loc := splitKey(plainKey)
	enc, err := sd.LatestStorage(plainKey)
	if err != nil {
		return err
	}
	//fmt.Printf("storageFn[sd]: %x|%x - %x\n", addr, loc, enc)
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	cell.Delete = cell.StorageLen == 0
	return nil
}

func (sd *SharedDomains) updateAccountData(addr []byte, account, prevAccount []byte) error {
	addrS := string(addr)
	sd.Commitment.TouchPlainKey(addrS, account, sd.Commitment.TouchAccount)
	sd.put(kv.AccountsDomain, addrS, account)
	return sd.aggCtx.account.PutWithPrev(addr, nil, account, prevAccount)
}

func (sd *SharedDomains) updateAccountCode(addr, code, prevCode []byte) error {
	addrS := string(addr)
	sd.Commitment.TouchPlainKey(addrS, code, sd.Commitment.TouchCode)
	sd.put(kv.CodeDomain, addrS, code)
	if len(code) == 0 {
		return sd.aggCtx.code.DeleteWithPrev(addr, nil, prevCode)
	}
	return sd.aggCtx.code.PutWithPrev(addr, nil, code, prevCode)
}

func (sd *SharedDomains) updateCommitmentData(prefix []byte, data, prev []byte) error {
	sd.put(kv.CommitmentDomain, string(prefix), data)
	return sd.aggCtx.commitment.PutWithPrev(prefix, nil, data, prev)
}

func (sd *SharedDomains) deleteAccount(addr, prev []byte) error {
	addrS := string(addr)
	sd.Commitment.TouchPlainKey(addrS, nil, sd.Commitment.TouchAccount)
	sd.put(kv.AccountsDomain, addrS, nil)
	if err := sd.aggCtx.account.DeleteWithPrev(addr, nil, prev); err != nil {
		return err
	}

	// commitment delete already has been applied via account
	pc, err := sd.LatestCode(addr)
	if err != nil {
		return err
	}
	if len(pc) > 0 {
		sd.Commitment.TouchPlainKey(addrS, nil, sd.Commitment.TouchCode)
		sd.put(kv.CodeDomain, addrS, nil)
		if err := sd.aggCtx.code.DeleteWithPrev(addr, nil, pc); err != nil {
			return err
		}
	}

	// bb, _ := hex.DecodeString("d96d1b15d6bec8e7d37038237b1e913ad99f7dee")
	// if bytes.Equal(bb, addr) {
	// 	fmt.Printf("delete account %x \n", addr)
	// }

	type pair struct{ k, v []byte }
	tombs := make([]pair, 0, 8)
	err = sd.IterateStoragePrefix(addr, func(k, v []byte) error {
		tombs = append(tombs, pair{k, v})
		return nil
	})
	if err != nil {
		return err
	}

	for _, tomb := range tombs {
		ks := string(tomb.k)
		sd.put(kv.StorageDomain, ks, nil)
		sd.Commitment.TouchPlainKey(ks, nil, sd.Commitment.TouchStorage)
		err = sd.aggCtx.storage.DeleteWithPrev(tomb.k, nil, tomb.v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sd *SharedDomains) writeAccountStorage(addr, loc []byte, value, preVal []byte) error {
	composite := addr
	if loc != nil { // if caller passed already `composite` key, then just use it. otherwise join parts
		composite = make([]byte, 0, len(addr)+len(loc))
		composite = append(append(composite, addr...), loc...)
	}
	compositeS := string(composite)
	sd.Commitment.TouchPlainKey(compositeS, value, sd.Commitment.TouchStorage)
	sd.put(kv.StorageDomain, compositeS, value)
	if len(value) == 0 {
		return sd.aggCtx.storage.DeleteWithPrev(composite, nil, preVal)
	}
	return sd.aggCtx.storage.PutWithPrev(composite, nil, value, preVal)
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte) (err error) {
	switch table {
	case kv.LogAddrIdx, kv.TblLogAddressIdx:
		err = sd.aggCtx.logAddrs.Add(key)
	case kv.LogTopicIdx, kv.TblLogTopicsIdx, kv.LogTopicIndex:
		err = sd.aggCtx.logTopics.Add(key)
	case kv.TblTracesToIdx:
		err = sd.aggCtx.tracesTo.Add(key)
	case kv.TblTracesFromIdx:
		err = sd.aggCtx.tracesFrom.Add(key)
	default:
		panic(fmt.Errorf("unknown shared index %s", table))
	}
	return err
}

func (sd *SharedDomains) SetContext(ctx *AggregatorV3Context) {
	sd.aggCtx = ctx
}

func (sd *SharedDomains) SetTx(tx kv.RwTx) {
	sd.roTx = tx
}

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if aggregationStep is reached
func (sd *SharedDomains) SetTxNum(ctx context.Context, txNum uint64) {
	if txNum%sd.Account.aggregationStep == 0 { //
		// We do not update txNum before commitment cuz otherwise committed state will be in the beginning of next file, not in the latest.
		// That's why we need to make txnum++ on SeekCommitment to get exact txNum for the latest committed state.
		fmt.Printf("[commitment] running due to txNum reached aggregation step %d\n", txNum/sd.Account.aggregationStep)
		_, err := sd.ComputeCommitment(ctx, true, sd.trace)
		if err != nil {
			panic(err)
		}
	}

	sd.txNum.Store(txNum)
	sd.aggCtx.account.SetTxNum(txNum)
	sd.aggCtx.code.SetTxNum(txNum)
	sd.aggCtx.storage.SetTxNum(txNum)
	sd.aggCtx.commitment.SetTxNum(txNum)
	sd.aggCtx.tracesTo.SetTxNum(txNum)
	sd.aggCtx.tracesFrom.SetTxNum(txNum)
	sd.aggCtx.logAddrs.SetTxNum(txNum)
	sd.aggCtx.logTopics.SetTxNum(txNum)
}

func (sd *SharedDomains) TxNum() uint64 { return sd.txNum.Load() }

func (sd *SharedDomains) BlockNum() uint64 { return sd.blockNum.Load() }

func (sd *SharedDomains) SetBlockNum(blockNum uint64) {
	sd.blockNum.Store(blockNum)
}

func (sd *SharedDomains) ComputeCommitment(ctx context.Context, saveStateAfter, trace bool) (rootHash []byte, err error) {
	// if commitment mode is Disabled, there will be nothing to compute on.
	mxCommitmentRunning.Inc()
	defer mxCommitmentRunning.Dec()

	// if commitment mode is Disabled, there will be nothing to compute on.
	rootHash, branchNodeUpdates, err := sd.Commitment.ComputeCommitment(ctx, trace)
	if err != nil {
		return nil, err
	}

	defer func(t time.Time) { mxCommitmentWriteTook.UpdateDuration(t) }(time.Now())

	keys := make([][]byte, 0, len(branchNodeUpdates))
	for k, _ := range branchNodeUpdates {
		keys = append(keys, []byte(k))
	}
	sort.SliceStable(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })

	for _, key := range keys {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		prefix := key
		update := branchNodeUpdates[string(prefix)]

		stateValue, err := sd.LatestCommitment(prefix)
		if err != nil {
			return nil, err
		}
		stated := commitment.BranchData(stateValue)
		merged, err := sd.Commitment.branchMerger.Merge(stated, update)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(stated, merged) {
			continue
		}
		if trace {
			fmt.Printf("sd computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
		}

		if err = sd.updateCommitmentData(prefix, merged, stated); err != nil {
			return nil, err
		}
		mxCommitmentBranchUpdates.Inc()
	}
	if saveStateAfter {
		if err := sd.Commitment.storeCommitmentState(sd.aggCtx.commitment, sd.blockNum.Load(), rootHash); err != nil {
			return nil, err
		}
	}
	return rootHash, nil
}

// IterateStoragePrefix iterates over key-value pairs of the storage domain that start with given prefix
// Such iteration is not intended to be used in public API, therefore it uses read-write transaction
// inside the domain. Another version of this for public API use needs to be created, that uses
// roTx instead and supports ending the iterations before it reaches the end.
func (sd *SharedDomains) IterateStoragePrefix(prefix []byte, it func(k []byte, v []byte) error) error {
	sc := sd.Storage.MakeContext()
	defer sc.Close()

	sd.Storage.stats.FilesQueries.Add(1)

	var cp CursorHeap
	cpPtr := &cp
	heap.Init(cpPtr)
	var k, v []byte
	var err error

	iter := sd.storage.Iter()
	if iter.Seek(string(prefix)) {
		kx := iter.Key()
		v = iter.Value()
		k = []byte(kx)

		if len(kx) > 0 && bytes.HasPrefix(k, prefix) {
			heap.Push(cpPtr, &CursorItem{t: RAM_CURSOR, key: common.Copy(k), val: common.Copy(v), iter: iter, endTxNum: sd.txNum.Load(), reverse: true})
		}
	}

	roTx := sd.roTx
	keysCursor, err := roTx.CursorDupSort(sd.Storage.keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, prefix) {
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * sd.Storage.aggregationStep
		if v, err = roTx.GetOne(sd.Storage.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(cpPtr, &CursorItem{t: DB_CURSOR, key: k, val: v, c: keysCursor, endTxNum: txNum, reverse: true})
	}

	sctx := sd.aggCtx.storage
	for _, item := range sctx.files {
		gg := NewArchiveGetter(item.src.decompressor.MakeGetter(), sd.Storage.compression)
		cursor, err := item.src.bindex.Seek(gg, prefix)
		if err != nil {
			return err
		}
		if cursor == nil {
			continue
		}
		cursor.getter = gg

		key := cursor.Key()
		if key != nil && bytes.HasPrefix(key, prefix) {
			val := cursor.Value()
			heap.Push(cpPtr, &CursorItem{t: FILE_CURSOR, key: key, val: val, btCursor: cursor, endTxNum: item.endTxNum, reverse: true})
		}
	}

	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := heap.Pop(cpPtr).(*CursorItem)
			switch ci1.t {
			case RAM_CURSOR:
				if ci1.iter.Next() {
					k = []byte(ci1.iter.Key())
					if k != nil && bytes.HasPrefix(k, prefix) {
						ci1.key = common.Copy(k)
						ci1.val = common.Copy(ci1.iter.Value())
						heap.Push(cpPtr, ci1)
					}
				}
			case FILE_CURSOR:
				if UseBtree || UseBpsTree {
					if ci1.btCursor.Next() {
						ci1.key = ci1.btCursor.Key()
						if ci1.key != nil && bytes.HasPrefix(ci1.key, prefix) {
							ci1.val = ci1.btCursor.Value()
							heap.Push(cpPtr, ci1)
						}
					}
				} else {
					ci1.dg.Reset(ci1.latestOffset)
					if !ci1.dg.HasNext() {
						break
					}
					key, _ := ci1.dg.Next(nil)
					if key != nil && bytes.HasPrefix(key, prefix) {
						ci1.key = key
						ci1.val, ci1.latestOffset = ci1.dg.Next(nil)
						heap.Push(cpPtr, ci1)
					}
				}
			case DB_CURSOR:
				k, v, err = ci1.c.NextNoDup()
				if err != nil {
					return err
				}

				if k != nil && bytes.HasPrefix(k, prefix) {
					ci1.key = common.Copy(k)
					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = roTx.GetOne(sd.Storage.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					heap.Push(cpPtr, ci1)
				}
			}
		}
		if len(lastVal) > 0 {
			if err := it(lastKey, lastVal); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sd *SharedDomains) Close() {
	sd.FinishWrites()
	sd.account = nil
	sd.code = nil
	sd.storage = nil
	sd.commitment = nil
	sd.LogAddrs = nil
	sd.LogTopics = nil
	sd.TracesFrom = nil
	sd.TracesTo = nil
}

// StartWrites - pattern: `defer domains.StartWrites().FinishWrites()`
func (sd *SharedDomains) StartWrites() *SharedDomains {
	sd.walLock.Lock()
	defer sd.walLock.Unlock()

	sd.aggCtx.account.StartWrites()
	sd.aggCtx.storage.StartWrites()
	sd.aggCtx.code.StartWrites()
	sd.aggCtx.commitment.StartWrites()
	sd.aggCtx.logAddrs.StartWrites()
	sd.aggCtx.logTopics.StartWrites()
	sd.aggCtx.tracesFrom.StartWrites()
	sd.aggCtx.tracesTo.StartWrites()

	if sd.account == nil {
		sd.account = map[string][]byte{}
	}
	if sd.commitment == nil {
		sd.commitment = map[string][]byte{}
	}
	if sd.code == nil {
		sd.code = map[string][]byte{}
	}
	if sd.storage == nil {
		sd.storage = btree2.NewMap[string, []byte](128)
	}
	return sd
}

func (sd *SharedDomains) StartUnbufferedWrites() *SharedDomains {
	sd.walLock.Lock()
	defer sd.walLock.Unlock()

	sd.aggCtx.account.StartUnbufferedWrites()
	sd.aggCtx.storage.StartUnbufferedWrites()
	sd.aggCtx.code.StartUnbufferedWrites()
	sd.aggCtx.commitment.StartUnbufferedWrites()
	sd.aggCtx.logAddrs.StartUnbufferedWrites()
	sd.aggCtx.logTopics.StartUnbufferedWrites()
	sd.aggCtx.tracesFrom.StartUnbufferedWrites()
	sd.aggCtx.tracesTo.StartUnbufferedWrites()

	if sd.account == nil {
		sd.account = map[string][]byte{}
	}
	if sd.commitment == nil {
		sd.commitment = map[string][]byte{}
	}
	if sd.code == nil {
		sd.code = map[string][]byte{}
	}
	if sd.storage == nil {
		sd.storage = btree2.NewMap[string, []byte](128)
	}

	return sd
}

func (sd *SharedDomains) FinishWrites() {
	sd.walLock.Lock()
	defer sd.walLock.Unlock()

	sd.aggCtx.account.FinishWrites()
	sd.aggCtx.storage.FinishWrites()
	sd.aggCtx.code.FinishWrites()
	sd.aggCtx.commitment.FinishWrites()
	sd.aggCtx.logAddrs.FinishWrites()
	sd.aggCtx.logTopics.FinishWrites()
	sd.aggCtx.tracesFrom.FinishWrites()
	sd.aggCtx.tracesTo.FinishWrites()
}

func (sd *SharedDomains) BatchHistoryWriteStart() *SharedDomains {
	sd.walLock.RLock()
	return sd
}

func (sd *SharedDomains) BatchHistoryWriteEnd() {
	sd.walLock.RUnlock()
}

func (sd *SharedDomains) DiscardHistory() {
	sd.aggCtx.account.DiscardHistory()
	sd.aggCtx.storage.DiscardHistory()
	sd.aggCtx.code.DiscardHistory()
	sd.aggCtx.commitment.DiscardHistory()
	sd.aggCtx.logAddrs.DiscardHistory()
	sd.aggCtx.logTopics.DiscardHistory()
	sd.aggCtx.tracesFrom.DiscardHistory()
	sd.aggCtx.tracesTo.DiscardHistory()
}
func (sd *SharedDomains) rotate() []flusher {
	sd.walLock.Lock()
	defer sd.walLock.Unlock()
	mut := sd.Mapmutation
	sd.Mapmutation = membatch.NewHashBatch(sd.roTx, sd.aggCtx.a.ctx.Done(), sd.aggCtx.a.dirs.Tmp, sd.aggCtx.a.logger)
	return []flusher{
		sd.aggCtx.account.Rotate(),
		sd.aggCtx.storage.Rotate(),
		sd.aggCtx.code.Rotate(),
		sd.aggCtx.commitment.Rotate(),
		sd.aggCtx.logAddrs.Rotate(),
		sd.aggCtx.logTopics.Rotate(),
		sd.aggCtx.tracesFrom.Rotate(),
		sd.aggCtx.tracesTo.Rotate(),
		mut,
	}
}

func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	flushers := sd.rotate()
	for _, f := range flushers {
		if err := f.Flush(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

// TemporalDomain satisfaction
func (sd *SharedDomains) DomainGet(name kv.Domain, k, k2 []byte) (v []byte, err error) {
	switch name {
	case kv.AccountsDomain:
		return sd.LatestAccount(k)
	case kv.StorageDomain:
		if k2 != nil {
			k = append(k, k2...)
		}
		return sd.LatestStorage(k)
	case kv.CodeDomain:
		return sd.LatestCode(k)
	case kv.CommitmentDomain:
		return sd.LatestCommitment(k)
	default:
		panic(name)
	}
}

// DomainPut
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte) error {
	if val == nil {
		return sd.DomainDel(domain, k1, k2, prevVal)
	}
	if prevVal == nil {
		var err error
		prevVal, err = sd.DomainGet(domain, k1, k2)
		if err != nil {
			return err
		}
	}
	switch domain {
	case kv.AccountsDomain:
		return sd.updateAccountData(k1, val, prevVal)
	case kv.StorageDomain:
		return sd.writeAccountStorage(k1, k2, val, prevVal)
	case kv.CodeDomain:
		if bytes.Equal(prevVal, val) {
			return nil
		}
		return sd.updateAccountCode(k1, val, prevVal)
	case kv.CommitmentDomain:
		return sd.updateCommitmentData(k1, val, prevVal)
	default:
		panic(domain)
	}
}

// DomainDel
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainDel(domain kv.Domain, k1, k2 []byte, prevVal []byte) error {
	if prevVal == nil {
		var err error
		prevVal, err = sd.DomainGet(domain, k1, k2)
		if err != nil {
			return err
		}
	}
	switch domain {
	case kv.AccountsDomain:
		return sd.deleteAccount(k1, prevVal)
	case kv.StorageDomain:
		return sd.writeAccountStorage(k1, k2, nil, prevVal)
	case kv.CodeDomain:
		if bytes.Equal(prevVal, nil) {
			return nil
		}
		return sd.updateAccountCode(k1, nil, prevVal)
	case kv.CommitmentDomain:
		return sd.updateCommitmentData(k1, nil, prevVal)
	default:
		panic(domain)
	}
}
