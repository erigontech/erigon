package state

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/assert"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/log/v3"
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
	noFlush int

	aggCtx *AggregatorRoTx
	sdCtx  *SharedDomainsCommitmentContext
	roTx   kv.Tx
	logger log.Logger

	txNum    uint64
	blockNum atomic.Uint64
	estSize  int
	trace    bool //nolint
	//muMaps   sync.RWMutex
	//walLock sync.RWMutex

	account    map[string][]byte
	code       map[string][]byte
	storage    *btree2.Map[string, []byte]
	commitment map[string][]byte

	dWriter          [kv.DomainLen]*domainBufferedWriter
	logAddrsWriter   *invertedIndexBufferedWriter
	logTopicsWriter  *invertedIndexBufferedWriter
	tracesFromWriter *invertedIndexBufferedWriter
	tracesToWriter   *invertedIndexBufferedWriter
}

type HasAggCtx interface {
	AggCtx() interface{}
}

func NewSharedDomains(tx kv.Tx, logger log.Logger) (*SharedDomains, error) {
	var ac *AggregatorRoTx
	if casted, ok := tx.(HasAggCtx); ok {
		ac = casted.AggCtx().(*AggregatorRoTx)
	} else {
		return nil, fmt.Errorf("type %T need AggCtx method", tx)
	}
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}

	sd := &SharedDomains{
		logger: logger,
		aggCtx: ac,
		roTx:   tx,
		//trace:            true,
		logAddrsWriter:   ac.logAddrs.NewWriter(),
		logTopicsWriter:  ac.logTopics.NewWriter(),
		tracesFromWriter: ac.tracesFrom.NewWriter(),
		tracesToWriter:   ac.tracesTo.NewWriter(),

		account:    map[string][]byte{},
		commitment: map[string][]byte{},
		code:       map[string][]byte{},
		storage:    btree2.NewMap[string, []byte](128),
	}
	for id, d := range ac.d {
		sd.dWriter[id] = d.NewWriter()
	}

	sd.SetTxNum(0)
	sd.sdCtx = NewSharedDomainsCommitmentContext(sd, CommitmentModeDirect, commitment.VariantHexPatriciaTrie)

	if _, err := sd.SeekCommitment(context.Background(), tx); err != nil {
		return nil, fmt.Errorf("SeekCommitment: %w", err)
	}
	return sd, nil
}

func (sd *SharedDomains) AggCtx() interface{} { return sd.aggCtx }

// aggregator context should call aggCtx.Unwind before this one.
func (sd *SharedDomains) Unwind(ctx context.Context, rwTx kv.RwTx, blockUnwindTo, txUnwindTo uint64) error {
	step := txUnwindTo / sd.aggCtx.a.StepSize()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	sd.aggCtx.a.logger.Info("aggregator unwind", "step", step,
		"txUnwindTo", txUnwindTo, "stepsRangeInDB", sd.aggCtx.a.StepsRangeInDBAsStr(rwTx))
	//fmt.Printf("aggregator unwind step %d txUnwindTo %d stepsRangeInDB %s\n", step, txUnwindTo, sd.aggCtx.a.StepsRangeInDBAsStr(rwTx))

	if err := sd.Flush(ctx, rwTx); err != nil {
		return err
	}

	withWarmup := false
	for _, d := range sd.aggCtx.d {
		if err := d.Unwind(ctx, rwTx, step, txUnwindTo); err != nil {
			return err
		}
	}
	if _, err := sd.aggCtx.logAddrs.Prune(ctx, rwTx, txUnwindTo, math.MaxUint64, math.MaxUint64, logEvery, true, withWarmup, nil); err != nil {
		return err
	}
	if _, err := sd.aggCtx.logTopics.Prune(ctx, rwTx, txUnwindTo, math.MaxUint64, math.MaxUint64, logEvery, true, withWarmup, nil); err != nil {
		return err
	}
	if _, err := sd.aggCtx.tracesFrom.Prune(ctx, rwTx, txUnwindTo, math.MaxUint64, math.MaxUint64, logEvery, true, withWarmup, nil); err != nil {
		return err
	}
	if _, err := sd.aggCtx.tracesTo.Prune(ctx, rwTx, txUnwindTo, math.MaxUint64, math.MaxUint64, logEvery, true, withWarmup, nil); err != nil {
		return err
	}

	sd.ClearRam(true)
	sd.SetTxNum(txUnwindTo)
	sd.SetBlockNum(blockUnwindTo)
	return sd.Flush(ctx, rwTx)
}

func (sd *SharedDomains) rebuildCommitment(ctx context.Context, roTx kv.Tx, blockNum uint64) ([]byte, error) {
	it, err := sd.aggCtx.AccountHistoryRange(int(sd.TxNum()), math.MaxInt64, order.Asc, -1, roTx)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sd.sdCtx.TouchPlainKey(string(k), nil, sd.sdCtx.TouchAccount)
	}

	it, err = sd.aggCtx.StorageHistoryRange(int(sd.TxNum()), math.MaxInt64, order.Asc, -1, roTx)
	if err != nil {
		return nil, err
	}

	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		sd.sdCtx.TouchPlainKey(string(k), nil, sd.sdCtx.TouchStorage)
	}

	sd.sdCtx.Reset()
	return sd.ComputeCommitment(ctx, true, blockNum, "rebuild commit")
}

// SeekCommitment lookups latest available commitment and sets it as current
func (sd *SharedDomains) SeekCommitment(ctx context.Context, tx kv.Tx) (txsFromBlockBeginning uint64, err error) {
	bn, txn, ok, err := sd.sdCtx.SeekCommitment(tx, sd.aggCtx.d[kv.CommitmentDomain], 0, math.MaxUint64)
	if err != nil {
		return 0, err
	}
	if ok {
		if bn > 0 {
			lastBn, _, err := rawdbv3.TxNums.Last(tx)
			if err != nil {
				return 0, err
			}
			if lastBn < bn {
				return 0, fmt.Errorf("TxNums index is at block %d and behind commitment %d. Likely it means that `domain snaps` are ahead of `block snaps`", lastBn, bn)
			}
		}
		sd.SetBlockNum(bn)
		sd.SetTxNum(txn)
		return 0, nil
	}
	// handle case when we have no commitment, but have executed blocks
	bnBytes, err := tx.GetOne(kv.SyncStageProgress, []byte("Execution")) //TODO: move stages to erigon-lib
	if err != nil {
		return 0, err
	}
	if len(bnBytes) == 8 {
		bn = binary.BigEndian.Uint64(bnBytes)
		txn, err = rawdbv3.TxNums.Max(tx, bn)
		if err != nil {
			return 0, err
		}
	}
	if bn == 0 && txn == 0 {
		sd.SetBlockNum(0)
		sd.SetTxNum(0)
		return 0, nil
	}
	sd.SetBlockNum(bn)
	sd.SetTxNum(txn)
	newRh, err := sd.rebuildCommitment(ctx, tx, bn)
	if err != nil {
		return 0, err
	}
	if bytes.Equal(newRh, commitment.EmptyRootHash) {
		sd.SetBlockNum(0)
		sd.SetTxNum(0)
		return 0, nil
	}
	if sd.trace {
		fmt.Printf("rebuilt commitment %x %d %d\n", newRh, sd.TxNum(), sd.BlockNum())
	}
	sd.SetBlockNum(bn)
	sd.SetTxNum(txn)
	return 0, nil
}

func (sd *SharedDomains) ClearRam(resetCommitment bool) {
	//sd.muMaps.Lock()
	//defer sd.muMaps.Unlock()
	sd.account = map[string][]byte{}
	sd.code = map[string][]byte{}
	sd.commitment = map[string][]byte{}
	if resetCommitment {
		sd.sdCtx.updates.List(true)
		sd.sdCtx.Reset()
	}

	sd.storage = btree2.NewMap[string, []byte](128)
	sd.estSize = 0
}

func (sd *SharedDomains) put(table kv.Domain, key string, val []byte) {
	// disable mutex - because work on parallel execution postponed after E3 release.
	//sd.muMaps.Lock()
	switch table {
	case kv.AccountsDomain:
		if old, ok := sd.account[key]; ok {
			sd.estSize += len(val) - len(old)
		} else {
			sd.estSize += len(key) + len(val)
		}
		sd.account[key] = val
	case kv.CodeDomain:
		if old, ok := sd.code[key]; ok {
			sd.estSize += len(val) - len(old)
		} else {
			sd.estSize += len(key) + len(val)
		}
		sd.code[key] = val
	case kv.StorageDomain:
		if old, ok := sd.storage.Set(key, val); ok {
			sd.estSize += len(val) - len(old)
		} else {
			sd.estSize += len(key) + len(val)
		}
	case kv.CommitmentDomain:
		if old, ok := sd.commitment[key]; ok {
			sd.estSize += len(val) - len(old)
		} else {
			sd.estSize += len(key) + len(val)
		}
		sd.commitment[key] = val
	default:
		panic(fmt.Errorf("sharedDomains put to invalid table %s", table))
	}
	//sd.muMaps.Unlock()
}

// Get returns cached value by key. Cache is invalidated when associated WAL is flushed
func (sd *SharedDomains) Get(table kv.Domain, key []byte) (v []byte, ok bool) {
	//sd.muMaps.RLock()
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
	//sd.muMaps.RUnlock()
	return v, ok
}

func (sd *SharedDomains) SizeEstimate() uint64 {
	//sd.muMaps.RLock()
	//defer sd.muMaps.RUnlock()
	return uint64(sd.estSize) * 2 // multiply 2 here, to cover data-structures overhead. more precise accounting - expensive.
}

func (sd *SharedDomains) LatestCommitment(prefix []byte) ([]byte, uint64, error) {
	if v, ok := sd.Get(kv.CommitmentDomain, prefix); ok {
		// sd cache values as is (without transformation) so safe to return
		return v, 0, nil
	}
	v, step, found, err := sd.aggCtx.d[kv.CommitmentDomain].getLatestFromDb(prefix, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}
	if found {
		// db store values as is (without transformation) so safe to return
		return v, step, nil
	}

	// GetfromFiles doesn't provide same semantics as getLatestFromDB - it returns start/end tx
	// of file where the value is stored (not exact step when kv has been set)
	v, _, startTx, endTx, err := sd.aggCtx.d[kv.CommitmentDomain].getFromFiles(prefix)
	if err != nil {
		return nil, 0, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}

	if !sd.aggCtx.a.commitmentValuesTransform || bytes.Equal(prefix, keyCommitmentState) {
		return v, endTx, nil
	}

	// replace shortened keys in the branch with full keys to allow HPH work seamlessly
	rv, err := sd.replaceShortenedKeysInBranch(prefix, commitment.BranchData(v), startTx, endTx)
	if err != nil {
		return nil, 0, err
	}
	return rv, endTx / sd.aggCtx.a.StepSize(), nil
}

// replaceShortenedKeysInBranch replaces shortened keys in the branch with full keys
func (sd *SharedDomains) replaceShortenedKeysInBranch(prefix []byte, branch commitment.BranchData, fStartTxNum uint64, fEndTxNum uint64) (commitment.BranchData, error) {
	if !sd.aggCtx.d[kv.CommitmentDomain].d.replaceKeysInValues && sd.aggCtx.a.commitmentValuesTransform {
		panic("domain.replaceKeysInValues is disabled, but agg.commitmentValuesTransform is enabled")
	}

	if !sd.aggCtx.a.commitmentValuesTransform ||
		len(branch) == 0 ||
		sd.aggCtx.minimaxTxNumInDomainFiles(false) == 0 ||
		bytes.Equal(prefix, keyCommitmentState) {

		return branch, nil // do not transform, return as is
	}

	return branch.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		if isStorage {
			if len(key) == length.Addr+length.Hash {
				return nil, nil // save storage key as is
			}
			// Optimised key referencing a state file record (file number and offset within the file)
			storagePlainKey, found := sd.aggCtx.d[kv.StorageDomain].lookupByShortenedKey(key, fStartTxNum, fEndTxNum)
			if !found {
				s0, s1 := fStartTxNum/sd.aggCtx.a.StepSize(), fEndTxNum/sd.aggCtx.a.StepSize()
				oft := decodeShorterKey(key)
				sd.logger.Crit("replace back lost storage full key", "shortened", fmt.Sprintf("%x", key),
					"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, oft))
				return nil, fmt.Errorf("replace back lost storage full key: %x", key)
			}
			return storagePlainKey, nil
		}

		if len(key) == length.Addr {
			return nil, nil // save account key as is
		}

		apkBuf, found := sd.aggCtx.d[kv.AccountsDomain].lookupByShortenedKey(key, fStartTxNum, fEndTxNum)
		if !found {
			oft := decodeShorterKey(key)
			s0, s1 := fStartTxNum/sd.aggCtx.a.StepSize(), fEndTxNum/sd.aggCtx.a.StepSize()
			sd.logger.Crit("replace back lost account full key", "shortened", fmt.Sprintf("%x", key),
				"decoded", fmt.Sprintf("step %d-%d; offt %d", s0, s1, oft))
			return nil, fmt.Errorf("replace back lost account full key: %x", key)
		}
		return apkBuf, nil
	})
}

func (sd *SharedDomains) LatestCode(addr []byte) ([]byte, uint64, error) {
	if v, ok := sd.Get(kv.CodeDomain, addr); ok {
		return v, 0, nil
	}
	v, step, _, err := sd.aggCtx.GetLatest(kv.CodeDomain, addr, nil, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("code %x read error: %w", addr, err)
	}
	return v, step, nil
}

func (sd *SharedDomains) LatestAccount(addr []byte) ([]byte, uint64, error) {
	if v, ok := sd.Get(kv.AccountsDomain, addr); ok {
		return v, 0, nil
	}
	v, step, _, err := sd.aggCtx.GetLatest(kv.AccountsDomain, addr, nil, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("account %x read error: %w", addr, err)
	}
	return v, step, nil
}

const CodeSizeTableFake = "CodeSize"

func (sd *SharedDomains) ReadsValid(readLists map[string]*KvList) bool {
	//sd.muMaps.RLock()
	//defer sd.muMaps.RUnlock()

	for table, list := range readLists {
		switch table {
		case kv.AccountsDomain.String():
			m := sd.account
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val) {
						return false
					}
				}
			}
		case kv.CodeDomain.String():
			m := sd.code
			for i, key := range list.Keys {
				if val, ok := m[key]; ok {
					if !bytes.Equal(list.Vals[i], val) {
						return false
					}
				}
			}
		case kv.StorageDomain.String():
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

func (sd *SharedDomains) LatestStorage(addrLoc []byte) ([]byte, uint64, error) {
	if v, ok := sd.Get(kv.StorageDomain, addrLoc); ok {
		return v, 0, nil
	}
	v, step, _, err := sd.aggCtx.GetLatest(kv.StorageDomain, addrLoc, nil, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", addrLoc, err)
	}
	return v, step, nil
}

func (sd *SharedDomains) updateAccountData(addr []byte, account, prevAccount []byte, prevStep uint64) error {
	addrS := string(addr)
	sd.sdCtx.TouchPlainKey(addrS, account, sd.sdCtx.TouchAccount)
	sd.put(kv.AccountsDomain, addrS, account)
	return sd.dWriter[kv.AccountsDomain].PutWithPrev(addr, nil, account, prevAccount, prevStep)
}

func (sd *SharedDomains) updateAccountCode(addr, code, prevCode []byte, prevStep uint64) error {
	addrS := string(addr)
	sd.sdCtx.TouchPlainKey(addrS, code, sd.sdCtx.TouchCode)
	sd.put(kv.CodeDomain, addrS, code)
	if len(code) == 0 {
		return sd.dWriter[kv.CodeDomain].DeleteWithPrev(addr, nil, prevCode, prevStep)
	}
	return sd.dWriter[kv.CodeDomain].PutWithPrev(addr, nil, code, prevCode, prevStep)
}

func (sd *SharedDomains) updateCommitmentData(prefix []byte, data, prev []byte, prevStep uint64) error {
	sd.put(kv.CommitmentDomain, string(prefix), data)
	return sd.dWriter[kv.CommitmentDomain].PutWithPrev(prefix, nil, data, prev, prevStep)
}

func (sd *SharedDomains) deleteAccount(addr, prev []byte, prevStep uint64) error {
	addrS := string(addr)
	if err := sd.DomainDelPrefix(kv.StorageDomain, addr); err != nil {
		return err
	}

	// commitment delete already has been applied via account
	if err := sd.DomainDel(kv.CodeDomain, addr, nil, nil, prevStep); err != nil {
		return err
	}

	sd.sdCtx.TouchPlainKey(addrS, nil, sd.sdCtx.TouchAccount)
	sd.put(kv.AccountsDomain, addrS, nil)
	if err := sd.dWriter[kv.AccountsDomain].DeleteWithPrev(addr, nil, prev, prevStep); err != nil {
		return err
	}

	return nil
}

func (sd *SharedDomains) writeAccountStorage(addr, loc []byte, value, preVal []byte, prevStep uint64) error {
	composite := addr
	if loc != nil { // if caller passed already `composite` key, then just use it. otherwise join parts
		composite = make([]byte, 0, len(addr)+len(loc))
		composite = append(append(composite, addr...), loc...)
	}
	compositeS := string(composite)
	sd.sdCtx.TouchPlainKey(compositeS, value, sd.sdCtx.TouchStorage)
	sd.put(kv.StorageDomain, compositeS, value)
	return sd.dWriter[kv.StorageDomain].PutWithPrev(composite, nil, value, preVal, prevStep)
}
func (sd *SharedDomains) delAccountStorage(addr, loc []byte, preVal []byte, prevStep uint64) error {
	composite := addr
	if loc != nil { // if caller passed already `composite` key, then just use it. otherwise join parts
		composite = make([]byte, 0, len(addr)+len(loc))
		composite = append(append(composite, addr...), loc...)
	}
	compositeS := string(composite)
	sd.sdCtx.TouchPlainKey(compositeS, nil, sd.sdCtx.TouchStorage)
	sd.put(kv.StorageDomain, compositeS, nil)
	return sd.dWriter[kv.StorageDomain].DeleteWithPrev(composite, nil, preVal, prevStep)
}

func (sd *SharedDomains) IndexAdd(table kv.InvertedIdx, key []byte) (err error) {
	switch table {
	case kv.LogAddrIdx, kv.TblLogAddressIdx:
		err = sd.logAddrsWriter.Add(key)
	case kv.LogTopicIdx, kv.TblLogTopicsIdx, kv.LogTopicIndex:
		err = sd.logTopicsWriter.Add(key)
	case kv.TblTracesToIdx:
		err = sd.tracesToWriter.Add(key)
	case kv.TblTracesFromIdx:
		err = sd.tracesFromWriter.Add(key)
	default:
		panic(fmt.Errorf("unknown shared index %s", table))
	}
	return err
}

func (sd *SharedDomains) SetTx(tx kv.RwTx) { sd.roTx = tx }
func (sd *SharedDomains) StepSize() uint64 { return sd.aggCtx.a.StepSize() }

// SetTxNum sets txNum for all domains as well as common txNum for all domains
// Requires for sd.rwTx because of commitment evaluation in shared domains if aggregationStep is reached
func (sd *SharedDomains) SetTxNum(txNum uint64) {
	sd.txNum = txNum
	for _, d := range sd.dWriter {
		if d != nil {
			d.SetTxNum(txNum)
		}
	}
	if sd.tracesToWriter != nil {
		sd.tracesToWriter.SetTxNum(txNum)
		sd.tracesFromWriter.SetTxNum(txNum)
		sd.logAddrsWriter.SetTxNum(txNum)
		sd.logTopicsWriter.SetTxNum(txNum)
	}
}

func (sd *SharedDomains) TxNum() uint64 { return sd.txNum }

func (sd *SharedDomains) BlockNum() uint64 { return sd.blockNum.Load() }

func (sd *SharedDomains) SetBlockNum(blockNum uint64) {
	sd.blockNum.Store(blockNum)
}

func (sd *SharedDomains) SetTrace(b bool) {
	sd.trace = b
}

func (sd *SharedDomains) ComputeCommitment(ctx context.Context, saveStateAfter bool, blockNum uint64, logPrefix string) (rootHash []byte, err error) {
	return sd.sdCtx.ComputeCommitment(ctx, saveStateAfter, blockNum, logPrefix)
}

// IterateStoragePrefix iterates over key-value pairs of the storage domain that start with given prefix
// Such iteration is not intended to be used in public API, therefore it uses read-write transaction
// inside the domain. Another version of this for public API use needs to be created, that uses
// roTx instead and supports ending the iterations before it reaches the end.
//
// k and v lifetime is bounded by the lifetime of the iterator
func (sd *SharedDomains) IterateStoragePrefix(prefix []byte, it func(k []byte, v []byte, step uint64) error) error {
	// Implementation:
	//     File endTxNum  = last txNum of file step
	//     DB endTxNum    = first txNum of step in db
	//     RAM endTxNum   = current txnum
	//  Example: stepSize=8, file=0-2.kv, db has key of step 2, current tx num is 17
	//     File endTxNum  = 15, because `0-2.kv` has steps 0 and 1, last txNum of step 1 is 15
	//     DB endTxNum    = 16, because db has step 2, and first txNum of step 2 is 16.
	//     RAM endTxNum   = 17, because current tcurrent txNum is 17

	haveRamUpdates := sd.storage.Len() > 0

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
			heap.Push(cpPtr, &CursorItem{t: RAM_CURSOR, key: common.Copy(k), val: common.Copy(v), step: 0, iter: iter, endTxNum: sd.txNum, reverse: true})
		}
	}

	roTx := sd.roTx
	keysCursor, err := roTx.CursorDupSort(sd.aggCtx.a.d[kv.StorageDomain].keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, prefix) {
		step := ^binary.BigEndian.Uint64(v)
		endTxNum := step * sd.StepSize() // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
		if haveRamUpdates && endTxNum >= sd.txNum {
			return fmt.Errorf("probably you didn't set SharedDomains.SetTxNum(). ram must be ahead of db: %d, %d", sd.txNum, endTxNum)
		}

		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		if v, err = roTx.GetOne(sd.aggCtx.a.d[kv.StorageDomain].valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(cpPtr, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), step: step, c: keysCursor, endTxNum: endTxNum, reverse: true})
	}

	sctx := sd.aggCtx.d[kv.StorageDomain]
	for i, item := range sctx.files {
		cursor, err := item.src.bindex.Seek(sctx.statelessGetter(i), prefix)
		if err != nil {
			return err
		}
		if cursor == nil {
			continue
		}

		key := cursor.Key()
		if key != nil && bytes.HasPrefix(key, prefix) {
			val := cursor.Value()
			txNum := item.endTxNum - 1 // !important: .kv files have semantic [from, t)
			heap.Push(cpPtr, &CursorItem{t: FILE_CURSOR, key: key, val: val, step: 0, btCursor: cursor, endTxNum: txNum, reverse: true})
		}
	}

	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		lastStep := cp[0].step
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
					step := ^binary.BigEndian.Uint64(v)
					endTxNum := step * sd.StepSize() // DB can store not-finished step, it means - then set first txn in step - it anyway will be ahead of files
					if haveRamUpdates && endTxNum >= sd.txNum {
						return fmt.Errorf("probably you didn't set SharedDomains.SetTxNum(). ram must be ahead of db: %d, %d", sd.txNum, endTxNum)
					}
					ci1.endTxNum = endTxNum

					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = roTx.GetOne(sd.aggCtx.a.d[kv.StorageDomain].valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					ci1.step = step
					heap.Push(cpPtr, ci1)
				}
			}
		}
		if len(lastVal) > 0 {
			if err := it(lastKey, lastVal, lastStep); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sd *SharedDomains) Close() {
	sd.SetBlockNum(0)
	if sd.aggCtx != nil {
		sd.SetTxNum(0)

		//sd.walLock.Lock()
		//defer sd.walLock.Unlock()
		for _, d := range sd.dWriter {
			d.close()
		}
		sd.logAddrsWriter.close()
		sd.logTopicsWriter.close()
		sd.tracesFromWriter.close()
		sd.tracesToWriter.close()
	}

	if sd.sdCtx != nil {
		sd.sdCtx.updates.keys = nil
		sd.sdCtx.updates.tree.Clear(true)
	}
}

func (sd *SharedDomains) Flush(ctx context.Context, tx kv.RwTx) error {
	if sd.noFlush > 0 {
		sd.noFlush--
	}

	if sd.noFlush == 0 {
		defer mxFlushTook.ObserveDuration(time.Now())
		fh, err := sd.ComputeCommitment(ctx, true, sd.BlockNum(), "flush-commitment")
		if err != nil {
			return err
		}
		if sd.trace {
			_, f, l, _ := runtime.Caller(1)
			fmt.Printf("[SD aggCtx=%d] FLUSHING at tx %d [%x], caller %s:%d\n", sd.aggCtx.id, sd.TxNum(), fh, filepath.Base(f), l)
		}
		for _, d := range sd.dWriter {
			if d != nil {
				if err := d.Flush(ctx, tx); err != nil {
					return err
				}
			}
		}
		if err := sd.logAddrsWriter.Flush(ctx, tx); err != nil {
			return err
		}
		if err := sd.logTopicsWriter.Flush(ctx, tx); err != nil {
			return err
		}
		if err := sd.tracesFromWriter.Flush(ctx, tx); err != nil {
			return err
		}
		if err := sd.tracesToWriter.Flush(ctx, tx); err != nil {
			return err
		}
		if dbg.PruneOnFlushTimeout != 0 {
			_, err = sd.aggCtx.PruneSmallBatches(ctx, dbg.PruneOnFlushTimeout, tx)
			if err != nil {
				return err
			}
		}

		for _, d := range sd.dWriter {
			if d != nil {
				d.close()
			}
		}
		sd.logAddrsWriter.close()
		sd.logTopicsWriter.close()
		sd.tracesFromWriter.close()
		sd.tracesToWriter.close()
	}
	return nil
}

// TemporalDomain satisfaction
func (sd *SharedDomains) DomainGet(name kv.Domain, k, k2 []byte) (v []byte, step uint64, err error) {
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
func (sd *SharedDomains) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte, prevStep uint64) error {
	if val == nil {
		return fmt.Errorf("DomainPut: %s, trying to put nil value. not allowed", domain)
	}
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.DomainGet(domain, k1, k2)
		if err != nil {
			return err
		}
	}
	switch domain {
	case kv.AccountsDomain:
		return sd.updateAccountData(k1, val, prevVal, prevStep)
	case kv.StorageDomain:
		return sd.writeAccountStorage(k1, k2, val, prevVal, prevStep)
	case kv.CodeDomain:
		if bytes.Equal(prevVal, val) {
			return nil
		}
		return sd.updateAccountCode(k1, val, prevVal, prevStep)
	case kv.CommitmentDomain:
		return sd.updateCommitmentData(k1, val, prevVal, prevStep)
	default:
		panic(domain)
	}
}

// DomainDel
// Optimizations:
//   - user can prvide `prevVal != nil` - then it will not read prev value from storage
//   - user can append k2 into k1, then underlying methods will not preform append
//   - if `val == nil` it will call DomainDel
func (sd *SharedDomains) DomainDel(domain kv.Domain, k1, k2 []byte, prevVal []byte, prevStep uint64) error {

	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.DomainGet(domain, k1, k2)
		if err != nil {
			return err
		}
	}
	switch domain {
	case kv.AccountsDomain:
		return sd.deleteAccount(k1, prevVal, prevStep)
	case kv.StorageDomain:
		return sd.delAccountStorage(k1, k2, prevVal, prevStep)
	case kv.CodeDomain:
		if prevVal == nil {
			return nil
		}
		return sd.updateAccountCode(k1, nil, prevVal, prevStep)
	case kv.CommitmentDomain:
		return sd.updateCommitmentData(k1, nil, prevVal, prevStep)
	default:
		panic(domain)
	}
}

func (sd *SharedDomains) DomainDelPrefix(domain kv.Domain, prefix []byte) error {
	if domain != kv.StorageDomain {
		return fmt.Errorf("DomainDelPrefix: not supported")
	}

	type tuple struct {
		k, v []byte
		step uint64
	}
	tombs := make([]tuple, 0, 8)
	if err := sd.IterateStoragePrefix(prefix, func(k, v []byte, step uint64) error {
		tombs = append(tombs, tuple{k, v, step})
		return nil
	}); err != nil {
		return err
	}
	for _, tomb := range tombs {
		if err := sd.DomainDel(kv.StorageDomain, tomb.k, nil, tomb.v, tomb.step); err != nil {
			return err
		}
	}

	if assert.Enable {
		forgotten := 0
		if err := sd.IterateStoragePrefix(prefix, func(k, v []byte, step uint64) error {
			forgotten++
			return nil
		}); err != nil {
			return err
		}
		if forgotten > 0 {
			panic(fmt.Errorf("DomainDelPrefix: %d forgotten keys after '%x' prefix removal", forgotten, prefix))
		}
	}
	return nil
}
func (sd *SharedDomains) Tx() kv.Tx { return sd.roTx }

type SharedDomainsCommitmentContext struct {
	sd           *SharedDomains
	discard      bool
	updates      *UpdateTree
	mode         CommitmentMode
	branchCache  map[string]cachedBranch
	patriciaTrie commitment.Trie
	justRestored atomic.Bool
}

func NewSharedDomainsCommitmentContext(sd *SharedDomains, mode CommitmentMode, trieVariant commitment.TrieVariant) *SharedDomainsCommitmentContext {
	ctx := &SharedDomainsCommitmentContext{
		sd:           sd,
		mode:         mode,
		updates:      NewUpdateTree(mode),
		discard:      dbg.DiscardCommitment(),
		patriciaTrie: commitment.InitializeTrie(trieVariant),
		branchCache:  make(map[string]cachedBranch),
	}

	ctx.patriciaTrie.ResetContext(ctx)
	return ctx
}

type cachedBranch struct {
	data []byte
	step uint64
}

// Cache should ResetBranchCache after each commitment computation
func (sdc *SharedDomainsCommitmentContext) ResetBranchCache() {
	sdc.branchCache = make(map[string]cachedBranch)
}

func (sdc *SharedDomainsCommitmentContext) GetBranch(pref []byte) ([]byte, uint64, error) {
	cached, ok := sdc.branchCache[string(pref)]
	if ok {
		// cached value is already transformed/clean to read.
		// Cache should ResetBranchCache after each commitment computation
		return cached.data, cached.step, nil
	}

	v, step, err := sdc.sd.LatestCommitment(pref)
	if err != nil {
		return nil, 0, fmt.Errorf("GetBranch failed: %w", err)
	}
	if sdc.sd.trace {
		fmt.Printf("[SDC] GetBranch: %x: %x\n", pref, v)
	}
	if len(v) == 0 {
		return nil, 0, nil
	}
	// Trie reads prefix during unfold and after everything is ready reads it again to Merge update, if any, so
	// cache branch until ResetBranchCache called
	sdc.branchCache[string(pref)] = cachedBranch{data: v, step: step}
	return v, step, nil
}

func (sdc *SharedDomainsCommitmentContext) PutBranch(prefix []byte, data []byte, prevData []byte, prevStep uint64) error {
	if sdc.sd.trace {
		fmt.Printf("[SDC] PutBranch: %x: %x\n", prefix, data)
	}
	sdc.branchCache[string(prefix)] = cachedBranch{data: data, step: prevStep}
	return sdc.sd.updateCommitmentData(prefix, data, prevData, prevStep)
}

func (sdc *SharedDomainsCommitmentContext) GetAccount(plainKey []byte, cell *commitment.Cell) error {
	encAccount, _, err := sdc.sd.LatestAccount(plainKey)
	if err != nil {
		return fmt.Errorf("GetAccount failed: %w", err)
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
		//fmt.Printf("GetAccount: %x: n=%d b=%d ch=%x\n", plainKey, nonce, balance, chash)
	}

	code, _, err := sdc.sd.LatestCode(plainKey)
	if err != nil {
		return fmt.Errorf("GetAccount: failed to read latest code: %w", err)
	}
	if len(code) > 0 {
		//fmt.Printf("GetAccount: code %x - %x\n", plainKey, code)
		sdc.updates.keccak.Reset()
		sdc.updates.keccak.Write(code)
		sdc.updates.keccak.Read(cell.CodeHash[:])
	} else {
		cell.CodeHash = commitment.EmptyCodeHashArray
	}
	cell.Delete = len(encAccount) == 0 && len(code) == 0
	return nil
}

func (sdc *SharedDomainsCommitmentContext) GetStorage(plainKey []byte, cell *commitment.Cell) error {
	// Look in the summary table first
	enc, _, err := sdc.sd.LatestStorage(plainKey)
	if err != nil {
		return err
	}
	//if sdc.sd.trace {
	//	fmt.Printf("[SDC] GetStorage: %x - %x\n", plainKey, enc)
	//}
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	cell.Delete = cell.StorageLen == 0
	return nil
}

func (sdc *SharedDomainsCommitmentContext) Reset() {
	if !sdc.justRestored.Load() {
		sdc.patriciaTrie.Reset()
	}
}

func (sdc *SharedDomainsCommitmentContext) TempDir() string {
	return sdc.sd.aggCtx.a.dirs.Tmp
}

//func (ctx *SharedDomainsCommitmentContext) Hasher() hash.Hash { return ctx.updates.keccak }
//
//func (ctx *SharedDomainsCommitmentContext) SetCommitmentMode(m CommitmentMode) { ctx.mode = m }
//

// TouchPlainKey marks plainKey as updated and applies different fn for different key types
// (different behaviour for Code, Account and Storage key modifications).
func (sdc *SharedDomainsCommitmentContext) TouchPlainKey(key string, val []byte, fn func(c *commitmentItem, val []byte)) {
	if sdc.discard {
		return
	}
	sdc.updates.TouchPlainKey(key, val, fn)
}

func (sdc *SharedDomainsCommitmentContext) KeysCount() uint64 {
	return sdc.updates.Size()
}

func (sdc *SharedDomainsCommitmentContext) TouchAccount(c *commitmentItem, val []byte) {
	sdc.updates.TouchAccount(c, val)
}

func (sdc *SharedDomainsCommitmentContext) TouchStorage(c *commitmentItem, val []byte) {
	sdc.updates.TouchStorage(c, val)
}

func (sdc *SharedDomainsCommitmentContext) TouchCode(c *commitmentItem, val []byte) {
	sdc.updates.TouchCode(c, val)
}

// Evaluates commitment for processed state.
func (sdc *SharedDomainsCommitmentContext) ComputeCommitment(ctext context.Context, saveState bool, blockNum uint64, logPrefix string) (rootHash []byte, err error) {
	defer sdc.ResetBranchCache()
	if dbg.DiscardCommitment() {
		sdc.updates.List(true)
		return nil, nil
	}
	mxCommitmentRunning.Inc()
	defer mxCommitmentRunning.Dec()
	defer func(s time.Time) { mxCommitmentTook.ObserveDuration(s) }(time.Now())

	touchedKeys, updates := sdc.updates.List(true)
	if sdc.sd.trace {
		defer func() {
			fmt.Printf("[SDC] rootHash %x block %d keys %d mode %s\n", rootHash, blockNum, len(touchedKeys), sdc.mode)
		}()
	}
	if len(touchedKeys) == 0 {
		rootHash, err = sdc.patriciaTrie.RootHash()
		return rootHash, err
	}

	// data accessing functions should be set when domain is opened/shared context updated
	sdc.patriciaTrie.SetTrace(sdc.sd.trace)
	sdc.Reset()

	switch sdc.mode {
	case CommitmentModeDirect:
		rootHash, err = sdc.patriciaTrie.ProcessKeys(ctext, touchedKeys, logPrefix)
		if err != nil {
			return nil, err
		}
	case CommitmentModeUpdate:
		rootHash, err = sdc.patriciaTrie.ProcessUpdates(ctext, touchedKeys, updates)
		if err != nil {
			return nil, err
		}
	case CommitmentModeDisabled:
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid commitment mode: %s", sdc.mode)
	}
	sdc.justRestored.Store(false)

	if saveState {
		if err := sdc.storeCommitmentState(blockNum, rootHash); err != nil {
			return nil, err
		}
	}

	return rootHash, err
}

func (sdc *SharedDomainsCommitmentContext) storeCommitmentState(blockNum uint64, rh []byte) error {
	if sdc.sd.aggCtx == nil {
		return fmt.Errorf("store commitment state: AggregatorContext is not initialized")
	}
	encodedState, err := sdc.encodeCommitmentState(blockNum, sdc.sd.txNum)
	if err != nil {
		return err
	}
	prevState, prevStep, err := sdc.GetBranch(keyCommitmentState)
	if err != nil {
		return err
	}
	if len(prevState) == 0 && prevState != nil {
		prevState = nil
	}
	// state could be equal but txnum/blocknum could be different.
	// We do skip only full matches
	if bytes.Equal(prevState, encodedState) {
		//fmt.Printf("[commitment] skip store txn %d block %d (prev b=%d t=%d) rh %x\n",
		//	binary.BigEndian.Uint64(prevState[8:16]), binary.BigEndian.Uint64(prevState[:8]), dc.ht.iit.txNum, blockNum, rh)
		return nil
	}
	if sdc.sd.trace {
		fmt.Printf("[commitment] store txn %d block %d rh %x\n", sdc.sd.txNum, blockNum, rh)
	}
	return sdc.sd.dWriter[kv.CommitmentDomain].PutWithPrev(keyCommitmentState, nil, encodedState, prevState, prevStep)
}

func (sdc *SharedDomainsCommitmentContext) encodeCommitmentState(blockNum, txNum uint64) ([]byte, error) {
	var state []byte
	var err error

	switch trie := (sdc.patriciaTrie).(type) {
	case *commitment.HexPatriciaHashed:
		state, err = trie.EncodeCurrentState(nil)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported state storing for patricia trie type: %T", sdc.patriciaTrie)
	}

	cs := &commitmentState{trieState: state, blockNum: blockNum, txNum: txNum}
	encoded, err := cs.Encode()
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// by that key stored latest root hash and tree state
var keyCommitmentState = []byte("state")

func (sd *SharedDomains) LatestCommitmentState(tx kv.Tx, sinceTx, untilTx uint64) (blockNum, txNum uint64, state []byte, err error) {
	return sd.sdCtx.LatestCommitmentState(tx, sd.aggCtx.d[kv.CommitmentDomain], sinceTx, untilTx)
}

func _decodeTxBlockNums(v []byte) (txNum, blockNum uint64) {
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:16])
}

// LatestCommitmentState [sinceTx, untilTx] searches for last encoded state for CommitmentContext.
// Found value does not become current state.
func (sdc *SharedDomainsCommitmentContext) LatestCommitmentState(tx kv.Tx, cd *DomainRoTx, sinceTx, untilTx uint64) (blockNum, txNum uint64, state []byte, err error) {
	if dbg.DiscardCommitment() {
		return 0, 0, nil, nil
	}
	if sdc.patriciaTrie.Variant() != commitment.VariantHexPatriciaTrie {
		return 0, 0, nil, fmt.Errorf("state storing is only supported hex patricia trie")
	}

	// Domain storing only 1 latest commitment (for each step). Erigon can unwind behind this - it means we must look into History (instead of Domain)
	// IdxRange: looking into DB and Files (.ef). Using `order.Desc` to find latest txNum with commitment
	it, err := cd.ht.IdxRange(keyCommitmentState, int(untilTx), int(sinceTx)-1, order.Desc, -1, tx) //[from, to)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("IdxRange: %w", err)
	}
	if it.HasNext() {
		txn, err := it.Next()
		if err != nil {
			return 0, 0, nil, err
		}
		state, err = cd.GetAsOf(keyCommitmentState, txn+1, tx) //WHYYY +1 ???
		if err != nil {
			return 0, 0, nil, err
		}
		if len(state) >= 16 {
			txNum, blockNum = _decodeTxBlockNums(state)
			return blockNum, txNum, state, nil
		}
	}

	// corner-case:
	// it's normal to not have commitment.ef and commitment.v files. They are not determenistic - depend on batchSize, and not very useful.
	// in this case `IdxRange` will be empty
	// and can fallback to reading latest commitment from .kv file
	if err = cd.IteratePrefix(tx, keyCommitmentState, func(key, value []byte) error {
		if len(value) < 16 {
			return fmt.Errorf("invalid state value size %d [%x]", len(value), value)
		}

		txn, _ := _decodeTxBlockNums(value)
		//fmt.Printf("[commitment] Seek found committed txn %d block %d\n", txn, bn)
		if txn >= sinceTx && txn <= untilTx {
			state = value
		}
		return nil
	}); err != nil {
		return 0, 0, nil, fmt.Errorf("failed to seek commitment, IteratePrefix: %w", err)
	}

	if len(state) < 16 {
		return 0, 0, nil, nil
	}

	txNum, blockNum = _decodeTxBlockNums(state)
	return blockNum, txNum, state, nil
}

// SeekCommitment [sinceTx, untilTx] searches for last encoded state from DomainCommitted
// and if state found, sets it up to current domain
func (sdc *SharedDomainsCommitmentContext) SeekCommitment(tx kv.Tx, cd *DomainRoTx, sinceTx, untilTx uint64) (blockNum, txNum uint64, ok bool, err error) {
	_, _, state, err := sdc.LatestCommitmentState(tx, cd, sinceTx, untilTx)
	if err != nil {
		return 0, 0, false, err
	}
	blockNum, txNum, err = sdc.restorePatriciaState(state)
	return blockNum, txNum, true, err
}

// After commitment state is retored, method .Reset() should NOT be called until new updates.
// Otherwise state should be restorePatriciaState()d again.

func (sdc *SharedDomainsCommitmentContext) restorePatriciaState(value []byte) (uint64, uint64, error) {
	cs := new(commitmentState)
	if err := cs.Decode(value); err != nil {
		if len(value) > 0 {
			return 0, 0, fmt.Errorf("failed to decode previous stored commitment state: %w", err)
		}
		// nil value is acceptable for SetState and will reset trie
	}
	if hext, ok := sdc.patriciaTrie.(*commitment.HexPatriciaHashed); ok {
		if err := hext.SetState(cs.trieState); err != nil {
			return 0, 0, fmt.Errorf("failed restore state : %w", err)
		}
		sdc.justRestored.Store(true) // to prevent double reset
		if sdc.sd.trace {
			rh, err := hext.RootHash()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to get root hash after state restore: %w", err)
			}
			fmt.Printf("[commitment] restored state: block=%d txn=%d rh=%x\n", cs.blockNum, cs.txNum, rh)
		}
	} else {
		return 0, 0, fmt.Errorf("state storing is only supported hex patricia trie")
	}
	return cs.blockNum, cs.txNum, nil
}
